/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_KILL_FILE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_HIGH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PROCS_FILE;

/**
 * The out of memory handler for cgroup v2. It is a {@link DefaultOOMHandler}
 * with what the cgroup version decides overridden: the out of memory
 * condition, the memory measure, how a container is killed and the pause
 * after a kill. The victim policy of {@link DefaultOOMHandler#run()} is
 * untouched, and so is every v1 path.
 *
 * cgroup v2 has no oom_kill_disable, so the kernel cannot be asked to freeze
 * the cgroup at its limit: memory.max is always a kill. The node manager
 * therefore gets its warning from memory.high, which
 * {@link CGroupsV2ElasticMemoryController} sets below memory.max and which
 * only throttles and reclaims, and this handler kills a container of its
 * choosing before memory.max is reached.
 *
 * A memory.high event is only a wake-up. The kernel raises it before it even
 * attempts reclaim, on a total usage that includes the page cache, so it says
 * nothing about whether the footprint is reclaimable. The condition that
 * actually justifies a kill is the memory footprint of
 * {@link CGroupsV2MemoryStat} standing above memory.high without
 * interruption for the configured hold duration.
 *
 * That verdict takes time to settle, so {@link #isUnderOOM()} blocks until
 * it has: the kill loop kills when the footprint held, and returns when it
 * dipped, which is what happens when a container frees its memory or exits
 * by itself.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CGroupsV2OOMHandler extends DefaultOOMHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CGroupsV2OOMHandler.class);

  /**
   * How long to wait for cgroup.kill to empty a container cgroup before
   * carrying on. The kernel delivers the signals synchronously, so this only
   * covers the reaping of the processes.
   */
  private static final int CGROUP_KILL_WAIT_ATTEMPTS = 100;
  private static final int CGROUP_KILL_WAIT_INTERVAL_MS = 10;

  /** How often {@link #awaitOOM()} re-evaluates a pending verdict. */
  private static final long OOM_POLL_INTERVAL_MS = 100;

  /**
   * The out of memory verdict. PENDING is over memory.high, but not yet for
   * the hold duration.
   */
  @VisibleForTesting
  enum Verdict { CLEAR, PENDING, KILL }

  private final Clock clock = new MonotonicClock();
  private final boolean enforceVirtualMemory;
  private final String yarnCGroupPath;
  private final long holdDurationMs;
  private final long postKillDelayMs;

  /**
   * When the footprint condition started holding, 0 when it does not hold.
   * Guarded by the monitor of this object.
   */
  private long firstBreachAtMs;

  /**
   * Create an OOM handler on the configuration and the cgroups handler of
   * the node manager. This has to be public to be able to construct through
   * reflection.
   * @param context node manager context to work with
   * @param enforceVirtualMemory true if virtual memory needs to be checked,
   *                   false if physical memory needs to be checked instead
   */
  public CGroupsV2OOMHandler(Context context, boolean enforceVirtualMemory) {
    this(context, enforceVirtualMemory, context.getConf(),
        ResourceHandlerModule.getCGroupsHandler());
  }

  /**
   * Create an OOM handler.
   * @param context node manager context to work with
   * @param enforceVirtualMemory true if virtual memory needs to be checked,
   *                   false if physical memory needs to be checked instead
   * @param conf Yarn configuration to read the hold duration and the post
   *             kill delay from
   * @param cgroups the cgroups handler to read and write cgroups with
   */
  public CGroupsV2OOMHandler(Context context, boolean enforceVirtualMemory,
      Configuration conf, CGroupsHandler cgroups) {
    super(context, enforceVirtualMemory, cgroups);
    this.enforceVirtualMemory = enforceVirtualMemory;
    this.yarnCGroupPath = cgroups.getPathForCGroup(
        CGroupsHandler.CGroupController.MEMORY, "");
    this.holdDurationMs = conf.getLong(
        NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS,
        DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS);
    this.postKillDelayMs = conf.getLong(
        NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS,
        DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS);
  }

  /**
   * The out-of-memory condition: the memory footprint of the root YARN cgroup
   * has been above memory.high continuously for the configured hold duration.
   *
   * This is not memory.current above memory.high. memory.current counts the
   * page cache, which the kernel reclaims on its own, so it would have us
   * kill containers over a cache that was about to be dropped anyway.
   *
   * The hold duration is oomd's PressureAbove hysteresis applied to the
   * footprint: the timer resets on any dip, so a condition that flaps never
   * kills.
   *
   * A memory.high event wakes the kill loop up before the verdict can be
   * known, so this blocks while the verdict is pending. See
   * {@link #awaitOOM()}.
   */
  @Override
  protected boolean isUnderOOM() throws ResourceHandlerException {
    return awaitOOM();
  }

  /**
   * One evaluation of the condition. Distinguishes a footprint that is not
   * over memory.high, where nothing is pending, from one that is but has not
   * held long enough yet, which is what {@link #awaitOOM()} waits on.
   */
  @VisibleForTesting
  synchronized Verdict evaluate() throws ResourceHandlerException {
    long now = clock.getTime();
    if (!isOverHighWatermark()) {
      firstBreachAtMs = 0;
      return Verdict.CLEAR;
    }
    if (firstBreachAtMs == 0) {
      firstBreachAtMs = now;
    }
    return now - firstBreachAtMs >= holdDurationMs
        ? Verdict.KILL : Verdict.PENDING;
  }

  /**
   * Block while the verdict is pending, re-evaluating every
   * {@link #OOM_POLL_INTERVAL_MS}. The hold is expected and silent.
   *
   * @return true when the footprint held above memory.high for the hold
   *         duration, so a container has to be killed; false when it dipped,
   *         so there is nothing to do this time
   * @throws ResourceHandlerException a cgroup file could not be read
   */
  @VisibleForTesting
  boolean awaitOOM() throws ResourceHandlerException {
    while (true) {
      Verdict verdict = evaluate();
      if (verdict != Verdict.PENDING) {
        return verdict == Verdict.KILL;
      }
      try {
        Thread.sleep(OOM_POLL_INTERVAL_MS);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  private boolean isOverHighWatermark() throws ResourceHandlerException {
    long footprint = CGroupsV2MemoryStat.readFootprint(cgroups, "",
        enforceVirtualMemory);
    long high = CGroupsV2MemoryStat.parseLimit(cgroups.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY, "", CGROUP_MEMORY_HIGH));
    if (footprint > high) {
      LOG.debug("{} uses {} bytes over its memory.high of {}",
          yarnCGroupPath, footprint, high);
      return true;
    }
    return false;
  }

  /**
   * With cgroup v2 the page cache is not dropped before the node manager is
   * notified, so it has to be excluded explicitly. See
   * {@link CGroupsV2MemoryStat}.
   */
  @Override
  protected long getMemoryUsage(String cGroupId)
      throws ResourceHandlerException {
    return CGroupsV2MemoryStat.readFootprint(
        cgroups, cGroupId, enforceVirtualMemory);
  }

  /**
   * One write to cgroup.kill. Whenever that write fails, every process of
   * the cgroup is signalled individually as on v1.
   */
  @Override
  protected boolean sigKill(Container container) {
    try {
      cgroupKill(container);
      return true;
    } catch (ResourceHandlerException | IOException ex) {
      LOG.warn(String.format("Could not kill container %s through"
              + " cgroup.kill, falling back to signalling every pid.",
          container.getContainerId()), ex);
    }
    return super.sigKill(container);
  }

  /**
   * Kill every process of the container with a single write. cgroup.kill kills
   * the whole subtree, so a docker child cgroup goes with it, and unlike the
   * kernel OOM killer it does not increment memory.events' oom_kill, which is
   * what keeps that counter a discriminator of the kernel's own kills.
   *
   * @param container Container to clean up
   * @throws ResourceHandlerException cgroup.procs could not be read
   * @throws IOException cgroup.kill could not be written, which is also the
   *                     case on kernels older than 5.14, where it is absent
   */
  private void cgroupKill(Container container)
      throws ResourceHandlerException, IOException {
    String containerId = container.getContainerId().toString();
    // cgroup.kill has no controller prefix, so it cannot go through
    // updateCGroupParam.
    Path killFile = Paths.get(
        cgroups.getPathForCGroup(
            CGroupsHandler.CGroupController.MEMORY, containerId),
        CGROUP_KILL_FILE);
    Files.write(killFile, "1".getBytes(StandardCharsets.UTF_8));
    LOG.debug("Terminating container {} by writing to {}",
        containerId, killFile);
    for (int attempt = 0; attempt < CGROUP_KILL_WAIT_ATTEMPTS; ++attempt) {
      if (cgroups.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
          containerId, CGROUP_PROCS_FILE).isEmpty()) {
        return;
      }
      try {
        Thread.sleep(CGROUP_KILL_WAIT_INTERVAL_MS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for processes to disappear");
        return;
      }
    }
    // The signals are delivered, only the reaping is late. Nothing is left to
    // do here but say so: the memory is on its way back either way.
    LOG.warn("Container {} still has processes {} ms after cgroup.kill",
        containerId, CGROUP_KILL_WAIT_ATTEMPTS * CGROUP_KILL_WAIT_INTERVAL_MS);
  }

  /**
   * Kill a container, then give the kernel time to reclaim its pages before
   * the caller decides that another one has to go too. Unlike v1, where the
   * cgroup is frozen while we work, nothing here stops the condition from
   * still reading as out of memory for as long as the reclaim takes.
   */
  @Override
  protected boolean killContainer() {
    boolean containerKilled = super.killContainer();
    if (containerKilled && postKillDelayMs > 0) {
      try {
        Thread.sleep(postKillDelayMs);
      } catch (InterruptedException ex) {
        LOG.debug("Interrupted while waiting after a kill");
      }
    }
    return containerKilled;
  }
}
