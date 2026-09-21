/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_OOM_CONTROL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_NO_LIMIT;

/**
 * Elastic memory control on cgroup v1. The kernel OOM killer is disabled on
 * the root YARN cgroup, so the kernel freezes the cgroup at its limit and
 * reports under_oom instead of killing anything, which leaves the choice of a
 * victim to the node manager, see {@link DefaultOOMHandler}.
 *
 * Nothing but the node manager will ever resolve a frozen cgroup, so an out
 * of memory condition the handler leaves unresolved past the configured
 * timeout stops the controller and fails the node manager.
 */
class CGroupsV1ElasticMemoryController extends CGroupElasticMemoryController {

  private final Clock clock = new MonotonicClock();
  private final int timeoutMS;

  CGroupsV1ElasticMemoryController(Configuration conf,
                                   Context context,
                                   CGroupsHandler cgroups,
                                   boolean controlPhysicalMemory,
                                   boolean controlVirtualMemory,
                                   long limit,
                                   Runnable oomHandlerOverride)
      throws YarnException {
    super(conf, context, cgroups, controlPhysicalMemory, controlVirtualMemory,
        limit, oomHandlerOverride);
    this.timeoutMS =
        1000 * conf.getInt(NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC,
        DEFAULT_NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC);
  }

  @Override
  protected DefaultOOMHandler newDefaultOOMHandler(Context context,
      boolean controlVirtual) {
    return new DefaultOOMHandler(context, controlVirtual);
  }

  @Override
  protected String[] oomListenerArgs() {
    return new String[] {"1", yarnCGroupPath};
  }

  /**
   * Whether the root YARN cgroup still reports under_oom.
   */
  private boolean isUnderOOM() throws ResourceHandlerException {
    String underOOM = cgroups.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL);
    return underOOM.contains(CGroupsHandler.UNDER_OOM);
  }

  /**
   * Resolve an OOM event.
   * Listen to the handler timeouts.
   * @param executor Executor to create watchdog with.
   * @throws InterruptedException interrupted
   * @throws ExecutionException cannot launch watchdog
   */
  @Override
  protected void resolveOOM(ExecutorService executor)
      throws InterruptedException, ExecutionException {
    // Just log, when we are still in OOM after a couple of seconds
    final long start = clock.getTime();
    Future<Boolean> watchdog =
        executor.submit(() -> watchAndLogOOMState(start));
    // Kill something to resolve the issue
    try {
      getOOMHandler().run();
    } catch (RuntimeException ex) {
      watchdog.cancel(true);
      throw new OOMNotResolvedException("OOM handler failed", ex);
    }
    if (!watchdog.get()) {
      // If we are still in OOM,
      // the watchdog will trigger stop
      // listening to exit this loop
      throw new OOMNotResolvedException("OOM handler timed out", null);
    }
  }

  /**
   * Just watch until we are in OOM and log. Send an update log every second.
   * @return if the OOM was resolved successfully
   */
  private boolean watchAndLogOOMState(long start) {
    long lastLog = start;
    try {
      long end = start;
      // Throw an error, if we are still in OOM after 5 seconds
      while(end - start < timeoutMS) {
        end = clock.getTime();
        if (isUnderOOM()) {
          if (end - lastLog > 1000) {
            LOG.warn(String.format(
                "OOM not resolved in %d ms", end - start));
            lastLog = end;
          }
        } else {
          LOG.info(String.format(
              "Resolved OOM in %d ms", end - start));
          return true;
        }
        // We do not want to saturate the CPU
        // leaving the resources to the actual OOM killer
        // but we want to be fast, too.
        Thread.sleep(10);
      }
    } catch (InterruptedException ex) {
      LOG.debug("Watchdog interrupted");
    } catch (Exception e) {
      LOG.warn("Exception running logging thread", e);
    }
    LOG.warn(String.format("OOM was not resolved in %d ms",
        clock.getTime() - start));
    stopListening();
    return false;
  }

  /**
   * Update root memory cgroup. This contains all containers.
   * The physical limit has to be set first then the virtual limit.
   */
  @Override
  protected void setCGroupParameters() throws ResourceHandlerException {
    // Disable the OOM killer
    cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL, "1");
    if (controlPhysicalMemory && !controlVirtualMemory) {
      try {
        // Ignore virtual memory limits, since we do not know what it is set to
        cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
            CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES, CGROUP_NO_LIMIT);
      } catch (ResourceHandlerException ex) {
        LOG.debug("Swap monitoring is turned off in the kernel");
      }
      // Set physical memory limits
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES, Long.toString(limit));
    } else if (controlVirtualMemory && !controlPhysicalMemory) {
      // Ignore virtual memory limits, since we do not know what it is set to
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES, CGROUP_NO_LIMIT);
      // Set physical limits to no more than virtual limits
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES, Long.toString(limit));
      // Set virtual memory limits
      // Important: it has to be set after physical limit is set
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES, Long.toString(limit));
    } else {
      throw new ResourceHandlerException(
          String.format("Unsupported scenario physical:%b virtual:%b",
              controlPhysicalMemory, controlVirtualMemory));
    }
  }

  /**
   * Reset root memory cgroup to OS defaults. This controls all containers.
   */
  @Override
  protected void resetCGroupParameters() {
    try {
      try {
        // Disable memory limits
        cgroups.updateCGroupParam(
            CGroupsHandler.CGroupController.MEMORY, "",
            CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES, CGROUP_NO_LIMIT);
      } catch (ResourceHandlerException ex) {
        LOG.debug("Swap monitoring is turned off in the kernel");
      }
      cgroups.updateCGroupParam(
          CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES, CGROUP_NO_LIMIT);
      // Enable the OOM killer
      cgroups.updateCGroupParam(
          CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_OOM_CONTROL, "0");
    } catch (ResourceHandlerException ex) {
      LOG.warn("Error in cleanup", ex);
    }
  }
}
