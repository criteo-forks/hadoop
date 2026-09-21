/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_CURRENT;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_HIGH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_MAX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_STAT;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_SWAP_MAX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGroupController.MEMORY;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the cgroup v2 specifics of the elastic memory controller: the root
 * cgroup writes, what it does about a failing handler, and the count of the
 * kernel's own kills. The out of memory verdict is the handler's, see
 * {@link TestCGroupsV2OOMHandler}.
 */
public class TestCGroupsV2ElasticMemoryController {

  private static final long GB = 1024L * 1024 * 1024;
  private static final long LIMIT = 8 * GB;

  private YarnConfiguration conf;
  private CGroupsHandler cgroups;
  private CountingAppender appender;

  /**
   * Counts the log events of the controller at a given level or above, so
   * that a path we call normal can be asserted to be quiet.
   */
  private static class CountingAppender extends AppenderSkeleton {
    private final List<LoggingEvent> events = new ArrayList<>();

    @Override
    protected void append(LoggingEvent event) {
      synchronized (events) {
        events.add(event);
      }
    }

    @Override
    public void close() {
    }

    @Override
    public boolean requiresLayout() {
      return false;
    }

    private List<LoggingEvent> atLeast(Level level) {
      List<LoggingEvent> matching = new ArrayList<>();
      synchronized (events) {
        for (LoggingEvent event : events) {
          if (event.getLevel().isGreaterOrEqual(level)) {
            matching.add(event);
          }
        }
      }
      return matching;
    }
  }

  @Before
  public void setUp() throws Exception {
    DefaultMetricsSystem.setMiniClusterMode(true);
    conf = new YarnConfiguration();
    cgroups = mock(CGroupsHandler.class);
    when(cgroups.isCGroupsV2()).thenReturn(true);
    when(cgroups.getPathForCGroup(any(), any()))
        .thenReturn("/sys/fs/cgroup/hadoop-yarn/");
    appender = new CountingAppender();
    LogManager.getLogger(CGroupElasticMemoryController.class)
        .addAppender(appender);
  }

  @After
  public void tearDown() {
    LogManager.getLogger(CGroupElasticMemoryController.class)
        .removeAppender(appender);
  }

  private CGroupsV2ElasticMemoryController controller(
      boolean controlVirtualMemory) throws Exception {
    return controller(controlVirtualMemory, null);
  }

  private CGroupsV2ElasticMemoryController controller(
      boolean controlVirtualMemory, Context context) throws Exception {
    return controller(controlVirtualMemory, context, mock(Runnable.class));
  }

  private CGroupsV2ElasticMemoryController controller(
      boolean controlVirtualMemory, Context context, Runnable handler)
      throws Exception {
    return (CGroupsV2ElasticMemoryController)
        CGroupElasticMemoryController.create(conf, context, cgroups,
            !controlVirtualMemory, controlVirtualMemory, LIMIT, handler);
  }

  /**
   * The root writes for physical memory: swap off, then the hard limit, then
   * the throttling watermark a margin below it. Plain byte counts, and no
   * memory.oom.group.
   */
  @Test
  public void testSetCGroupParametersPhysical() throws Exception {
    conf.setInt(YarnConfiguration
        .NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_HIGH_MARGIN_MB, 1024);
    controller(false).setCGroupParameters();
    ElasticMemoryMetrics metrics = ElasticMemoryMetrics.create();
    assertEquals(LIMIT, metrics.memoryLimitBytes.value());
    assertEquals(LIMIT - GB, metrics.memoryHighBytes.value());

    InOrder order = inOrder(cgroups);
    order.verify(cgroups).updateCGroupParam(
        MEMORY, "", CGROUP_MEMORY_SWAP_MAX, "0");
    order.verify(cgroups).updateCGroupParam(
        MEMORY, "", CGROUP_MEMORY_MAX, Long.toString(LIMIT));
    order.verify(cgroups).updateCGroupParam(
        MEMORY, "", CGROUP_MEMORY_HIGH, Long.toString(LIMIT - GB));
  }

  /**
   * The root writes for virtual memory: the hard limit and the watermark
   * first, then the swap limit, as in v1.
   */
  @Test
  public void testSetCGroupParametersVirtual() throws Exception {
    conf.setInt(YarnConfiguration
        .NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_HIGH_MARGIN_MB, 1024);
    controller(true).setCGroupParameters();

    InOrder order = inOrder(cgroups);
    order.verify(cgroups).updateCGroupParam(
        MEMORY, "", CGROUP_MEMORY_MAX, Long.toString(LIMIT));
    order.verify(cgroups).updateCGroupParam(
        MEMORY, "", CGROUP_MEMORY_HIGH, Long.toString(LIMIT - GB));
    order.verify(cgroups).updateCGroupParam(
        MEMORY, "", CGROUP_MEMORY_SWAP_MAX, Long.toString(LIMIT));
  }

  /**
   * The default margin is 5% of the limit, with a floor of 512 MiB.
   */
  @Test
  public void testDefaultHighMargin() throws Exception {
    controller(false).setCGroupParameters();

    // 5% of 8 GiB is below the floor.
    verifyRootWrite(CGROUP_MEMORY_HIGH, Long.toString(LIMIT - 512 * 1024 * 1024));
  }

  private void verifyRootWrite(String param, String value) throws Exception {
    verify(cgroups).updateCGroupParam(MEMORY, "", param, value);
  }

  /**
   * The cleanup lifts the throttling watermark, then the hard limit, then the
   * swap limit, and writes the literal the kernel accepts for "no limit".
   */
  @Test
  public void testResetCGroupParameters() throws Exception {
    controller(false).resetCGroupParameters();

    InOrder order = inOrder(cgroups);
    order.verify(cgroups).updateCGroupParam(
        MEMORY, "", CGROUP_MEMORY_HIGH, "max");
    order.verify(cgroups).updateCGroupParam(
        MEMORY, "", CGROUP_MEMORY_MAX, "max");
    order.verify(cgroups).updateCGroupParam(
        MEMORY, "", CGROUP_MEMORY_SWAP_MAX, "max");
  }

  /**
   * A cleanup write that fails must not make the following ones be skipped.
   */
  @Test
  public void testResetCGroupParametersKeepsGoingAfterAFailure()
      throws Exception {
    doThrow(new ResourceHandlerException("read only"))
        .when(cgroups).updateCGroupParam(
            MEMORY, "", CGROUP_MEMORY_HIGH, "max");

    controller(false).resetCGroupParameters();

    verifyRootWrite(CGROUP_MEMORY_MAX, "max");
    verifyRootWrite(CGROUP_MEMORY_SWAP_MAX, "max");
  }

  /**
   * The kernel OOM killer getting there first is counted, by the delta the
   * listener reports, and nothing else on that stream is.
   */
  @Test
  public void testKernelOomKillsAreCounted() throws Exception {
    ElasticMemoryMetrics metrics = ElasticMemoryMetrics.create();
    long killsBefore = metrics.kernelOomKills.value();
    CGroupsV2ElasticMemoryController controller = controller(false);
    controller.onListenerError("oom-listener kernel OOM: oom_kill increased"
        + " by 2 to 5 in /sys/fs/cgroup/hadoop-yarn");
    assertEquals(killsBefore + 2, metrics.kernelOomKills.value());

    // An oom increment is not an oom_kill, and neither is anything else the
    // listener may write to its standard error.
    controller.onListenerError("oom-listener kernel OOM: oom increased by 1"
        + " to 3 in /sys/fs/cgroup/hadoop-yarn");
    controller.onListenerError("oom-listener something else entirely");
    assertEquals(killsBefore + 2, metrics.kernelOomKills.value());
  }

  /**
   * The memory footprint sums the RSS keys of memory.stat, and a key the
   * kernel does not expose counts as 0 rather than failing the read.
   */
  @Test
  public void testFootprintDegradesWithoutTheKernelKey() throws Exception {
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_STAT)))
        .thenReturn("anon 100\nfile_mapped 20\nkernel 3\nslab 99999\n");
    assertEquals(123,
        CGroupsV2MemoryStat.readFootprint(cgroups, "", false));

    // Before Linux 5.18 there is no kernel key.
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_STAT)))
        .thenReturn("anon 100\nfile_mapped 20\nslab 99999\n");
    assertEquals(120,
        CGroupsV2MemoryStat.readFootprint(cgroups, "", false));
  }

  @Test
  public void testSamplePublishesFreshMeasures() throws Exception {
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_STAT)))
        .thenReturn("anon 100\nfile_mapped 20\nkernel 3\n");
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_CURRENT)))
        .thenReturn("456");

    controller(false).sample();

    ElasticMemoryMetrics metrics = ElasticMemoryMetrics.create();
    assertEquals(123, metrics.memoryFootprintBytes.value());
    assertEquals(456, metrics.memoryCurrentBytes.value());
  }

  /**
   * memory.max is a kill the kernel performs by itself, so a handler that
   * fails is not a reason to stop: the failure is logged, the listener stays,
   * and the root limits stay. Stopping here is what left a node with no
   * elastic memory control at all, silently.
   */
  @Test
  public void testResolveOOMKeepsListeningWhenTheHandlerFails()
      throws Exception {
    Runnable handler = mock(Runnable.class);
    doThrow(new YarnRuntimeException("nothing to kill")).when(handler).run();
    CGroupsV2ElasticMemoryController controller =
        controller(false, null, handler);

    controller.resolveOOM(null);

    verify(handler).run();
    assertEquals("The failure is logged: " + appender.atLeast(Level.WARN),
        1, appender.atLeast(Level.WARN).size());
    verify(cgroups, never()).updateCGroupParam(any(), any(), any(), any());
  }
}
