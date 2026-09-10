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

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_HIGH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_MAX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_STAT;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_SWAP_CURRENT;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_SWAP_MAX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGroupController.MEMORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the cgroup v2 specifics of the elastic memory controller: the root
 * cgroup writes, and the out of memory condition with its hysteresis.
 */
public class TestCGroupsV2ElasticMemoryController {

  private static final long GB = 1024L * 1024 * 1024;
  private static final long LIMIT = 8 * GB;
  private static final long HIGH = 7 * GB;

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
    conf = new YarnConfiguration();
    cgroups = mock(CGroupsHandler.class);
    when(cgroups.isCGroupsV2()).thenReturn(true);
    when(cgroups.getPathForCGroup(any(), any()))
        .thenReturn("/sys/fs/cgroup/hadoop-yarn/");
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_HIGH)))
        .thenReturn(Long.toString(HIGH));
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
    return (CGroupsV2ElasticMemoryController)
        CGroupElasticMemoryController.create(conf, context, cgroups,
            !controlVirtualMemory, controlVirtualMemory, LIMIT,
            mock(Runnable.class));
  }

  private void stubFootprint(long anon) throws Exception {
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_STAT)))
        .thenReturn("anon " + anon + "\nfile_mapped 0\nkernel 0\n");
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
   * A limit file reading "max" is unlimited, not a parse error. This is what
   * the cleanup writes into it, so the next check reads it back.
   */
  @Test
  public void testUnlimitedHighIsNeverBreached() throws Exception {
    conf.setLong(YarnConfiguration
        .NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS, 0);
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_HIGH)))
        .thenReturn("max");
    stubFootprint(LIMIT);

    assertEquals(Long.MAX_VALUE, CGroupsV2MemoryStat.parseLimit("max"));
    assertFalse("An unlimited memory.high can never be breached",
        controller(false).isUnderOOM());
  }

  /**
   * The footprint over the watermark is the whole condition, and evaluating
   * it on a healthy node is routine, so nothing is logged at WARN or above.
   */
  @Test
  public void testOutOfMemoryOnTheFootprintAlone() throws Exception {
    conf.setLong(YarnConfiguration
        .NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS, 0);
    stubFootprint(HIGH + 1);

    CGroupsV2ElasticMemoryController controller = controller(false);
    assertTrue("The footprint condition has to trigger",
        controller.isUnderOOM());

    stubFootprint(HIGH - 1);
    assertFalse("A footprint under the watermark must not trigger",
        controller.isUnderOOM());

    assertEquals("Evaluating the condition is routine, not a warning: "
            + appender.atLeast(Level.WARN),
        0, appender.atLeast(Level.WARN).size());
  }

  /**
   * The hold timer resets on any dip, so a condition that flaps never kills.
   */
  @Test
  public void testHoldTimerResetsOnADip() throws Exception {
    conf.setLong(YarnConfiguration
        .NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS, 60000);
    CGroupsV2ElasticMemoryController controller = controller(false);

    stubFootprint(HIGH + 1);
    assertFalse("The hold duration has not elapsed yet",
        controller.isUnderOOM());
    stubFootprint(HIGH - 1);
    assertFalse("The condition cleared", controller.isUnderOOM());
    stubFootprint(HIGH + 1);
    assertFalse("The timer has to have restarted from zero",
        controller.isUnderOOM());
  }

  /**
   * With virtual memory enforced the swap in use counts towards the
   * footprint.
   */
  @Test
  public void testSwapCountsTowardsTheFootprintOnVirtualMemory()
      throws Exception {
    conf.setLong(YarnConfiguration
        .NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS, 0);
    stubFootprint(HIGH - 1);
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_SWAP_CURRENT)))
        .thenReturn("4096");

    assertTrue("rss + swap is over the watermark",
        controller(true).isUnderOOM());
  }

  /**
   * The kernel OOM killer getting there first is counted, by the delta the
   * listener reports, and nothing else on that stream is.
   */
  @Test
  public void testKernelOomKillsAreCounted() throws Exception {
    NodeManagerMetrics metrics = mock(NodeManagerMetrics.class);
    Context context = mock(Context.class);
    when(context.getNodeManagerMetrics()).thenReturn(metrics);

    CGroupsV2ElasticMemoryController controller = controller(false, context);
    controller.onListenerError("oom-listener kernel OOM: oom_kill increased"
        + " by 2 to 5 in /sys/fs/cgroup/hadoop-yarn");
    verify(metrics).kernelOomKills(2);

    // An oom increment is not an oom_kill, and neither is anything else the
    // listener may write to its standard error.
    controller.onListenerError("oom-listener kernel OOM: oom increased by 1"
        + " to 3 in /sys/fs/cgroup/hadoop-yarn");
    controller.onListenerError("oom-listener something else entirely");
    verify(metrics, times(1)).kernelOomKills(anyLong());
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
}
