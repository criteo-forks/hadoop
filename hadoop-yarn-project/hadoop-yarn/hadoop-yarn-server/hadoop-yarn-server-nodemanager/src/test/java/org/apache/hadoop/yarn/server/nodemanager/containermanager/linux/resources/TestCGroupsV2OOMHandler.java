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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsV2OOMHandler.Verdict;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_KILL_FILE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_HIGH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_PRESSURE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_STAT;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_SWAP_CURRENT;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PROCS_FILE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGroupController.MEMORY;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.TestDefaultOOMHandler.createContainer;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.TestDefaultOOMHandler.createContainerExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the cgroup v2 out of memory handler: the out of memory verdict and
 * its hysteresis, the optional kernel pressure refinement, the wait for the
 * verdict, and the v2 way of measuring and killing a container. The victim
 * policy is inherited from {@link DefaultOOMHandler} and is tested in
 * {@link TestDefaultOOMHandler}.
 */
public class TestCGroupsV2OOMHandler {

  private static final long GB = 1024L * 1024 * 1024;
  private static final long LIMIT = 8 * GB;
  private static final long HIGH = 7 * GB;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private YarnConfiguration conf;
  private CGroupsHandler cgroups;
  private CountingAppender appender;

  /**
   * Counts the log events of the handler at a given level or above, so that
   * a path we call normal can be asserted to be quiet.
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
    when(cgroups.getPathForCGroupParam(any(), any(), any()))
        .thenReturn("/sys/fs/cgroup/hadoop-yarn/memory.pressure");
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_HIGH)))
        .thenReturn(Long.toString(HIGH));
    appender = new CountingAppender();
    // The handler logs under its own name, the victim policy it inherits
    // under the name of the class that holds it.
    LogManager.getLogger(CGroupsV2OOMHandler.class).addAppender(appender);
    LogManager.getLogger(DefaultOOMHandler.class).addAppender(appender);
  }

  @After
  public void tearDown() {
    LogManager.getLogger(CGroupsV2OOMHandler.class).removeAppender(appender);
    LogManager.getLogger(DefaultOOMHandler.class).removeAppender(appender);
  }

  private CGroupsV2OOMHandler handler(boolean enforceVirtualMemory) {
    return handler(enforceVirtualMemory, mock(Context.class));
  }

  private CGroupsV2OOMHandler handler(boolean enforceVirtualMemory,
      Context context) {
    return new CGroupsV2OOMHandler(context, enforceVirtualMemory, conf,
        cgroups);
  }

  private void setHold(long ms) {
    conf.setLong(YarnConfiguration
        .NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS, ms);
  }

  private void stubFootprint(long anon) throws Exception {
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_STAT)))
        .thenReturn("anon " + anon + "\nfile_mapped 0\nkernel 0\n");
  }

  private void stubNoPressure() throws Exception {
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_PRESSURE)))
        .thenThrow(new ResourceHandlerException("psi=1 is not set"));
  }

  private static String pressure(long fullTotal) {
    return "some avg10=1.00 avg60=0.50 avg300=0.10 total=123456\n"
        + "full avg10=0.50 avg60=0.20 avg300=0.05 total=" + fullTotal + "\n";
  }

  private static long elapsedMs(long startNanos) {
    return (System.nanoTime() - startNanos) / 1000000;
  }

  /**
   * A limit file reading "max" is unlimited, not a parse error. This is what
   * the controller's cleanup writes into it, so the next check reads it back.
   */
  @Test
  public void testUnlimitedHighIsNeverBreached() throws Exception {
    setHold(0);
    stubNoPressure();
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_HIGH)))
        .thenReturn("max");
    stubFootprint(LIMIT);

    assertEquals(Long.MAX_VALUE, CGroupsV2MemoryStat.parseLimit("max"));
    assertEquals("An unlimited memory.high can never be breached",
        Verdict.CLEAR, handler(false).evaluate());
  }

  /**
   * The primary path: no memory.pressure on the host. The condition holds on
   * the memory footprint alone, and the absence of pressure information is
   * not an error, so nothing is logged at WARN or above.
   */
  @Test
  public void testOutOfMemoryWithoutPressureInformation() throws Exception {
    setHold(0);
    stubNoPressure();
    stubFootprint(HIGH + 1);

    CGroupsV2OOMHandler handler = handler(false);
    assertEquals("The footprint condition alone has to trigger",
        Verdict.KILL, handler.evaluate());

    stubFootprint(HIGH - 1);
    assertEquals("A footprint under the watermark must not trigger",
        Verdict.CLEAR, handler.evaluate());

    assertEquals("A kernel without pressure information is the normal case,"
            + " not a misconfiguration: " + appender.atLeast(Level.WARN),
        0, appender.atLeast(Level.WARN).size());
  }

  /**
   * The hold timer resets on any dip, so a condition that flaps never kills.
   */
  @Test
  public void testHoldTimerResetsOnADip() throws Exception {
    setHold(60000);
    stubNoPressure();
    CGroupsV2OOMHandler handler = handler(false);

    stubFootprint(HIGH + 1);
    assertEquals("The hold duration has not elapsed yet",
        Verdict.PENDING, handler.evaluate());
    stubFootprint(HIGH - 1);
    assertEquals("The condition cleared", Verdict.CLEAR, handler.evaluate());
    stubFootprint(HIGH + 1);
    assertEquals("The timer has to have restarted from zero",
        Verdict.PENDING, handler.evaluate());
  }

  /**
   * With pressure information available the footprint condition is narrowed
   * by it: a flat full total means the kernel is not stalling on memory, so
   * nothing is killed. Together with the case below this pins the invariant
   * that the pressure term can only ever turn a kill into a wait.
   */
  @Test
  public void testFlatPressureTotalDoesNotKill() throws Exception {
    setHold(0);
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_PRESSURE)))
        .thenReturn(pressure(42));
    stubFootprint(HIGH + 1);

    CGroupsV2OOMHandler handler = handler(false);
    assertEquals("The first sample has no previous value to compare to",
        Verdict.PENDING, handler.evaluate());
    assertEquals("A full total that did not move means no memory stall",
        Verdict.PENDING, handler.evaluate());
  }

  /**
   * Both terms hold: the footprint is over the watermark and the kernel
   * reports that memory stalled since the previous sample.
   */
  @Test
  public void testFootprintAndPressureKill() throws Exception {
    setHold(0);
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_PRESSURE)))
        .thenReturn(pressure(42))
        .thenReturn(pressure(42))
        .thenReturn(pressure(4711));
    stubFootprint(HIGH + 1);

    CGroupsV2OOMHandler handler = handler(false);
    assertEquals("The first sample has no previous value to compare to",
        Verdict.PENDING, handler.evaluate());
    assertEquals("Both the footprint and the pressure condition hold",
        Verdict.KILL, handler.evaluate());
  }

  /**
   * A footprint under the watermark is not made out of memory by pressure.
   */
  @Test
  public void testPressureAloneNeverKills() throws Exception {
    setHold(0);
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_PRESSURE)))
        .thenReturn(pressure(42))
        .thenReturn(pressure(4711))
        .thenReturn(pressure(9000));
    stubFootprint(HIGH - 1);

    CGroupsV2OOMHandler handler = handler(false);
    assertEquals(Verdict.CLEAR, handler.evaluate());
    assertEquals(Verdict.CLEAR, handler.evaluate());
  }

  /**
   * With virtual memory enforced the swap in use counts towards the
   * footprint.
   */
  @Test
  public void testSwapCountsTowardsTheFootprintOnVirtualMemory()
      throws Exception {
    setHold(0);
    stubNoPressure();
    stubFootprint(HIGH - 1);
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_SWAP_CURRENT)))
        .thenReturn("4096");

    assertEquals("rss + swap is over the watermark",
        Verdict.KILL, handler(true).evaluate());
  }

  @Test
  public void testParsePressureFullTotal() {
    assertEquals(4711,
        CGroupsV2OOMHandler.parsePressureFullTotal(pressure(4711)));
    assertEquals("An empty file yields no counter", -1,
        CGroupsV2OOMHandler.parsePressureFullTotal(""));
    assertEquals("A file with no full line yields no counter", -1,
        CGroupsV2OOMHandler.parsePressureFullTotal(
            "some avg10=0.00 avg60=0.00 avg300=0.00 total=17\n"));
  }

  /**
   * A memory.high event wakes the kill loop up before the verdict is known.
   * One evaluation says pending at that point; the check the loop actually
   * runs on waits through the hold and says kill. The hold itself is the
   * expected course of events, so it is silent.
   */
  @Test(timeout = 20000)
  public void testAwaitOOMWaitsThroughTheHold() throws Exception {
    setHold(300);
    stubNoPressure();
    stubFootprint(HIGH + 1);
    CGroupsV2OOMHandler handler = handler(false);

    long start = System.nanoTime();
    assertEquals("Evaluated once, the hold cannot have elapsed",
        Verdict.PENDING, handler.evaluate());
    assertTrue("Waited for, the hold elapses", handler.isUnderOOM());
    long elapsed = elapsedMs(start);
    assertTrue("Returned after " + elapsed + " ms, before the hold",
        elapsed >= 300);
    assertEquals("Waiting through the hold is the normal course of events: "
        + appender.atLeast(Level.WARN), 0, appender.atLeast(Level.WARN).size());
  }

  /**
   * A footprint that dips while the verdict is pending clears it: the wait
   * ends at once with nothing to kill, it does not last the whole hold. This
   * is what happens when a container frees its memory or exits by itself.
   */
  @Test(timeout = 20000)
  public void testAwaitOOMClearsOnADip() throws Exception {
    setHold(60000);
    stubNoPressure();
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_STAT)))
        .thenReturn("anon " + (HIGH + 1) + "\nfile_mapped 0\nkernel 0\n",
            "anon " + (HIGH - 1) + "\nfile_mapped 0\nkernel 0\n");
    CGroupsV2OOMHandler handler = handler(false);

    long start = System.nanoTime();
    assertFalse("The dip on the second poll clears the verdict",
        handler.isUnderOOM());
    long elapsed = elapsedMs(start);
    assertTrue("Waited " + elapsed + " ms, more than one poll interval",
        elapsed < 5000);
  }

  /**
   * With pressure information on, a held footprint is still pending until
   * the kernel reports a stall. The wait covers that term too, and since a
   * node can sit there legitimately it says so once, not on every poll.
   */
  @Test(timeout = 20000)
  public void testAwaitOOMWaitsForPressureToStall() throws Exception {
    setHold(0);
    stubFootprint(HIGH + 1);
    // The probe at construction, then two flat totals, then a stall.
    when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_PRESSURE)))
        .thenReturn(pressure(100), pressure(100), pressure(100),
            pressure(100), pressure(200));
    CGroupsV2OOMHandler handler = handler(false);

    assertTrue("Killed once the full total grew", handler.awaitOOM());
    verify(cgroups, times(5))
        .getCGroupParam(any(), any(), eq(CGROUP_MEMORY_PRESSURE));
    assertEquals("Past the hold with no stall is said once: "
        + appender.atLeast(Level.WARN), 1, appender.atLeast(Level.WARN).size());
  }

  /**
   * The kill loop runs on the waiting check, not on one evaluation. With no
   * container to kill it can only reach its "I am giving up" failure by
   * passing the hold first: a single evaluation would have said pending and
   * the loop would have returned quietly.
   */
  @Test(timeout = 20000)
  public void testRunWaitsForTheVerdict() throws Exception {
    setHold(300);
    stubNoPressure();
    stubFootprint(HIGH + 1);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(new ConcurrentHashMap<>());
    CGroupsV2OOMHandler handler = handler(false, context);

    long start = System.nanoTime();
    try {
      handler.run();
      fail("The handler has to reach the kill and find nothing");
    } catch (YarnRuntimeException expected) {
      long elapsed = elapsedMs(start);
      assertTrue("Gave up after " + elapsed + " ms, before the hold",
          elapsed >= 300);
    }
  }

  /**
   * With cgroup v2 the page cache is not dropped before the node manager is
   * notified, so the measure that decides whether a container is over its
   * request has to be the RSS keys of memory.stat rather than the total
   * usage. Here only the second container is over, and the victim policy
   * orders it before the one that was launched later.
   */
  @Test
  public void testContainerOutOfLimitFromMemoryStat() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, true, 2L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, true, 1L, true);
    containers.put(c2.getContainerId(), c2);
    Context context = contextOf(containers);

    // 4 + 4 + 1 MB against the 10 MB request of createContainer
    File cGroup1 = stubContainer(c1, memoryStat(4, 4, 1));
    // 6 + 4 + 1 MB against the same request
    File cGroup2 = stubContainer(c2, memoryStat(6, 4, 1));

    killingHandler(context, 1).run();

    assertTrue("The container over its request had to be the victim",
        new File(cGroup2, CGROUP_KILL_FILE).exists());
    assertFalse("The container within its request had to be spared",
        new File(cGroup1, CGROUP_KILL_FILE).exists());
  }

  /**
   * A memory.stat without the kernel key, as read on kernels older than
   * 5.18, degrades to anon + file_mapped instead of failing the check.
   */
  @Test
  public void testContainerOutOfLimitWithoutTheKernelKey() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, true, 2L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, true, 1L, true);
    containers.put(c2.getContainerId(), c2);
    Context context = contextOf(containers);

    File cGroup1 = stubContainer(c1,
        "anon " + mb(4) + "\nfile_mapped " + mb(4) + "\nslab 4711\n");
    File cGroup2 = stubContainer(c2,
        "anon " + mb(8) + "\nfile_mapped " + mb(4) + "\nslab 4711\n");

    killingHandler(context, 1).run();

    assertTrue("anon + file_mapped alone has to decide",
        new File(cGroup2, CGROUP_KILL_FILE).exists());
    assertFalse(new File(cGroup1, CGROUP_KILL_FILE).exists());
  }

  /**
   * With cgroup v2 a container is killed in a single write to cgroup.kill,
   * which takes its docker child cgroup down with it and, unlike the kernel
   * OOM killer, does not increment memory.events' oom_kill.
   */
  @Test
  public void testKillsThroughCGroupKill() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, true, 1L, true);
    containers.put(c1.getContainerId(), c1);
    Context context = contextOf(containers);
    File cGroup = stubContainer(c1, memoryStat(1, 0, 0));

    killingHandler(context, 1).run();

    assertEquals("The kill has to go through cgroup.kill", "1",
        FileUtils.readFileToString(new File(cGroup, CGROUP_KILL_FILE),
            StandardCharsets.UTF_8));
    verify(context.getContainerExecutor(), times(0)).signalContainer(any());
  }

  /**
   * cgroup.kill does not exist before Linux 5.14, and the write can fail for
   * other reasons too. The per pid SIGKILL loop stays as the fallback.
   */
  @Test
  public void testFallsBackToSignallingEveryPid() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, true, 1L, true);
    containers.put(c1.getContainerId(), c1);
    Context context = contextOf(containers);

    // There is no such directory, so the write to cgroup.kill fails.
    String id = c1.getContainerId().toString();
    when(cgroups.getPathForCGroup(MEMORY, id))
        .thenReturn(new File(tmp.getRoot(), "gone").getAbsolutePath());
    when(cgroups.getCGroupParam(MEMORY, id, CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cgroups.getCGroupParam(MEMORY, id, CGROUP_MEMORY_STAT))
        .thenReturn(memoryStat(1, 0, 0));

    killingHandler(context, 1).run();

    verify(context.getContainerExecutor(), times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1234")
            .setContainer(c1)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
  }

  /**
   * After a kill the handler pauses, so that the kernel has a chance to
   * reclaim the pages of the container that just died, and it re-reads the
   * condition before it decides that another one has to go too.
   */
  @Test
  public void testPostKillDelayAndRecheck() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, false, 1L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, false, 2L, true);
    containers.put(c2.getContainerId(), c2);
    Context context = contextOf(containers);

    // The pid loop is used here, because it is what removes the container
    // from the node manager context and so lets a second one be picked.
    for (Container container : new Container[] {c1, c2}) {
      String id = container.getContainerId().toString();
      when(cgroups.getPathForCGroup(MEMORY, id))
          .thenReturn(new File(tmp.getRoot(), "gone-" + id).getAbsolutePath());
      when(cgroups.getCGroupParam(MEMORY, id, CGROUP_PROCS_FILE))
          .thenReturn("123" + id).thenReturn("");
      when(cgroups.getCGroupParam(MEMORY, id, CGROUP_MEMORY_STAT))
          .thenReturn(memoryStat(1, 0, 0));
    }

    long postKillDelayMs = 200;
    conf.setLong(YarnConfiguration
        .NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS,
        postKillDelayMs);
    // Still out of memory after the first kill, resolved after the second.
    CGroupsV2OOMHandler handler = killingHandler(context, 2);

    long start = System.nanoTime();
    handler.run();
    long elapsed = elapsedMs(start);

    verify(context.getContainerExecutor(), times(2)).signalContainer(any());
    assertTrue("The handler has to pause after each kill, it took only "
        + elapsed + " ms", elapsed >= 2 * postKillDelayMs);
  }

  private Context contextOf(
      ConcurrentHashMap<ContainerId, Container> containers) throws Exception {
    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);
    return context;
  }

  /**
   * Give a container a cgroup directory cgroup.kill can be written to, an
   * empty cgroup.procs so that the kill is seen to complete, and the given
   * memory.stat.
   *
   * @return the cgroup directory of the container
   */
  private File stubContainer(Container container, String memoryStat)
      throws Exception {
    String id = container.getContainerId().toString();
    File cGroup = tmp.newFolder(id);
    when(cgroups.getPathForCGroup(MEMORY, id))
        .thenReturn(cGroup.getAbsolutePath());
    when(cgroups.getCGroupParam(MEMORY, id, CGROUP_PROCS_FILE))
        .thenReturn("");
    when(cgroups.getCGroupParam(MEMORY, id, CGROUP_MEMORY_STAT))
        .thenReturn(memoryStat);
    return cGroup;
  }

  /**
   * A handler whose verdict is kill for the given number of evaluations and
   * clear afterwards, so that the kill path can be tested without driving
   * the root cgroup through the hold.
   */
  private CGroupsV2OOMHandler killingHandler(Context context, int kills)
      throws Exception {
    stubNoPressure();
    AtomicInteger remaining = new AtomicInteger(kills);
    return new CGroupsV2OOMHandler(context, false, conf, cgroups) {
      @Override
      synchronized Verdict evaluate() {
        return remaining.getAndDecrement() > 0 ? Verdict.KILL : Verdict.CLEAR;
      }
    };
  }

  private static String memoryStat(long anonMb, long fileMappedMb,
      long kernelMb) {
    return "anon " + mb(anonMb)
        + "\nfile_mapped " + mb(fileMappedMb)
        + "\nkernel " + mb(kernelMb)
        + "\nslab 4711\n";
  }

  private static long mb(long megaBytes) {
    return megaBytes * 1024 * 1024;
  }
}
