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

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_HIGH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_STAT;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for elastic non-strict memory controller based on cgroups.
 *
 * Every case runs against both cgroup versions. The v1 parametrisation is the
 * non-regression gate of the v2 work: on the day it ships every node of the
 * cluster still runs cgroup v1, so these cases have to keep passing
 * unchanged.
 */
@RunWith(Parameterized.class)
public class TestCGroupElasticMemoryController {
  protected static final Logger LOG = LoggerFactory
      .getLogger(TestCGroupElasticMemoryController.class);

  /** The value the mocked handler reports for the root memory.high. */
  private static final long HIGH_WATERMARK = 8000;

  @Parameterized.Parameters(name = "cgroups v{0}")
  public static Collection<Object[]> versions() {
    return Arrays.asList(new Object[][] {{1}, {2}});
  }

  private final boolean cgroupsV2;
  private YarnConfiguration conf = new YarnConfiguration();
  private File script = new File("target/" +
      TestCGroupElasticMemoryController.class.getName());

  public TestCGroupElasticMemoryController(int version) {
    this.cgroupsV2 = version == 2;
  }

  @Before
  public void setUp() {
    // The cases below drive the out of memory condition directly, so no
    // hysteresis, and a margin that fits under the byte sized limits they use.
    conf.setLong(YarnConfiguration
        .NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS, 0);
    conf.setInt(YarnConfiguration
        .NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_HIGH_MARGIN_MB, 0);
  }

  /**
   * A handler of the version under test, reporting the requested out of
   * memory state of the root YARN cgroup.
   */
  private CGroupsHandler mockCGroupsHandler(boolean underOOM)
      throws ResourceHandlerException {
    CGroupsHandler cgroups = mock(CGroupsHandler.class);
    when(cgroups.isCGroupsV2()).thenReturn(cgroupsV2);
    when(cgroups.getPathForCGroup(any(), any())).thenReturn("");
    if (cgroupsV2) {
      when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_STAT)))
          .thenReturn(memoryStat(
              underOOM ? HIGH_WATERMARK + 1 : HIGH_WATERMARK - 1));
      when(cgroups.getCGroupParam(any(), any(), eq(CGROUP_MEMORY_HIGH)))
          .thenReturn(Long.toString(HIGH_WATERMARK));
    } else {
      when(cgroups.getCGroupParam(any(), any(), any()))
          .thenReturn(underOOM ? "under_oom 1" : "under_oom 0");
    }
    return cgroups;
  }

  private static String memoryStat(long anon) {
    return "anon " + anon + "\nfile_mapped 0\nkernel 0\nslab 12345\n";
  }

  /**
   * Test that at least one memory type is requested.
   * @throws YarnException on exception
   */
  @Test(expected = YarnException.class)
  public void testConstructorOff()
      throws YarnException {
    CGroupElasticMemoryController.create(
        conf,
        null,
        null,
        false,
        false,
        10000
    );
  }

  /**
   * Test that the controller matches the version of the handler.
   * @throws Exception on exception
   */
  @Test
  public void testCreateMatchesTheHandlerVersion() throws Exception {
    CGroupElasticMemoryController controller =
        CGroupElasticMemoryController.create(
            conf, null, mockCGroupsHandler(false), true, false, 10000);
    assertEquals("The controller has to match the version of the handler",
        cgroupsV2
            ? CGroupsV2ElasticMemoryController.class
            : CGroupsV1ElasticMemoryController.class,
        controller.getClass());
  }

  /**
   * Test that the OOM logic is pluggable.
   * @throws Exception on exception
   */
  @Test
  public void testConstructorHandler()
      throws Exception {
    conf.setClass(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_HANDLER,
        DummyRunnableWithContext.class, Runnable.class);
    CGroupElasticMemoryController.create(
        conf,
        null,
        mockCGroupsHandler(false),
        true,
        false,
        10000
    );
  }

  /**
   * Test that the handler is notified about multiple OOM events.
   * @throws Exception on exception
   */
  @Test(timeout = 20000)
  public void testMultipleOOMEvents() throws Exception {
    conf.set(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH,
        script.getAbsolutePath());
    try {
      FileUtils.writeStringToFile(script,
          "#!/bin/bash\nprintf oomevent;printf oomevent;\n", StandardCharsets.UTF_8, false);
      assertTrue("Could not set executable",
          script.setExecutable(true));

      CGroupsHandler cgroups = mockCGroupsHandler(false);

      Runnable handler = mock(Runnable.class);
      doNothing().when(handler).run();

      CGroupElasticMemoryController controller =
          CGroupElasticMemoryController.create(
              conf,
              null,
              cgroups,
              true,
              false,
              10000,
              handler
          );
      controller.run();
      verify(handler, times(2)).run();
    } finally {
      assertTrue(String.format("Could not clean up script %s",
          script.getAbsolutePath()), script.delete());
    }
  }

  /**
   * Test the scenario that the controller is stopped before.
   * the child process starts
   * @throws Exception one exception
   */
  @Test(timeout = 20000)
  public void testStopBeforeStart() throws Exception {
    conf.set(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH,
        script.getAbsolutePath());
    try {
      FileUtils.writeStringToFile(script, "#!/bin/bash\nprintf oomevent;printf oomevent;\n",
          StandardCharsets.UTF_8, false);
      assertTrue("Could not set executable",
          script.setExecutable(true));

      CGroupsHandler cgroups = mockCGroupsHandler(false);

      Runnable handler = mock(Runnable.class);
      doNothing().when(handler).run();

      CGroupElasticMemoryController controller =
          CGroupElasticMemoryController.create(
              conf,
              null,
              cgroups,
              true,
              false,
              10000,
              handler
          );
      controller.stopListening();
      controller.run();
      verify(handler, times(0)).run();
    } finally {
      assertTrue(String.format("Could not clean up script %s",
          script.getAbsolutePath()), script.delete());
    }
  }

  /**
   * Test the edge case that OOM is never resolved.
   * @throws Exception on exception
   */
  @Test(timeout = 20000, expected = YarnRuntimeException.class)
  public void testInfiniteOOM() throws Exception {
    conf.set(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH,
        script.getAbsolutePath());
    Runnable handler = mock(Runnable.class);
    try {
      FileUtils.writeStringToFile(script, "#!/bin/bash\nprintf oomevent;sleep 1000;\n",
          StandardCharsets.UTF_8, false);
      assertTrue("Could not set executable",
          script.setExecutable(true));

      CGroupsHandler cgroups = mockCGroupsHandler(true);

      doNothing().when(handler).run();

      CGroupElasticMemoryController controller =
          CGroupElasticMemoryController.create(
              conf,
              null,
              cgroups,
              true,
              false,
              10000,
              handler
          );
      controller.run();
    } finally {
      verify(handler, times(1)).run();
      assertTrue(String.format("Could not clean up script %s",
          script.getAbsolutePath()), script.delete());
    }
  }

  /**
   * Test the edge case that OOM cannot be resolved due to the lack of
   * containers.
   * @throws Exception on exception
   */
  @Test(timeout = 20000, expected = YarnRuntimeException.class)
  public void testNothingToKill() throws Exception {
    conf.set(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH,
        script.getAbsolutePath());
    Runnable handler = mock(Runnable.class);
    try {
      FileUtils.writeStringToFile(script, "#!/bin/bash\nprintf oomevent;sleep 1000;\n",
          StandardCharsets.UTF_8, false);
      assertTrue("Could not set executable",
          script.setExecutable(true));

      CGroupsHandler cgroups = mockCGroupsHandler(true);

      doThrow(new YarnRuntimeException("Expected")).when(handler).run();

      CGroupElasticMemoryController controller =
          CGroupElasticMemoryController.create(
              conf,
              null,
              cgroups,
              true,
              false,
              10000,
              handler
          );
      controller.run();
    } finally {
      verify(handler, times(1)).run();
      assertTrue(String.format("Could not clean up script %s",
          script.getAbsolutePath()), script.delete());
    }
  }

  /**
   * Test that node manager can exit listening.
   * This is done by running a long running listener for 10000 seconds.
   * Then we wait for 2 seconds and stop listening.
   * We do not use a script this time to avoid leaking the child process.
   * @throws Exception exception occurred
   */
  @Test(timeout = 20000)
  public void testNormalExit() throws Exception {
    conf.set(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH,
        "sleep");
    ExecutorService service = Executors.newFixedThreadPool(1);
    try {
      CGroupsHandler cgroups = mockCGroupsHandler(false);
      // The version and this will be passed to sleep as arguments, and sleep
      // sleeps for the sum of its operands.
      when(cgroups.getPathForCGroup(any(), any())).thenReturn("10000");

      Runnable handler = mock(Runnable.class);
      doNothing().when(handler).run();

      CGroupElasticMemoryController controller =
          CGroupElasticMemoryController.create(
              conf,
              null,
              cgroups,
              true,
              false,
              10000,
              handler
          );
      long start = System.currentTimeMillis();
      service.submit(() -> {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ex) {
          assertTrue("Wait interrupted.", false);
        }
        LOG.info(String.format("Calling process destroy in %d ms",
            System.currentTimeMillis() - start));
        controller.stopListening();
        LOG.info("Called process destroy.");
      });
      controller.run();
    } finally {
      service.shutdown();
    }
  }

  /**
   * Test that DefaultOOMHandler is instantiated correctly in
   * the elastic constructor.
   * @throws Exception Could not set up elastic memory control.
   */
  @Test
  public void testDefaultConstructor() throws Exception {
    CGroupElasticMemoryController.create(
        conf, null, mockCGroupsHandler(false), true, false, 10);
  }
}
