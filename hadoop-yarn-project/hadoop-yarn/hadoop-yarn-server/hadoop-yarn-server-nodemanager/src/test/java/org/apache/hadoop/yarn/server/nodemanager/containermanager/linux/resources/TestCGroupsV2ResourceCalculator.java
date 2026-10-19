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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.CpuTimeTracker;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for CGroupsV2ResourceCalculator.
 */
public class TestCGroupsV2ResourceCalculator {

  private Path root;

  @Before
  public void before() throws IOException {
    root = Files.createTempDirectory("TestCGroupsV2ResourceCalculator");
  }

  @After
  public void after() throws IOException {
    FileUtils.deleteDirectory(root.toFile());
  }

  @Test
  public void testPidNotFound() {
    CGroupsV2ResourceCalculator calculator = createCalculator();
    calculator.updateProcessTree();
    assertEquals(-1, calculator.getRssMemorySize(), 0L);
  }

  @Test
  public void readFiles() throws IOException {
    Files.createDirectories(root.resolve("proc/42"));
    Files.createDirectories(root.resolve("mount/cgroup2/yarn/container_1"));

    writeToFile("proc/42/cgroup",
        "0::/container_1");
    // slab is a decoy: it is part of kernel, which is reported on its own
    // line, and must not be summed a second time.
    writeToFile("mount/cgroup2/yarn/container_1/memory.stat",
        "anon 22000",
        "file_mapped 3000",
        "kernel 5000",
        "slab 1774128");
    writeToFile("mount/cgroup2/yarn/container_1/memory.swap.current",
        "11000");
    writeToFile("mount/cgroup2/yarn/container_1/cpu.stat",
        "usage_usec 333",
        "meaning_of_life 42");

    CGroupsV2ResourceCalculator calculator = createCalculator();
    when(calculator.getcGroupsHandler().getCGroupV2MountPath())
        .thenReturn(root.resolve("mount/cgroup2/yarn").toString());
    when(calculator.getcGroupsHandler().getRelativePathForCGroup(eq("/container_1")))
        .thenReturn("container_1");

    calculator.updateProcessTree();

    assertEquals(333000L, calculator.getCumulativeCpuTime(), 0L);
    // anon + file_mapped + kernel
    assertEquals(30000L, calculator.getRssMemorySize(), 0L);
    assertEquals(11000L, calculator.getVirtualMemorySize(), 0L);
    assertEquals(-1L, calculator.getRssMemorySize(2), 0L);
    assertEquals(-1L, calculator.getVirtualMemorySize(2), 0L);
  }

  /**
   * The kernel key only exists since Linux 5.18, the measure has to degrade
   * to anon + file_mapped instead of becoming unavailable.
   */
  @Test
  public void readFilesWithoutKernelMemory() throws IOException {
    Files.createDirectories(root.resolve("proc/42"));
    Files.createDirectories(root.resolve("mount/cgroup2/yarn/container_1"));

    writeToFile("proc/42/cgroup",
        "0::/container_1");
    writeToFile("mount/cgroup2/yarn/container_1/memory.stat",
        "anon 22000",
        "file_mapped 3000",
        "slab 1774128");

    CGroupsV2ResourceCalculator calculator = createCalculator();
    when(calculator.getcGroupsHandler().getCGroupV2MountPath())
        .thenReturn(root.resolve("mount/cgroup2/yarn").toString());
    when(calculator.getcGroupsHandler().getRelativePathForCGroup(eq("/container_1")))
        .thenReturn("container_1");

    calculator.updateProcessTree();

    assertEquals(25000L, calculator.getRssMemorySize(), 0L);
  }

  /**
   * With docker /proc/&lt;pid&gt;/cgroup points at the cgroup the docker daemon
   * created below the YARN container one. The statistics have to be read at
   * the container_x level, which is the hierarchical parent of it.
   */
  @Test
  public void readFilesWithDocker() throws IOException {
    Files.createDirectories(root.resolve("proc/42"));
    Files.createDirectories(root.resolve("mount/cgroup2/yarn/container_1"));

    writeToFile("proc/42/cgroup",
        "0::/container_1"
            + "/de5b77f2cdf6b5e87de9c23da5bc07de0d69f010af503dcd15832a3389c31565");
    writeToFile("mount/cgroup2/yarn/container_1/memory.stat",
        "anon 22000",
        "file_mapped 3000",
        "kernel 5000");

    CGroupsV2ResourceCalculator calculator = createCalculator();
    when(calculator.getcGroupsHandler().getCGroupV2MountPath())
        .thenReturn(root.resolve("mount/cgroup2/yarn").toString());

    calculator.updateProcessTree();

    assertEquals(30000L, calculator.getRssMemorySize(), 0L);
  }

  /**
   * Pid 1 is the availability probe of ContainersMonitorImpl, not a
   * container: the yarn hierarchy root is read instead.
   */
  @Test
  public void readFilesOfYarnHierarchyRoot() throws IOException {
    Files.createDirectories(root.resolve("proc/1"));
    Files.createDirectories(root.resolve("mount/cgroup2/hadoop-yarn"));

    writeToFile("proc/1/cgroup",
        "0::/init.scope");
    writeToFile("mount/cgroup2/hadoop-yarn/memory.stat",
        "anon 22000",
        "file_mapped 3000",
        "kernel 5000");

    CGroupsV2ResourceCalculator calculator = createCalculator("1");
    when(calculator.getcGroupsHandler().getCGroupV2MountPath())
        .thenReturn(root.resolve("mount/cgroup2").toString());
    when(calculator.getcGroupsHandler().getRelativePathForCGroup(eq("")))
        .thenReturn("hadoop-yarn/");

    calculator.updateProcessTree();

    assertEquals(30000L, calculator.getRssMemorySize(), 0L);
  }

  private CGroupsV2ResourceCalculator createCalculator() {
    return createCalculator("42");
  }

  private CGroupsV2ResourceCalculator createCalculator(String pid) {
    CGroupsV2ResourceCalculator calculator = new CGroupsV2ResourceCalculator(pid);
    calculator.setCpuTimeTracker(mock(CpuTimeTracker.class));
    calculator.setcGroupsHandler(mock(CGroupsHandler.class));
    calculator.setProcFs(root.toString() + "/proc/");
    calculator.setJiffyLengthMs(1_000);
    return calculator;
  }

  private void writeToFile(String path, String... lines) throws IOException {
    FileUtils.writeStringToFile(
        root.resolve(path).toFile(),
        Arrays.stream(lines).collect(Collectors.joining(System.lineSeparator())),
        StandardCharsets.UTF_8);
  }
}
