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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for CGroupsResourceCalculator.
 */
public class TestCGroupsResourceCalculator {

  private Path root;

  @Before
  public void before() throws IOException {
    root = Files.createTempDirectory("TestCGroupsResourceCalculator");
  }

  @After
  public void after() throws IOException {
    FileUtils.deleteDirectory(root.toFile());
  }

  @Test
  public void testNoMemoryCGgroupMount() throws Exception {
    writeToFile("proc/41/cgroup",
        "7:devices:/yarn/container_1",
        "6:cpuacct,cpu:/yarn/container_1",
        "5:pids:/yarn/container_1"
    );

    CGroupsResourceCalculator calculator = createCalculator();
    calculator.updateProcessTree();
    assertEquals(-1, calculator.getVirtualMemorySize());
  }

  @Test
  public void testCGgroupNotFound() throws Exception {
    writeToFile("proc/41/cgroup",
        "7:devices:/yarn/container_1",
        "6:cpuacct,cpu:/yarn/container_1",
        "5:pids:/yarn/container_1",
        "4:memory:/yarn/container_1"
    );

    CGroupsResourceCalculator calculator = createCalculator();
    calculator.updateProcessTree();
    assertEquals(-1, calculator.getCumulativeCpuTime());
  }

  @Test
  public void testParsing() throws Exception {
    writeToFile("proc/41/cgroup",
        "7:devices:/yarn/container_1",
        "6:cpuacct,cpu:/yarn/container_1",
        "5:pids:/yarn/container_1",
        "4:memory:/yarn/container_1"
    );

    writeToFile("mount/cgroup/yarn/container_1/cpuacct.stat",
        "Can you handle this?",
        "user 5415",
        "system 3632"
    );

    CGroupsResourceCalculator calculator = createCalculator();
    calculator.updateProcessTree();
    assertEquals(90470, calculator.getCumulativeCpuTime());

    // Only a few lines of memory.stat are written, the decoys are there to
    // check that the non hierarchical counters are skipped: the expected
    // value must come from total_rss and total_mapped_file only.
    writeToFile("mount/cgroup/yarn/container_1/memory.stat",
        "cache 1000000",
        "rss 2000000",
        "rss_huge 0",
        "shmem 172032",
        "mapped_file 100000",
        "total_cache 10000000",
        "total_rss 20000000",
        "total_rss_huge 4469030912",
        "total_shmem 2412544",
        "total_mapped_file 200000",
        "total_dirty 1060474880"
    );
    writeToFile("mount/cgroup/yarn/container_1/memory.kmem.usage_in_bytes",
        "10000"
    );

    calculator.updateProcessTree();
    // total_rss + total_mapped_file + kernel memory
    assertEquals(20210000, calculator.getRssMemorySize());
    assertEquals(-1, calculator.getVirtualMemorySize());

    writeToFile("mount/cgroup/yarn/container_1/memory.memsw.usage_in_bytes",
        "418496513"
    );

    calculator.updateProcessTree();
    assertEquals(20210000, calculator.getRssMemorySize());
    assertEquals(418496513, calculator.getVirtualMemorySize());
  }

  /**
   * With docker /proc/&lt;pid&gt;/cgroup points at the cgroup the docker daemon
   * created below the YARN container one. The statistics have to be read at
   * the container_x level, which is the hierarchical parent of it.
   */
  @Test
  public void testParsingWithDocker() throws Exception {
    String dockerId =
        "/de5b77f2cdf6b5e87de9c23da5bc07de0d69f010af503dcd15832a3389c31565";
    writeToFile("proc/41/cgroup",
        "7:devices:/yarn/container_1" + dockerId,
        "6:cpuacct,cpu:/yarn/container_1" + dockerId,
        "5:pids:/yarn/container_1" + dockerId,
        "4:memory:/yarn/container_1" + dockerId
    );

    writeToFile("mount/cgroup/yarn/container_1/cpuacct.stat",
        "user 5415",
        "system 3632"
    );
    writeToFile("mount/cgroup/yarn/container_1/memory.stat",
        "total_rss 20000000",
        "total_mapped_file 200000"
    );
    writeToFile("mount/cgroup/yarn/container_1/memory.kmem.usage_in_bytes",
        "10000"
    );

    CGroupsResourceCalculator calculator = createCalculator();
    calculator.updateProcessTree();
    assertEquals(90470, calculator.getCumulativeCpuTime());
    assertEquals(20210000, calculator.getRssMemorySize());
  }

  /**
   * Pid 1 is the availability probe of ContainersMonitorImpl, not a
   * container: the yarn hierarchy root is read instead.
   */
  @Test
  public void testParsingYarnHierarchyRoot() throws Exception {
    writeToFile("proc/1/cgroup",
        "7:devices:/",
        "6:cpuacct,cpu:/",
        "5:pids:/",
        "4:memory:/"
    );

    writeToFile("mount/cgroup/yarn/cpuacct.stat",
        "user 5415",
        "system 3632"
    );

    CGroupsResourceCalculator calculator = createCalculator("1");
    calculator.updateProcessTree();
    assertEquals(90470, calculator.getCumulativeCpuTime());
  }

  private CGroupsResourceCalculator createCalculator() {
    return createCalculator("41");
  }

  private CGroupsResourceCalculator createCalculator(String pid) {
    CGroupsResourceCalculator calculator = new CGroupsResourceCalculator(pid);
    calculator.setCpuTimeTracker(mock(CpuTimeTracker.class));
    calculator.setcGroupsHandler(mock(CGroupsHandler.class));
    when(calculator.getcGroupsHandler().getRelativePathForCGroup("container_1"))
        .thenReturn("/yarn/container_1");
    when(calculator.getcGroupsHandler().getRelativePathForCGroup(""))
        .thenReturn("/yarn/");
    when(calculator.getcGroupsHandler().getControllerPath(any()))
        .thenReturn(root.resolve("mount/cgroup").toString());
    calculator.setProcFs(root.toString() + "/proc/");
    calculator.setJiffyLengthMs(10);
    return calculator;
  }

  private void writeToFile(String path, String... lines) throws IOException {
    FileUtils.writeStringToFile(
        root.resolve(path).toFile(),
        Arrays.stream(lines).collect(Collectors.joining(System.lineSeparator())),
        StandardCharsets.UTF_8);
  }
}
