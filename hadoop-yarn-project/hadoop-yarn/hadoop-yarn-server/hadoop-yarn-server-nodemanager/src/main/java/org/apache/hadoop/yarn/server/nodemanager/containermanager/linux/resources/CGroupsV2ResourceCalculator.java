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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

/**
 * A Cgroup version 2 file-system based Resource calculator without the process tree features.
 *
 * Warning: this implementation will not work properly when configured
 * using the mapreduce.job.process-tree.class job property.
 * Theoretically the ResourceCalculatorProcessTree can be configured
 * using the mapreduce.job.process-tree.class job property, however it
 * has a dependency on an instantiated ResourceHandlerModule,
 * which is only initialised in the NodeManager process and not in the containers.
 *
 * Limitation:
 * The ResourceCalculatorProcessTree class can be configured using the
 * mapreduce.job.process-tree.class property within a MapReduce job.
 * However, it is important to note that instances of ResourceCalculatorProcessTree operate
 * within the context of a MapReduce task. This presents a limitation:
 * these instances do not have access to the ResourceHandlerModule,
 * which is only initialized within the NodeManager process
 * and not within individual containers where MapReduce tasks execute.
 * As a result, the current implementation of ResourceCalculatorProcessTree is incompatible
 * with the mapreduce.job.process-tree.class property. This incompatibility arises
 * because the ResourceHandlerModule is essential for managing and monitoring resource usage,
 * and without it, the ResourceCalculatorProcessTree cannot function as intended
 * within the confines of a MapReduce task. Therefore, any attempts to utilize this class
 * through the mapreduce.job.process-tree.class property
 * will not succeed under the current architecture.
 */
public class CGroupsV2ResourceCalculator extends AbstractCGroupsResourceCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(CGroupsV2ResourceCalculator.class);

  /**
   * <a href="https://docs.kernel.org/admin-guide/cgroup-v2.html#cpu-interface-files">DOC</a>
   *
   * ...
   * cpu.stat
   *  A read-only flat-keyed file. This file exists whether the controller is enabled or not.
   *  It always reports the following three stats:
   *  - usage_usec
   *  - user_usec
   *  - system_usec
   *  ...
   *
   */
  private static final String CPU_STAT = "cpu.stat#usage_usec";

  /**
   * <a href="https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files">DOC</a>
   *
   * ...
   * memory.stat
   *  A read-only flat-keyed file which exists on non-root cgroups.
   *  This breaks down the cgroup’s memory footprint into different types of memory,
   *  type-specific details, and other information on the state
   *  and past events of the memory management system.
   *  All memory amounts are in bytes.
   *  ...
   *  anon
   *   Amount of memory used in anonymous mappings such as brk(), sbrk(), and mmap(MAP_ANONYMOUS)
   *  file_mapped
   *   Amount of cached filesystem data mapped with mmap()
   *  kernel (since Linux 5.18)
   *   Amount of total kernel memory, including (kernel_stack, pagetables,
   *   percpu, vmalloc, slab) in addition to other kernel memory use cases
   * ...
   *
   */
  private static final String MEM_STAT = "memory.stat";

  /**
   * The cgroup v1 names of these counters are rss, mapped_file and
   * memory.kmem.usage_in_bytes: the memory a container really needs under
   * pressure, page cache excluded. See the "A proper memory measure for
   * cgroup based memory management on Yarn" design note and
   * {@link CGroupsResourceCalculator}. In v2 memory.stat is hierarchical by
   * default, so there is no total_ prefixed variant to pick.
   * On kernels older than 5.18 the kernel key is absent and the sum
   * degrades to anon + file_mapped.
   */
  private static final List<String> RSS_MEMORY_KEYS = Arrays.asList(
      MEM_STAT + "#anon",
      MEM_STAT + "#file_mapped",
      MEM_STAT + "#kernel"
  );

  /**
   * <a href="https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files">DOC</a>
   *
   * ...
   * memory.swap.current
   *  A read-only single value file which exists on non-root cgroups.
   *  The total amount of swap currently being used by the cgroup and its descendants.
   * ...
   *
   */
  private static final String MEMSW_STAT = "memory.swap.current";

  public CGroupsV2ResourceCalculator(String pid) {
    super(
        pid,
        Collections.singletonList(CPU_STAT),
        RSS_MEMORY_KEYS,
        MEMSW_STAT
    );
  }

  @Override
  protected List<Path> getCGroupFilesToLoadInStats() {
    List<Path> result = new ArrayList<>();
    try (Stream<Path> cGroupFiles = Files.list(getCGroupPath())){
      cGroupFiles.forEach(result::add);
    } catch (IOException e) {
      LOG.debug("Failed to list cgroup files for pid: " + getPid(), e);
    }
    LOG.debug("Found cgroup files for pid {} is {}", getPid(), result);
    return  result;
  }

  private Path getCGroupPath() throws IOException {
    String mountPath = getcGroupsHandler().getCGroupV2MountPath();
    if (YARN_HIERARCHY_PID.equals(getPid())) {
      // Not a container: this is the availability probe of
      // ContainersMonitorImpl, it has to land on the yarn hierarchy root.
      return Paths.get(mountPath, getcGroupsHandler().getRelativePathForCGroup(""));
    }

    // example line: 0::/hadoop-yarn/container_1
    // with docker:  0::/hadoop-yarn/container_1/<64 hex characters of docker id>
    String cGroupPath = StringUtils.substringAfterLast(
        readLinesFromCGroupFileFromProcDir().get(0), ":");
    // The docker child cgroup is cut off: the statistics are read at the YARN
    // container level, which is hierarchical and so includes the docker one.
    Matcher containerId = CONTAINER_ID_PATTERN.matcher(cGroupPath);
    if (containerId.find()) {
      return Paths.get(mountPath, cGroupPath.substring(0, containerId.end(1)));
    }

    LOG.error("Found no container id in the cgroup path {} of pid {}",
        cGroupPath, getPid());
    return Paths.get(mountPath, cGroupPath);
  }
}
