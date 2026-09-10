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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The cgroup v2 memory footprint measure shared by
 * {@link CGroupsV2ElasticMemoryController} and {@link DefaultOOMHandler}, plus
 * the parsing of the v2 limit and flat-keyed file formats.
 *
 * There is a single definition of what "the memory a cgroup needs" means, and
 * it lives here: {@link CGroupsV2ResourceCalculator} builds its own key list
 * from {@link #RSS_MEMORY_KEYS}.
 */
final class CGroupsV2MemoryStat {

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
  static final List<String> RSS_MEMORY_KEYS = Collections.unmodifiableList(
      Arrays.asList("anon", "file_mapped", "kernel"));

  private CGroupsV2MemoryStat() {
  }

  /**
   * Parse a value of memory.high, memory.max or memory.swap.max. These files
   * read back the literal "max" when there is no limit, which is also what
   * the controller writes into them on cleanup, so every parser of them has
   * to accept it.
   *
   * @param value the file contents
   * @return the limit in bytes, {@link Long#MAX_VALUE} when unlimited
   */
  static long parseLimit(String value) {
    String trimmed = value.trim();
    return CGroupsHandler.CGROUP_V2_NO_LIMIT.equals(trimmed)
        ? Long.MAX_VALUE
        : Long.parseLong(trimmed);
  }

  /**
   * Parse a flat-keyed cgroup v2 file, one "key value" pair per line. Lines
   * whose value is not a number are skipped rather than failing the parse:
   * the kernel adds keys to these files over time.
   *
   * @param content the file contents
   * @return the numeric entries of the file
   */
  static Map<String, Long> parseFlatKeyed(String content) {
    Map<String, Long> parsed = new HashMap<>();
    for (String line : content.split("\n")) {
      String[] parts = line.trim().split("\\s+");
      if (parts.length < 2) {
        continue;
      }
      try {
        parsed.put(parts[0], Long.parseLong(parts[1]));
      } catch (NumberFormatException ex) {
        // Not a counter line, ignore it.
      }
    }
    return parsed;
  }

  /**
   * Sum the requested keys. A key the kernel does not expose counts as 0, it
   * is not a failure: memory.stat's kernel key only exists since Linux 5.18.
   *
   * @param parsed the parsed file
   * @param keys the keys to add up
   * @return the sum in bytes
   */
  static long sum(Map<String, Long> parsed, List<String> keys) {
    long total = 0;
    for (String key : keys) {
      total += parsed.getOrDefault(key, 0L);
    }
    return total;
  }

  /**
   * Read the memory footprint of a cgroup: the {@link #RSS_MEMORY_KEYS} of its
   * memory.stat, plus the swap it uses when the enforced dimension is virtual
   * memory. memory.stat is hierarchical, so this includes the docker child
   * cgroup of a container.
   *
   * This is deliberately not memory.current, which counts the page cache: the
   * cache is reclaimable, so a cgroup over its memory.current is not
   * necessarily a cgroup that cannot make progress.
   *
   * @param cgroups the handler to read through
   * @param cGroupId the cgroup, "" for the root YARN cgroup
   * @param includeSwap whether swap counts towards the footprint
   * @return the footprint in bytes
   * @throws ResourceHandlerException memory.stat could not be read
   */
  static long readFootprint(CGroupsHandler cgroups, String cGroupId,
      boolean includeSwap) throws ResourceHandlerException {
    long footprint = sum(
        parseFlatKeyed(cgroups.getCGroupParam(
            CGroupsHandler.CGroupController.MEMORY, cGroupId,
            CGroupsHandler.CGROUP_MEMORY_STAT)),
        RSS_MEMORY_KEYS);
    if (includeSwap) {
      footprint += Long.parseLong(cgroups.getCGroupParam(
          CGroupsHandler.CGroupController.MEMORY, cGroupId,
          CGroupsHandler.CGROUP_MEMORY_SWAP_CURRENT).trim());
    }
    return footprint;
  }
}
