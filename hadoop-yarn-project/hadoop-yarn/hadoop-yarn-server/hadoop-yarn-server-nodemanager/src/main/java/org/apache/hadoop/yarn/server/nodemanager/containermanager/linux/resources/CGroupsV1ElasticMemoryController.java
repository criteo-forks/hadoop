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

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_OOM_CONTROL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_NO_LIMIT;

/**
 * Elastic memory control on cgroup v1. The kernel OOM killer is disabled on
 * the root YARN cgroup, so the kernel freezes the cgroup at its limit and
 * reports under_oom instead of killing anything, which leaves the choice of a
 * victim to the node manager.
 */
class CGroupsV1ElasticMemoryController extends CGroupElasticMemoryController {

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
  }

  @Override
  protected String[] oomListenerArgs() {
    return new String[] {"1", yarnCGroupPath};
  }

  @Override
  protected boolean isUnderOOM() throws ResourceHandlerException {
    String underOOM = cgroups.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL);
    return underOOM.contains(CGroupsHandler.UNDER_OOM);
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
