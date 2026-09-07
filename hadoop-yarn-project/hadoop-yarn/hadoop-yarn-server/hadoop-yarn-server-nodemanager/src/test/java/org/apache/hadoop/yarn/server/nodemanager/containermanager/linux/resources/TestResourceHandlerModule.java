/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestResourceHandlerModule {
  private static final Logger LOG =
       LoggerFactory.getLogger(TestResourceHandlerModule.class);
  private Configuration emptyConf;
  private Configuration networkEnabledConf;
  private File tmpDir;

  @Before
  public void setup() throws Exception {
    emptyConf = new YarnConfiguration();
    networkEnabledConf = new YarnConfiguration();

    networkEnabledConf.setBoolean(YarnConfiguration.NM_NETWORK_RESOURCE_ENABLED,
        true);
    ResourceHandlerModule.nullifyResourceHandlerChain();
    ResourceHandlerModule.nullifyCGroupHandlers();

    tmpDir = new File(System.getProperty("test.build.data"),
        "resource-handler-module");
    FileUtils.deleteQuietly(tmpDir);
    assertTrue(tmpDir.mkdirs());
  }

  @After
  public void teardown() {
    FileUtil.fullyDelete(tmpDir);
  }

  @Test
  public void testOutboundBandwidthHandler() {
    try {
      //This resourceHandler should be non-null only if network as a resource
      //is explicitly enabled
      OutboundBandwidthResourceHandler resourceHandler = ResourceHandlerModule
          .initOutboundBandwidthResourceHandler(emptyConf);
      Assert.assertNull(resourceHandler);

      //When network as a resource is enabled this should be non-null
      resourceHandler = ResourceHandlerModule
          .initOutboundBandwidthResourceHandler(networkEnabledConf);
      Assert.assertNotNull(resourceHandler);

      //Ensure that outbound bandwidth resource handler is present in the chain
      ResourceHandlerChain resourceHandlerChain = ResourceHandlerModule
          .getConfiguredResourceHandlerChain(networkEnabledConf,
              mock(Context.class));
      if (resourceHandlerChain != null) {
        List<ResourceHandler> resourceHandlers = resourceHandlerChain
            .getResourceHandlerList();
        //Exactly one resource handler in chain
        assertThat(resourceHandlers).hasSize(1);
        //Same instance is expected to be in the chain.
        Assert.assertTrue(resourceHandlers.get(0) == resourceHandler);
      } else {
        Assert.fail("Null returned");
      }
    } catch (ResourceHandlerException e) {
      Assert.fail("Unexpected ResourceHandlerException: " + e);
    }
  }

  @Test
  public void testDiskResourceHandler() throws Exception {

    DiskResourceHandler handler =
        ResourceHandlerModule.initDiskResourceHandler(emptyConf);
    Assert.assertNull(handler);

    Configuration diskConf = new YarnConfiguration();
    diskConf.setBoolean(YarnConfiguration.NM_DISK_RESOURCE_ENABLED, true);

    handler = ResourceHandlerModule.initDiskResourceHandler(diskConf);
    Assert.assertNotNull(handler);

    ResourceHandlerChain resourceHandlerChain =
        ResourceHandlerModule.getConfiguredResourceHandlerChain(diskConf,
            mock(Context.class));
    if (resourceHandlerChain != null) {
      List<ResourceHandler> resourceHandlers =
          resourceHandlerChain.getResourceHandlerList();
      // Exactly one resource handler in chain
      assertThat(resourceHandlers).hasSize(1);
      // Same instance is expected to be in the chain.
      Assert.assertTrue(resourceHandlers.get(0) == handler);
    } else {
      Assert.fail("Null returned");
    }
  }

  /**
   * On a v2 only node the v1 handler is never created, so
   * getCGroupsHandler() has to return the v2 one, otherwise everything
   * relying on it (docker cgroup parent, container executor cgroup root,
   * resource calculator, elastic memory control) sees a null handler.
   */
  @Test
  public void testCGroupsHandlerOnV2OnlyNode() throws Exception {
    File v2MountPath = createV2MountPath("v2-only",
        "cpuset cpu io memory hugetlb pids rdma misc");

    Configuration conf = createCGroupsV2Configuration(
        v2MountPath.getAbsolutePath(), v2MountPath.getAbsolutePath());
    ResourceHandlerModule.getConfiguredResourceHandlerChain(conf,
        mock(Context.class));

    CGroupsHandler handler = ResourceHandlerModule.getCGroupsHandler();
    Assert.assertNotNull("A cgroups handler is expected on a v2 only node",
        handler);
    Assert.assertTrue("The memory controller is served by cgroup v2",
        handler.isCGroupsV2());
    Assert.assertTrue(ResourceHandlerModule.isCGroupsV2Enabled());
    Assert.assertEquals("hadoop-yarn",
        ResourceHandlerModule.getCgroupsRelativeRoot());
  }

  /**
   * Mixed mode: the memory controller is not delegated in the v2 hierarchy,
   * so the memory limits live in v1 and getCGroupsHandler() has to return
   * the v1 handler.
   */
  @Test
  public void testCGroupsHandlerOnMixedNode() throws Exception {
    File v1MountPath = new File(tmpDir, "mixed");
    assertTrue(new File(v1MountPath, "memory").mkdirs());
    File v2MountPath = createV2MountPath("mixed/unified",
        "cpuset cpu io hugetlb pids rdma misc");

    Configuration conf = createCGroupsV2Configuration(
        v1MountPath.getAbsolutePath(), v2MountPath.getAbsolutePath());
    ResourceHandlerModule.getConfiguredResourceHandlerChain(conf,
        mock(Context.class));

    CGroupsHandler handler = ResourceHandlerModule.getCGroupsHandler();
    Assert.assertNotNull("A cgroups handler is expected on a mixed node",
        handler);
    Assert.assertFalse("The memory controller is served by cgroup v1",
        handler.isCGroupsV2());
    Assert.assertEquals("hadoop-yarn",
        ResourceHandlerModule.getCgroupsRelativeRoot());
  }

  private File createV2MountPath(String relativePath, String controllers)
      throws Exception {
    File v2MountPath = new File(tmpDir, relativePath);
    assertTrue(v2MountPath.mkdirs());
    FileUtils.writeStringToFile(
        new File(v2MountPath, CGroupsHandler.CGROUP_CONTROLLERS_FILE),
        controllers + "\n", StandardCharsets.UTF_8);
    return v2MountPath;
  }

  private Configuration createCGroupsV2Configuration(String mountPath,
      String v2MountPath) {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_V2_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT, false);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH,
        mountPath);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_V2_MOUNT_PATH,
        v2MountPath);
    conf.setBoolean(YarnConfiguration.NM_MEMORY_RESOURCE_ENABLED, true);
    return conf;
  }
}