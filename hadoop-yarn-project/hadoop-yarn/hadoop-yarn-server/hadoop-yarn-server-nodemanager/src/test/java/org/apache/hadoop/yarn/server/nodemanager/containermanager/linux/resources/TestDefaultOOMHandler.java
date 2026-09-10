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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_KILL_FILE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_STAT;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PROCS_FILE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_OOM_CONTROL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_USAGE_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test default out of memory handler.
 */
public class TestDefaultOOMHandler {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  /**
   * Test an OOM situation where there are no containers that can be killed.
   */
  @Test(expected = YarnRuntimeException.class)
  public void testExceptionThrownWithNoContainersToKill() throws Exception {
    Context context = mock(Context.class);

    when(context.getContainers()).thenReturn(new ConcurrentHashMap<>(0));

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");

    DefaultOOMHandler handler = new DefaultOOMHandler(context, false) {
      @Override
      protected CGroupsHandler getCGroupsHandler() {
        return cGroupsHandler;
      }
    };

    handler.run();
  }

  /**
   * Test an OOM situation where there are no running containers that
   * can be killed.
   */
  @Test(expected = YarnRuntimeException.class)
  public void testExceptionThrownWithNoRunningContainersToKill()
      throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, true, 1L, false);
    containers.put(c1.getContainerId(), c1);

    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");

    DefaultOOMHandler handler = new DefaultOOMHandler(context, false) {
      @Override
      protected CGroupsHandler getCGroupsHandler() {
        return cGroupsHandler;
      }
    };

    handler.run();
  }

  /**
   * We have two running guaranteed containers, both of which are out of limit.
   * We should kill the later one.
   */
  @Test
  public void testBothRunningGuaranteedContainersOverLimitUponOOM()
      throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, true, 1L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, true, 2L, true);
    containers.put(c2.getContainerId(), c2);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);


    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(11));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();


    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }

  /**
   * We have two running GUARANTEED containers, one of which is out of limit.
   * We should kill the one that's out of its limit. This should
   * happen even if it was launched earlier than the other one.
   */
  @Test
  public void testOneGuaranteedContainerOverLimitUponOOM() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, true, 2L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, true, 1L, true);
    containers.put(c2.getContainerId(), c2);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));

    // container c2 is out of its limit
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(11));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }

  /**
   * We have two running GUARANTEE containers, neither of which is out of limit.
   * We should kill the later launched one.
   */
  @Test
  public void testNoGuaranteedContainerOverLimitOOM() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, true, 1L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, true, 2L, true);
    containers.put(c2.getContainerId(), c2);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }

  /**
   * We have two OPPORTUNISTIC containers, one running and the other not.
   * We should kill the running one.
   */
  @Test
  public void testKillOnlyRunningContainersUponOOM() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, false, 1L, false);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, false, 2L, true);
    containers.put(c2.getContainerId(), c2);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }


  /**
   * We have two 'running' OPPORTUNISTIC containers. Killing the most-
   * recently launched one fails because its cgroup.procs file is not
   * available. The other OPPORTUNISTIC containers should be killed in
   * this case.
   */
  @Test
  public void  testKillOpportunisticContainerWithKillFailuresUponOOM()
      throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, false, 1L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, false, 2L, true);
    containers.put(c2.getContainerId(), c2);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    // c2 process has not started, hence no cgroup.procs file yet
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenThrow(
            new ResourceHandlerException(CGROUP_PROCS_FILE + " not found"));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c1)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }

  /**
   * We have two 'running' OPPORTUNISTIC containers and one GUARANTEED
   * container. Killing two OPPORTUNISTIC containers fails because they
   * have not really started running as processes since the root cgroup
   * is under oom. We should try to kill one container successfully. In
   * this case, the GUARANTEED container should be killed.
   */
  @Test
  public void testKillGuaranteedContainerWithKillFailuresUponOOM()
      throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, false, 1L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, false, 2L, true);
    containers.put(c2.getContainerId(), c2);
    Container c3 = createContainer(3, true, 2L, true);
    containers.put(c3.getContainerId(), c3);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");
    // c1 process has not started, hence no cgroup.procs file yet
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenThrow(
            new ResourceHandlerException(CGROUP_PROCS_FILE + " not found"));
    // c2 process has not started, hence no cgroup.procs file yet
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenThrow(
            new ResourceHandlerException(CGROUP_PROCS_FILE + " not found"));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c3)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }

  /**
   * Test an OOM situation where no containers are killed successfully.
   *
   * We have two 'running' containers, none of which are actually
   * running as processes. Their cgroup.procs file is not available,
   * so kill them won't succeed.
   */
  @Test(expected = YarnRuntimeException.class)
  public void testExceptionThrownWhenNoContainersKilledSuccessfully()
      throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, false, 1L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, false, 2L, true);
    containers.put(c2.getContainerId(), c2);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");
    // c1 process has not started, hence no cgroup.procs file yet
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenThrow(
            new ResourceHandlerException(CGROUP_PROCS_FILE + " not found"));
    // c2 process has not started, hence no cgroup.procs file yet
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenThrow(
            new ResourceHandlerException(CGROUP_PROCS_FILE + " not found"));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();
  }

  /**
   * We have two running opportunistic containers, both of which are out of
   * limit. We should kill the later one.
   */
  @Test
  public void testBothOpportunisticContainersOverLimitUponOOM()
      throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, false, 1L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, false, 2L, true);
    containers.put(c2.getContainerId(), c2);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);


    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(11));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();


    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }

  /**
   * We have two running OPPORTUNISTIC containers, one of which is out of
   * limit. We should kill the one that's out of its limit. This should
   * happen even if it was launched earlier than the other one.
   */
  @Test
  public void testOneOpportunisticContainerOverLimitUponOOM() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, false, 2L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, false, 1L, true);
    containers.put(c2.getContainerId(), c2);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    // contnainer c2 is out of its limit
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(11));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }

  /**
   * We have two running OPPORTUNISTIC containers, neither of which is out of
   * limit. We should kill the later one.
   */
  @Test
  public void testNoOpportunisticContainerOverLimitOOM() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, false, 1L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, false, 2L, true);
    containers.put(c2.getContainerId(), c2);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }

  /**
   * We have two running OPPORTUNISTIC containers and one running GUARANTEED
   * container. One of the OPPORTUNISTIC container is out of limit.
   * OOM is resolved after killing the OPPORTUNISTIC container that
   * exceeded its limit even though it is launched earlier than the
   * other OPPORTUNISTIC container.
   */
  @Test
  public void testKillOneOverLimitOpportunisticContainerUponOOM()
      throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    int currentContainerId = 0;
    Container c1 = createContainer(currentContainerId++, false, 2, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(currentContainerId++, false, 1, true);
    containers.put(c2.getContainerId(), c2);
    Container c3 = createContainer(currentContainerId++, true, 1, true);
    containers.put(c3.getContainerId(), c3);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));

    // container c2 is out of its limit
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(11));

    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1236").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }
  /**
   * We have two running OPPORTUNISTIC containers and one running GUARANTEED
   * container. None of the containers exceeded its memory limit.
   * OOM is resolved after killing the most recently launched OPPORTUNISTIC
   * container.
   */
  @Test
  public void testKillOneLaterOpportunisticContainerUponOOM() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    int currentContainerId = 0;
    Container c1 = createContainer(currentContainerId++, false, 1, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(currentContainerId++, false, 2, true);
    containers.put(c2.getContainerId(), c2);
    Container c3 = createContainer(currentContainerId++, true, 1, true);
    containers.put(c3.getContainerId(), c3);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1236").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }

  /**
   * We have two running OPPORTUNISTIC containers and one running GUARANTEED
   * container. One of the OPPORTUNISTIC container is out of limit.
   * OOM is resolved after killing both OPPORTUNISTIC containers.
   */
  @Test
  public void testKillBothOpportunisticContainerUponOOM() throws Exception {
    int currentContainerId = 0;

    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(currentContainerId++, false, 2, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(currentContainerId++, false, 1, true);
    containers.put(c2.getContainerId(), c2);
    Container c3 = createContainer(currentContainerId++, true, 1, true);
    containers.put(c3.getContainerId(), c3);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1236").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));


    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c1)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1234")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(2)).signalContainer(any());
  }

  /**
   * We have two running OPPORTUNISTIC containers and one running GUARANTEED
   * container. The GUARANTEED container is out of limit. OOM is resolved
   * after first killing the two OPPORTUNISTIC containers and then the
   * GUARANTEED container.
   */
  @Test
  public void testKillGuaranteedContainerUponOOM() throws Exception {
    int currentContainerId = 0;

    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(currentContainerId++, false, 2, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(currentContainerId++, false, 1, true);
    containers.put(c2.getContainerId(), c2);
    Container c3 = createContainer(currentContainerId++, true, 1, true);
    containers.put(c3.getContainerId(), c3);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1236").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(11));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1234")
            .setContainer(c1)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c1)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1236")
            .setContainer(c1)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(3)).signalContainer(any());
  }

  /**
   * We have two running OPPORTUNISTIC containers and one running GUARANTEED
   * container. None of the containers exceeded its memory limit.
   * OOM is resolved after killing all running containers.
   */
  @Test
  public void testKillAllContainersUponOOM() throws Exception {
    int currentContainerId = 0;

    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(currentContainerId++, false, 1, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(currentContainerId++, false, 2, true);
    containers.put(c2.getContainerId(), c2);
    Container c3 = createContainer(currentContainerId++, true, 1, true);
    containers.put(c3.getContainerId(), c3);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 0");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1236").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1234")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c1)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1236")
            .setContainer(c3)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(3)).signalContainer(any());
  }

  /**
   * We have two running OPPORTUNISTIC containers and one running
   * GUARANTEED container.
   * None of the containers exceeded its memory limit.
   * OOM is not resolved even after killing all running containers.
   * A YarnRuntimeException is excepted to be thrown.
   */
  @Test(expected = YarnRuntimeException.class)
  public void testOOMUnresolvedAfterKillingAllContainers() throws Exception {
    int currentContainerId = 0;

    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(currentContainerId++, false, 1, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(currentContainerId++, false, 2, true);
    containers.put(c2.getContainerId(), c2);
    Container c3 = createContainer(currentContainerId++, true, 3, true);
    containers.put(c3.getContainerId(), c3);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 1")
        .thenReturn("under_oom 1");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c2.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1236").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c3.getContainerId().toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));

    DefaultOOMHandler handler =
        new DefaultOOMHandler(context, false) {
          @Override
          protected CGroupsHandler getCGroupsHandler() {
            return cGroupsHandler;
          }
        };
    handler.run();
  }

  /**
   * With cgroup v2 the page cache is not dropped before the node manager is
   * notified, so the measure that decides whether a container is over its
   * request has to be the RSS keys of memory.stat rather than the total
   * usage. Here only the second container is over, and the victim policy
   * orders it before the one that was launched later.
   */
  @Test
  public void testV2ContainerOutOfLimitFromMemoryStat() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, true, 2L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, true, 1L, true);
    containers.put(c2.getContainerId(), c2);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mockV2CGroupsHandler();
    // 4 + 4 + 1 MB against the 10 MB request of createContainer
    File cGroup1 = stubV2Container(cGroupsHandler, c1, memoryStat(4, 4, 1));
    // 6 + 4 + 1 MB against the same request
    File cGroup2 = stubV2Container(cGroupsHandler, c2, memoryStat(6, 4, 1));

    v2Handler(context, cGroupsHandler, 1).run();

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
  public void testV2ContainerOutOfLimitWithoutTheKernelKey() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, true, 2L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, true, 1L, true);
    containers.put(c2.getContainerId(), c2);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mockV2CGroupsHandler();
    File cGroup1 = stubV2Container(cGroupsHandler, c1,
        "anon " + mb(4) + "\nfile_mapped " + mb(4) + "\nslab 4711\n");
    File cGroup2 = stubV2Container(cGroupsHandler, c2,
        "anon " + mb(8) + "\nfile_mapped " + mb(4) + "\nslab 4711\n");

    v2Handler(context, cGroupsHandler, 1).run();

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
  public void testV2KillsThroughCGroupKill() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, true, 1L, true);
    containers.put(c1.getContainerId(), c1);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mockV2CGroupsHandler();
    File cGroup = stubV2Container(cGroupsHandler, c1, memoryStat(1, 0, 0));

    v2Handler(context, cGroupsHandler, 1).run();

    assertEquals("The kill has to go through cgroup.kill", "1",
        FileUtils.readFileToString(new File(cGroup, CGROUP_KILL_FILE),
            StandardCharsets.UTF_8));
    verify(ex, times(0)).signalContainer(any());
  }

  /**
   * cgroup.kill does not exist before Linux 5.14, and the write can fail for
   * other reasons too. The per pid SIGKILL loop stays as the fallback.
   */
  @Test
  public void testV2FallsBackToSignallingEveryPid() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, true, 1L, true);
    containers.put(c1.getContainerId(), c1);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mockV2CGroupsHandler();
    // There is no such directory, so the write to cgroup.kill fails.
    when(cGroupsHandler.getPathForCGroup(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString()))
        .thenReturn(new File(tmp.getRoot(), "gone").getAbsolutePath());
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_PROCS_FILE))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        c1.getContainerId().toString(), CGROUP_MEMORY_STAT))
        .thenReturn(memoryStat(1, 0, 0));

    v2Handler(context, cGroupsHandler, 1).run();

    verify(ex, times(1)).signalContainer(
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
  public void testV2PostKillDelayAndRecheck() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    Container c1 = createContainer(1, false, 1L, true);
    containers.put(c1.getContainerId(), c1);
    Container c2 = createContainer(2, false, 2L, true);
    containers.put(c2.getContainerId(), c2);

    ContainerExecutor ex = createContainerExecutor(containers);
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);
    when(context.getContainerExecutor()).thenReturn(ex);

    CGroupsHandler cGroupsHandler = mockV2CGroupsHandler();
    // The pid loop is used here, because it is what removes the container
    // from the node manager context and so lets a second one be picked.
    for (Container container : new Container[] {c1, c2}) {
      String id = container.getContainerId().toString();
      when(cGroupsHandler.getPathForCGroup(
          CGroupsHandler.CGroupController.MEMORY, id))
          .thenReturn(new File(tmp.getRoot(), "gone-" + id).getAbsolutePath());
      when(cGroupsHandler.getCGroupParam(
          CGroupsHandler.CGroupController.MEMORY, id, CGROUP_PROCS_FILE))
          .thenReturn("123" + id).thenReturn("");
      when(cGroupsHandler.getCGroupParam(
          CGroupsHandler.CGroupController.MEMORY, id, CGROUP_MEMORY_STAT))
          .thenReturn(memoryStat(1, 0, 0));
    }

    // Still out of memory after the first kill, resolved after the second.
    DefaultOOMHandler handler = v2Handler(context, cGroupsHandler, 2);
    long postKillDelayMs = 200;
    handler.setPostKillDelayMs(postKillDelayMs);

    long start = System.currentTimeMillis();
    handler.run();
    long elapsed = System.currentTimeMillis() - start;

    verify(ex, times(2)).signalContainer(any());
    assertTrue("The handler has to pause after each kill, it took only "
        + elapsed + " ms", elapsed >= 2 * postKillDelayMs);
  }

  private CGroupsHandler mockV2CGroupsHandler() {
    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.isCGroupsV2()).thenReturn(true);
    return cGroupsHandler;
  }

  /**
   * Give a container a cgroup directory cgroup.kill can be written to, an
   * empty cgroup.procs so that the kill is seen to complete, and the given
   * memory.stat.
   *
   * @return the cgroup directory of the container
   */
  private File stubV2Container(CGroupsHandler cGroupsHandler,
      Container container, String memoryStat) throws Exception {
    String id = container.getContainerId().toString();
    File cGroup = tmp.newFolder(id);
    when(cGroupsHandler.getPathForCGroup(
        CGroupsHandler.CGroupController.MEMORY, id))
        .thenReturn(cGroup.getAbsolutePath());
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY, id, CGROUP_PROCS_FILE))
        .thenReturn("");
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY, id, CGROUP_MEMORY_STAT))
        .thenReturn(memoryStat);
    return cGroup;
  }

  /**
   * A handler as the cgroup v2 elastic memory controller sets it up: the out
   * of memory condition comes from the controller, and it holds for the given
   * number of checks.
   */
  private DefaultOOMHandler v2Handler(Context context,
      CGroupsHandler cGroupsHandler, int underOOMChecks) {
    DefaultOOMHandler handler = new DefaultOOMHandler(context, false) {
      @Override
      protected CGroupsHandler getCGroupsHandler() {
        return cGroupsHandler;
      }
    };
    AtomicInteger remaining = new AtomicInteger(underOOMChecks);
    handler.setUnderOOMCheck(() -> remaining.getAndDecrement() > 0);
    return handler;
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
  private static ContainerId createContainerId(int id) {
    ApplicationId applicationId = ApplicationId.newInstance(1, 1);

    ApplicationAttemptId applicationAttemptId
        = mock(ApplicationAttemptId.class);
    when(applicationAttemptId.getApplicationId()).thenReturn(applicationId);
    when(applicationAttemptId.getAttemptId()).thenReturn(1);

    ContainerId containerId = mock(ContainerId.class);
    when(containerId.toString()).thenReturn(Integer.toString(id));
    when(containerId.getContainerId()).thenReturn(new Long(1));

    return containerId;
  }

  private static Container createContainer(int containerId,
      boolean guaranteed, long launchTime, boolean running) {
    Container c1 = mock(Container.class);
    ContainerId cid1 = createContainerId(containerId);
    when(c1.getContainerId()).thenReturn(cid1);

    ContainerTokenIdentifier token = mock(ContainerTokenIdentifier.class);
    ExecutionType type =
        guaranteed ? ExecutionType.GUARANTEED : ExecutionType.OPPORTUNISTIC;
    when(token.getExecutionType()).thenReturn(type);
    when(c1.getContainerTokenIdentifier()).thenReturn(token);

    when(c1.getResource()).thenReturn(Resource.newInstance(10, 1));
    when(c1.getContainerLaunchTime()).thenReturn(launchTime);
    when(c1.isRunning()).thenReturn(running);

    return c1;
  }

  String getMB(long mb) {
    return Long.toString(mb * 1024 * 1024);
  }

  private static ContainerExecutor createContainerExecutor(
      ConcurrentHashMap<ContainerId, Container> containers)
      throws IOException {
    ContainerExecutor ex = mock(ContainerExecutor.class);
    when(ex.signalContainer(any())).thenAnswer(
        invocation -> {
          Object[] arguments = invocation.getArguments();
          Container container = ((ContainerSignalContext)
              arguments[0]).getContainer();
          // remove container from NM context immediately
          containers.remove(container.getContainerId());
          return true;
        });
    return ex;
  }
}