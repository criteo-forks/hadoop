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

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsV2OOMHandler.Verdict;
import org.junit.Test;

import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

/** Tests the dedicated elastic memory metrics source. */
public class TestElasticMemoryMetrics {

  @Test
  public void testSourceNameAndGauges() {
    DefaultMetricsSystem.setMiniClusterMode(true);
    ElasticMemoryMetrics metrics = ElasticMemoryMetrics.create();
    metrics.cgroupVersion.set(2);
    metrics.memoryHighBytes.set(4711);

    MetricsRecordBuilder record = getMetrics(ElasticMemoryMetrics.NAME);
    assertGauge("CgroupVersion", 2, record);
    assertGauge("MemoryHighBytes", 4711L, record);
  }

  @Test
  public void testVerdictIsPublishedOneHot() {
    DefaultMetricsSystem.setMiniClusterMode(true);
    ElasticMemoryMetrics metrics = ElasticMemoryMetrics.create();

    metrics.setVerdict(Verdict.PENDING);
    MetricsRecordBuilder record = getMetrics(ElasticMemoryMetrics.NAME);
    assertGauge("VerdictClear", 0, record);
    assertGauge("VerdictPending", 1, record);
    assertGauge("VerdictKill", 0, record);

    metrics.setVerdict(Verdict.KILL);
    record = getMetrics(ElasticMemoryMetrics.NAME);
    assertGauge("VerdictClear", 0, record);
    assertGauge("VerdictPending", 0, record);
    assertGauge("VerdictKill", 1, record);

    metrics.clearVerdict();
    record = getMetrics(ElasticMemoryMetrics.NAME);
    assertGauge("VerdictClear", 1, record);
    assertGauge("VerdictPending", 0, record);
    assertGauge("VerdictKill", 0, record);
    assertGauge("BreachDurationMs", 0L, record);
  }
}
