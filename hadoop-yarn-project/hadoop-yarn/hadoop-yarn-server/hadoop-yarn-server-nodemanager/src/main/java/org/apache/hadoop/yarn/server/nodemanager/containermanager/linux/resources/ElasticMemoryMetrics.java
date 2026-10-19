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

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsV2OOMHandler.Verdict;

/**
 * Metrics exported by the cgroup v2 elastic memory controller.
 *
 * The verdict is published as three 0/1 gauges rather than an enum ordinal,
 * so a consumer never has to know the order of {@link Verdict}. Exactly one
 * of them is 1 in every snapshot: {@link #setVerdict} and
 * {@link #getMetrics} share this object's monitor.
 */
@Metrics(about = "Elastic memory control", context = "yarn")
public class ElasticMemoryMetrics implements MetricsSource {
  static final String NAME = "ElasticMemoryControl";

  private final MetricsRegistry registry = new MetricsRegistry(NAME);

  // CHECKSTYLE:OFF:VisibilityModifier
  @Metric("cgroup hierarchy version the controller runs on")
  MutableGaugeInt cgroupVersion;
  @Metric("memory.max set on the YARN root cgroup")
  MutableGaugeLong memoryLimitBytes;
  @Metric("memory.high set on the YARN root cgroup")
  MutableGaugeLong memoryHighBytes;
  @Metric("memory.current of the YARN root cgroup")
  MutableGaugeLong memoryCurrentBytes;
  @Metric("footprint the kill decision is taken on")
  MutableGaugeLong memoryFootprintBytes;
  @Metric("1 while the footprint is not over memory.high")
  MutableGaugeInt verdictClear;
  @Metric("1 while over memory.high but the hold or pressure gate is not met")
  MutableGaugeInt verdictPending;
  @Metric("1 while the controller may kill a container")
  MutableGaugeInt verdictKill;
  @Metric("ms the footprint has been over memory.high, 0 if not")
  MutableGaugeLong breachDurationMs;
  @Metric("containers killed by the elastic memory controller")
  MutableCounterLong containersKilled;
  @Metric("kills by the kernel OOM killer inside the YARN cgroup")
  MutableCounterLong kernelOomKills;
  // CHECKSTYLE:ON:VisibilityModifier

  private static ElasticMemoryMetrics instance;

  /** Return the single metrics source registered for this NodeManager. */
  static synchronized ElasticMemoryMetrics create() {
    if (instance == null) {
      instance = DefaultMetricsSystem.instance().register(
          NAME, "Elastic memory control", new ElasticMemoryMetrics());
      // A live NodeManager always publishes one verdict, never none.
      instance.setVerdict(Verdict.CLEAR);
    }
    return instance;
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector,
      boolean all) {
    registry.snapshot(collector.addRecord(registry.info()), all);
  }

  synchronized void setVerdict(Verdict verdict) {
    verdictClear.set(verdict == Verdict.CLEAR ? 1 : 0);
    verdictPending.set(verdict == Verdict.PENDING ? 1 : 0);
    verdictKill.set(verdict == Verdict.KILL ? 1 : 0);
  }

  void clearVerdict() {
    setVerdict(Verdict.CLEAR);
    breachDurationMs.set(0);
  }
}
