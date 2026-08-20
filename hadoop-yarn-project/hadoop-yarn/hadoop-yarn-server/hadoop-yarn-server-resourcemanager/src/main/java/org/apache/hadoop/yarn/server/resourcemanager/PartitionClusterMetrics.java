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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.apache.hadoop.metrics2.lib.Interns.info;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Exposes per-partition (node label) cluster metrics via JMX.
 *
 * One instance is created for each partition. JMX beans are registered as
 * {@code Hadoop:service=ResourceManager,name=PartitionClusterMetrics,partition=<label>}.
 */
@InterfaceAudience.Private
public class PartitionClusterMetrics implements MetricsSource {

  private static final MetricsInfo RECORD_INFO =
      info("PartitionClusterMetrics",
          "Cluster metrics per partition (node label)");

  private static final ConcurrentMap<String, PartitionClusterMetrics>
      INSTANCES = new ConcurrentHashMap<>();

  private final MetricsRegistry registry;
  private final MutableGaugeInt numActiveNMs;
  private final MutableGaugeInt numDecommissioningNMs;
  private final MutableGaugeInt numDecommissionedNMs;
  private final MutableGaugeInt numUnhealthyNMs;
  private final MutableGaugeInt numLostNMs;
  private final MutableGaugeInt numRebootedNMs;
  private final MutableGaugeInt numShutdownNMs;
  private final MutableGaugeLong capabilityMB;
  private final MutableGaugeLong capabilityVirtualCores;
  private final String partition;

  private PartitionClusterMetrics(String partition) {
    this.partition = partition;
    this.registry = new MetricsRegistry(RECORD_INFO);
    this.registry.tag(info("Partition", "Partition name"),
        partition.isEmpty() ? "" : partition);
    this.numActiveNMs = registry.newGauge(
        info("NumActiveNMs", "Number of active NMs in partition"), 0);
    this.numDecommissioningNMs = registry.newGauge(
        info("NumDecommissioningNMs",
            "Number of decommissioning NMs in partition"), 0);
    this.numDecommissionedNMs = registry.newGauge(
        info("NumDecommissionedNMs",
            "Number of decommissioned NMs in partition"), 0);
    this.numUnhealthyNMs = registry.newGauge(
        info("NumUnhealthyNMs",
            "Number of unhealthy NMs in partition"), 0);
    this.numLostNMs = registry.newGauge(
        info("NumLostNMs", "Number of lost NMs in partition"), 0);
    this.numRebootedNMs = registry.newGauge(
        info("NumRebootedNMs",
            "Number of rebooted NMs in partition"), 0);
    this.numShutdownNMs = registry.newGauge(
        info("NumShutdownNMs",
            "Number of shutdown NMs in partition"), 0);
    this.capabilityMB = registry.newGauge(
        info("CapabilityMB", "Total memory capability (MB) in partition"), 0L);
    this.capabilityVirtualCores = registry.newGauge(
        info("CapabilityVirtualCores",
            "Total vcore capability in partition"), 0L);
  }

  /**
   * Get (or create and register) the metrics instance for a partition.
   * Follows the same convention as {@code PartitionQueueMetrics}: the default
   * (NO_LABEL) partition uses an empty string in JMX.
   */
  public static PartitionClusterMetrics getMetrics(String partition) {
    return INSTANCES.computeIfAbsent(partition, p -> {
      PartitionClusterMetrics m = new PartitionClusterMetrics(p);
      MetricsSystem ms = DefaultMetricsSystem.instance();
      if (ms != null) {
        String jmxPartition = p.isEmpty() ? "" : p;
        String name = "PartitionClusterMetrics,partition=" + jmxPartition;
        ms.register(name,
            "Cluster metrics for partition: " + jmxPartition, m);
      }
      return m;
    });
  }

  public void setNumActiveNMs(int count) {
    numActiveNMs.set(count);
  }

  public int getNumActiveNMs() {
    return numActiveNMs.value();
  }

  public void incrNumActiveNMs() {
    numActiveNMs.incr();
  }

  public void decrNumActiveNMs() {
    numActiveNMs.decr();
  }

  public void incrDecommissioningNMs() {
    numDecommissioningNMs.incr();
  }

  public void decrDecommissioningNMs() {
    numDecommissioningNMs.decr();
  }

  public void incrDecommissionedNMs() {
    numDecommissionedNMs.incr();
  }

  public void decrDecommissionedNMs() {
    numDecommissionedNMs.decr();
  }

  public void incrUnhealthyNMs() {
    numUnhealthyNMs.incr();
  }

  public void decrUnhealthyNMs() {
    numUnhealthyNMs.decr();
  }

  public void incrLostNMs() {
    numLostNMs.incr();
  }

  public void decrLostNMs() {
    numLostNMs.decr();
  }

  public void incrRebootedNMs() {
    numRebootedNMs.incr();
  }

  public void decrRebootedNMs() {
    numRebootedNMs.decr();
  }

  public void incrShutdownNMs() {
    numShutdownNMs.incr();
  }

  public void decrShutdownNMs() {
    numShutdownNMs.decr();
  }

  public void setCapability(long memoryMB, long vCores) {
    capabilityMB.set(memoryMB);
    capabilityVirtualCores.set(vCores);
  }

  public long getCapabilityMB() {
    return capabilityMB.value();
  }

  public long getCapabilityVirtualCores() {
    return capabilityVirtualCores.value();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(RECORD_INFO), all);
  }

  @VisibleForTesting
  public static void destroy() {
    INSTANCES.clear();
  }

  @VisibleForTesting
  static ConcurrentMap<String, PartitionClusterMetrics> getInstances() {
    return INSTANCES;
  }
}
