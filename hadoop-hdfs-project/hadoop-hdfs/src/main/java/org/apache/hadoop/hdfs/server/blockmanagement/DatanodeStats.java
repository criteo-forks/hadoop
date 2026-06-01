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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Datanode statistics.
 * For decommissioning/decommissioned nodes, only used capacity is counted.
 *
 * <p>The aggregate counters exposed by the public getters are maintained
 * incrementally for O(1) reads but are fully reversible because every
 * contribution is captured as a {@link GlobalContribution} snapshot keyed by
 * the contributing {@link DatanodeDescriptor}. Per-storage-type accounting is
 * delegated to {@link StorageTypeStatsMap} which uses the same pattern. This
 * makes {@link #add} / {@link #subtract} idempotent: repeating an
 * {@code add} simply replaces the node's contribution, and a {@code subtract}
 * on a node that was never added is a no-op. As a result the counters cannot
 * drift even if these methods are called asymmetrically (block reports
 * racing ahead of the first heartbeat after registration, storage state
 * transitions, exceptions escaping between paired calls, etc.).
 */
class DatanodeStats {

  /** Snapshot of what a single {@link DatanodeDescriptor} contributes. */
  private static final class GlobalContribution {
    final boolean inService;
    final long capacityTotal;
    final long capacityUsed;
    final long capacityUsedNonDfs;
    final long capacityRemaining;
    final long blockPoolUsed;
    final long cacheCapacity;
    final long cacheUsed;
    final int xceiverCount;

    GlobalContribution(boolean inService, long capacityTotal, long capacityUsed,
        long capacityUsedNonDfs, long capacityRemaining, long blockPoolUsed,
        long cacheCapacity, long cacheUsed, int xceiverCount) {
      this.inService = inService;
      this.capacityTotal = capacityTotal;
      this.capacityUsed = capacityUsed;
      this.capacityUsedNonDfs = capacityUsedNonDfs;
      this.capacityRemaining = capacityRemaining;
      this.blockPoolUsed = blockPoolUsed;
      this.cacheCapacity = cacheCapacity;
      this.cacheUsed = cacheUsed;
      this.xceiverCount = xceiverCount;
    }
  }

  private final StorageTypeStatsMap statsMap = new StorageTypeStatsMap();

  private long capacityTotal = 0L;
  private long capacityUsed = 0L;
  private long capacityUsedNonDfs = 0L;
  private long capacityRemaining = 0L;
  private long blockPoolUsed = 0L;
  private int xceiverCount = 0;
  private long cacheCapacity = 0L;
  private long cacheUsed = 0L;

  private int nodesInService = 0;
  private int nodesInServiceXceiverCount = 0;
  private int expiredHeartbeats = 0;

  /** Per-DN contribution snapshot for the global aggregates. */
  private final Map<DatanodeDescriptor, GlobalContribution> contributions =
      new HashMap<>();

  synchronized void add(final DatanodeDescriptor node) {
    GlobalContribution prev = contributions.remove(node);
    if (prev != null) {
      revert(prev);
    }
    GlobalContribution now = capture(node);
    apply(now);
    contributions.put(node, now);

    statsMap.refresh(node);
  }

  synchronized void subtract(final DatanodeDescriptor node) {
    GlobalContribution prev = contributions.remove(node);
    if (prev != null) {
      revert(prev);
    }

    statsMap.remove(node);
  }

  private static GlobalContribution capture(final DatanodeDescriptor node) {
    boolean inService = node.isInService();
    boolean decomOrEnteringMaintenance = !inService
        && (node.isDecommissionInProgress() || node.isEnteringMaintenance());

    long cTotal = 0L;
    long cUsed = 0L;
    long cUsedNonDfs = 0L;
    long cRemaining = 0L;
    long bpUsed = 0L;
    long cacheCap = 0L;
    long cacheUsd = 0L;
    if (inService) {
      cTotal = node.getCapacity();
      cUsed = node.getDfsUsed();
      cUsedNonDfs = node.getNonDfsUsed();
      cRemaining = node.getRemaining();
      bpUsed = node.getBlockPoolUsed();
      cacheCap = node.getCacheCapacity();
      cacheUsd = node.getCacheUsed();
    } else if (decomOrEnteringMaintenance) {
      cacheCap = node.getCacheCapacity();
      cacheUsd = node.getCacheUsed();
    }
    int xceiver = node.getXceiverCount();
    return new GlobalContribution(inService, cTotal, cUsed, cUsedNonDfs,
        cRemaining, bpUsed, cacheCap, cacheUsd, xceiver);
  }

  private void apply(GlobalContribution c) {
    xceiverCount += c.xceiverCount;
    capacityTotal += c.capacityTotal;
    capacityUsed += c.capacityUsed;
    capacityUsedNonDfs += c.capacityUsedNonDfs;
    capacityRemaining += c.capacityRemaining;
    blockPoolUsed += c.blockPoolUsed;
    cacheCapacity += c.cacheCapacity;
    cacheUsed += c.cacheUsed;
    if (c.inService) {
      nodesInService++;
      nodesInServiceXceiverCount += c.xceiverCount;
    }
  }

  private void revert(GlobalContribution c) {
    xceiverCount -= c.xceiverCount;
    capacityTotal -= c.capacityTotal;
    capacityUsed -= c.capacityUsed;
    capacityUsedNonDfs -= c.capacityUsedNonDfs;
    capacityRemaining -= c.capacityRemaining;
    blockPoolUsed -= c.blockPoolUsed;
    cacheCapacity -= c.cacheCapacity;
    cacheUsed -= c.cacheUsed;
    if (c.inService) {
      nodesInService--;
      nodesInServiceXceiverCount -= c.xceiverCount;
    }
  }

  /** Increment expired heartbeat counter. */
  void incrExpiredHeartbeats() {
    expiredHeartbeats++;
  }

  synchronized Map<StorageType, StorageTypeStats> getStatsMap() {
    return statsMap.get();
  }

  synchronized long getCapacityTotal() {
    return capacityTotal;
  }

  synchronized long getCapacityUsed() {
    return capacityUsed;
  }

  synchronized long getCapacityRemaining() {
    return capacityRemaining;
  }

  synchronized long getBlockPoolUsed() {
    return blockPoolUsed;
  }

  synchronized int getXceiverCount() {
    return xceiverCount;
  }

  synchronized long getCacheCapacity() {
    return cacheCapacity;
  }

  synchronized long getCacheUsed() {
    return cacheUsed;
  }

  synchronized int getNodesInService() {
    return nodesInService;
  }

  synchronized int getNodesInServiceXceiverCount() {
    return nodesInServiceXceiverCount;
  }

  synchronized int getExpiredHeartbeats() {
    return expiredHeartbeats;
  }

  synchronized float getCapacityRemainingPercent() {
    return DFSUtilClient.getPercentRemaining(capacityRemaining, capacityTotal);
  }

  synchronized float getPercentBlockPoolUsed() {
    return DFSUtilClient.getPercentUsed(blockPoolUsed, capacityTotal);
  }

  synchronized long getCapacityUsedNonDFS() {
    return capacityUsedNonDfs;
  }

  synchronized float getCapacityUsedPercent() {
    return DFSUtilClient.getPercentUsed(capacityUsed, capacityTotal);
  }

  /**
   * Per-storage-type accounting. Delegates to a {@link StorageTypeStats}
   * instance per storage type, which itself uses contribution snapshots so
   * that {@link #refresh} / {@link #remove} are idempotent and self-healing.
   */
  static final class StorageTypeStatsMap {

    private final Map<StorageType, StorageTypeStats> storageTypeStatsMap =
        new EnumMap<>(StorageType.class);

    private Map<StorageType, StorageTypeStats> get() {
      return new EnumMap<>(storageTypeStatsMap);
    }

    /**
     * Refresh {@code node}'s contribution across all storage types it
     * currently has (non-FAILED), and drop its contribution from any type
     * it no longer has. Safe to call repeatedly.
     */
    private void refresh(final DatanodeDescriptor node) {
      Map<StorageType, List<DatanodeStorageInfo>> byType =
          new EnumMap<>(StorageType.class);
      for (DatanodeStorageInfo info : node.getStorageInfos()) {
        if (info.getState() != DatanodeStorage.State.FAILED) {
          byType.computeIfAbsent(info.getStorageType(),
              k -> new ArrayList<>()).add(info);
        }
      }

      for (Map.Entry<StorageType, List<DatanodeStorageInfo>> e
          : byType.entrySet()) {
        StorageTypeStats stats = storageTypeStatsMap.get(e.getKey());
        if (stats == null) {
          stats = new StorageTypeStats(e.getKey());
          storageTypeStatsMap.put(e.getKey(), stats);
        }
        stats.put(node, e.getValue());
      }

      Iterator<Map.Entry<StorageType, StorageTypeStats>> it =
          storageTypeStatsMap.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<StorageType, StorageTypeStats> entry = it.next();
        if (!byType.containsKey(entry.getKey())) {
          entry.getValue().remove(node);
        }
        if (entry.getValue().isEmpty()) {
          it.remove();
        }
      }
    }

    /**
     * Drop {@code node}'s contribution from every storage type. Safe to call
     * repeatedly; types the node never contributed to are unaffected.
     */
    private void remove(final DatanodeDescriptor node) {
      Iterator<Map.Entry<StorageType, StorageTypeStats>> it =
          storageTypeStatsMap.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<StorageType, StorageTypeStats> entry = it.next();
        entry.getValue().remove(node);
        if (entry.getValue().isEmpty()) {
          it.remove();
        }
      }
    }
  }
}
