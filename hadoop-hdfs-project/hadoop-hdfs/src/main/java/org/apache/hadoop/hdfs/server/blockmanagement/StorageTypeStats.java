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

import java.beans.ConstructorProperties;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;

/**
 * Statistics per StorageType.
 *
 * The aggregate counters exposed by the public getters are maintained
 * incrementally for O(1) reads but are fully reversible because every
 * contribution is captured as a {@link Contribution} snapshot, keyed by the
 * contributing {@link DatanodeDescriptor}. This makes all mutations
 * idempotent: re-applying a node simply replaces its contribution, and
 * removing a node we never added is a no-op. As a result the counters cannot
 * drift even if {@link #put} / {@link #remove} are called asymmetrically
 * (e.g. block reports racing ahead of the first heartbeat after registration,
 * storage state transitions, or exceptions escaping between paired calls).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class StorageTypeStats {

  /** Snapshot of what a single {@link DatanodeDescriptor} contributes. */
  private static final class Contribution {
    final boolean inService;
    final long capacityTotal;
    final long capacityUsed;
    final long capacityNonDfsUsed;
    final long capacityRemaining;
    final long blockPoolUsed;
    final int xceiverCount;

    Contribution(boolean inService, long capacityTotal, long capacityUsed,
        long capacityNonDfsUsed, long capacityRemaining, long blockPoolUsed,
        int xceiverCount) {
      this.inService = inService;
      this.capacityTotal = capacityTotal;
      this.capacityUsed = capacityUsed;
      this.capacityNonDfsUsed = capacityNonDfsUsed;
      this.capacityRemaining = capacityRemaining;
      this.blockPoolUsed = blockPoolUsed;
      this.xceiverCount = xceiverCount;
    }
  }

  private long capacityTotal = 0L;
  private long capacityUsed = 0L;
  private long capacityNonDfsUsed = 0L;
  private long capacityRemaining = 0L;
  private long blockPoolUsed = 0L;
  private int nodesInService = 0;
  private int totalNodes = 0;
  private int nodesInServiceXceiverCount = 0;

  private StorageType storageType;

  /**
   * Per-DN contribution snapshot. Reference-identity keyed: each DN
   * {@link DatanodeDescriptor} instance contributes at most once.
   */
  private final Map<DatanodeDescriptor, Contribution> contributions =
      new HashMap<>();

  @VisibleForTesting
  void setDataNodesInServiceXceiverCount(int avgXceiverPerDatanode,
      int numNodesInService) {
    this.nodesInService = numNodesInService;
    this.nodesInServiceXceiverCount = numNodesInService * avgXceiverPerDatanode;
  }

  @ConstructorProperties({"capacityTotal", "capacityUsed", "capacityNonDfsUsed",
      "capacityRemaining", "blockPoolUsed", "nodesInService"})
  public StorageTypeStats(
      long capacityTotal, long capacityUsed, long capacityNonDfsUsedUsed,
      long capacityRemaining, long blockPoolUsed, int nodesInService) {
    this.capacityTotal = capacityTotal;
    this.capacityUsed = capacityUsed;
    this.capacityNonDfsUsed = capacityNonDfsUsedUsed;
    this.capacityRemaining = capacityRemaining;
    this.blockPoolUsed = blockPoolUsed;
    this.nodesInService = nodesInService;
  }

  public long getCapacityTotal() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return capacityTotal/nodesInService;
    }
    return capacityTotal;
  }

  public long getCapacityUsed() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return capacityUsed/nodesInService;
    }
    return capacityUsed;
  }

  public long getCapacityNonDfsUsed() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return capacityNonDfsUsed/nodesInService;
    }
    return capacityNonDfsUsed;
  }

  public long getCapacityRemaining() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return capacityRemaining/nodesInService;
    }
    return capacityRemaining;
  }

  public long getBlockPoolUsed() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return blockPoolUsed/nodesInService;
    }
    return blockPoolUsed;
  }

  public int getNodesInService() {
    return nodesInService;
  }

  public int getTotalNodes() {
    return totalNodes;
  }

  public int getNodesInServiceXceiverCount() {
    return nodesInServiceXceiverCount;
  }

  StorageTypeStats(StorageType storageType) {
    this.storageType = storageType;
  }

  StorageTypeStats(StorageTypeStats other) {
    capacityTotal = other.capacityTotal;
    capacityUsed = other.capacityUsed;
    capacityNonDfsUsed = other.capacityNonDfsUsed;
    capacityRemaining = other.capacityRemaining;
    blockPoolUsed = other.blockPoolUsed;
    nodesInService = other.nodesInService;
    totalNodes = other.totalNodes;
    nodesInServiceXceiverCount = other.nodesInServiceXceiverCount;
  }

  /**
   * Add or refresh {@code node}'s contribution to this storage type stats,
   * computed from the supplied (non-FAILED) storages belonging to this type.
   *
   * Idempotent: if {@code node} already had a contribution it is reverted
   * before the new one is applied, so calling {@code put} repeatedly with
   * the same arguments yields the same state.
   */
  void put(DatanodeDescriptor node,
      Iterable<DatanodeStorageInfo> storagesOfThisType) {
    Contribution prev = contributions.remove(node);
    if (prev != null) {
      revert(prev);
    }
    Contribution now = capture(node, storagesOfThisType);
    apply(now);
    contributions.put(node, now);
  }

  /**
   * Remove {@code node}'s contribution, if any.
   * Idempotent: no-op if {@code node} was never tracked.
   */
  void remove(DatanodeDescriptor node) {
    Contribution prev = contributions.remove(node);
    if (prev != null) {
      revert(prev);
    }
  }

  boolean isEmpty() {
    return contributions.isEmpty();
  }

  private Contribution capture(DatanodeDescriptor node,
      Iterable<DatanodeStorageInfo> storagesOfThisType) {
    boolean inService = node.isInService();
    long cTotal = 0L;
    long cUsed = 0L;
    long cNonDfs = 0L;
    long cRemaining = 0L;
    long bpUsed = 0L;
    for (DatanodeStorageInfo info : storagesOfThisType) {
      assert storageType == info.getStorageType();
      cUsed += info.getDfsUsed();
      cNonDfs += info.getNonDfsUsed();
      bpUsed += info.getBlockPoolUsed();
      if (inService) {
        cTotal += info.getCapacity();
        cRemaining += info.getRemaining();
      } else {
        cTotal += info.getDfsUsed();
      }
    }
    int xceiver = inService ? node.getXceiverCount() : 0;
    return new Contribution(inService, cTotal, cUsed, cNonDfs, cRemaining,
        bpUsed, xceiver);
  }

  private void apply(Contribution c) {
    capacityTotal += c.capacityTotal;
    capacityUsed += c.capacityUsed;
    capacityNonDfsUsed += c.capacityNonDfsUsed;
    capacityRemaining += c.capacityRemaining;
    blockPoolUsed += c.blockPoolUsed;
    totalNodes++;
    if (c.inService) {
      nodesInService++;
      nodesInServiceXceiverCount += c.xceiverCount;
    }
  }

  private void revert(Contribution c) {
    capacityTotal -= c.capacityTotal;
    capacityUsed -= c.capacityUsed;
    capacityNonDfsUsed -= c.capacityNonDfsUsed;
    capacityRemaining -= c.capacityRemaining;
    blockPoolUsed -= c.blockPoolUsed;
    totalNodes--;
    if (c.inService) {
      nodesInService--;
      nodesInServiceXceiverCount -= c.xceiverCount;
    }
  }
}
