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

package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.Time;

/**
 * The class is responsible for choosing the desired number of targets
 * for placing block replicas on environment with node-group layer.
 * The replica placement strategy is adjusted to:
 * If the writer is on a datanode, the 1st replica is placed on the local
 * node(or local node-group or on local rack), otherwise a random datanode.
 * The 2nd replica is placed on a datanode that is on a different rack with 1st
 * replica node.
 * The 3rd replica is placed on a datanode which is on a different node-group
 * but the same rack as the second replica node.
 */
public class BlockPlacementPolicyWithPlacementGroup extends BlockPlacementPolicyWithNodeGroup {

  protected BlockPlacementPolicyWithPlacementGroup() {
  }

  private volatile long lastRefreshTimestamp = 0;
  private long refreshIntervalMs = 5 * 1000;
  private TreeMap<String, Float> scopeUsageRatio;
  private ReentrantReadWriteLock scopeUsageRatioLock = new ReentrantReadWriteLock(true);

  private void computeScopeUsageRatio() {
    scopeUsageRatioLock.writeLock().lock();
    try{
      if (lastRefreshTimestamp + refreshIntervalMs > Time.now()) {
        return;
      }
      HashMap<String, Long> scopeCapacity = new HashMap<>();
      HashMap<String, Long> scopeRemaining = new HashMap<>();
      for (Node node : clusterMap.getLeaves(NodeBase.ROOT)) {
        DatanodeDescriptor dn = (DatanodeDescriptor) node;
        String placementGroup = NetworkTopology.getFirstHalf(dn.getNetworkLocation());
        scopeCapacity.merge(placementGroup, dn.getCapacity(), Long::sum);
        scopeRemaining.merge(placementGroup, dn.getRemaining(), Long::sum);
        scopeUsageRatio = new TreeMap<>();
      }
      scopeCapacity.forEach(
          //if remaining value missing, set usage to 100%
          (scope, capacity) ->
              scopeUsageRatio.put(scope, 1 - scopeRemaining.getOrDefault(scope, capacity) / (float) capacity));

      lastRefreshTimestamp = Time.now();
    } finally {
      scopeUsageRatioLock.writeLock().unlock();
    }
  }

  private String getLeastUsedScope() {
    if (lastRefreshTimestamp + refreshIntervalMs < Time.now()) {
      computeScopeUsageRatio();
    }
    scopeUsageRatioLock.readLock().lock();
    try {
      return scopeUsageRatio.firstKey();
    } finally {
      scopeUsageRatioLock.readLock().unlock();
    }
  }

  @Override
  protected Node chooseTargetInOrder(int numOfReplicas,
                                     Node writer,
                                     final Set<Node> excludedNodes,
                                     final long blocksize,
                                     final int maxNodesPerRack,
                                     final List<DatanodeStorageInfo> results,
                                     final boolean avoidStaleNodes,
                                     final boolean newBlock,
                                     EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    final int numOfResults = results.size();
    if (numOfResults == 0) {
      DatanodeStorageInfo storageInfo = chooseLocalStorage(writer,
          excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes,
          storageTypes, true);

      writer = (storageInfo != null) ? storageInfo.getDatanodeDescriptor() : null;

      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    final DatanodeDescriptor dn0 = results.get(0).getDatanodeDescriptor();
    final String placementGroup0 = NetworkTopology.getLastHalf(dn0.getNetworkLocation());
    if (numOfResults <= 1) {
      chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes);
      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    if (numOfResults <= 2) {
      final DatanodeDescriptor dn1 = results.get(1).getDatanodeDescriptor();
      if (clusterMap.isOnSameRack(dn0, dn1)) {
        chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      } else if (newBlock) {
        final DatanodeDescriptor localDn = getLeastUsedScope().equals(placementGroup0) ? dn0 : dn1;
        chooseLocalRack(localDn, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      } else {
        chooseLocalRack(writer, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      }
      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    return writer;
  }
}
