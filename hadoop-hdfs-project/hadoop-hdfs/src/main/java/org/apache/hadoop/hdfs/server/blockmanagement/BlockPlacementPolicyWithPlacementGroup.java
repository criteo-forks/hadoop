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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.Time;

/**
 * The class is designed to work with Criteo way of migrating hadoop cluster seamlessly
 * At this moment data is written in 2 different datacenters. While BlockPlacementPolicyWithNodeGroup
 * are sufficient for most case, this class adds a refinement that consist in placing a third replica in the
 * least used datacenter, allowing correct balance of the 2 datacenters.
 */
public class BlockPlacementPolicyWithPlacementGroup extends BlockPlacementPolicyWithNodeGroup {
  
  private long scopeUsageRatioRefreshIntervalMs;
  private volatile long scopeUsageRatioLastRefreshTimestamp = 0;
  private volatile String leastUsedScope;
  
  protected BlockPlacementPolicyWithPlacementGroup() {
  }
  
  @Override
  public void initialize(Configuration conf, FSClusterStats stats, NetworkTopology clusterMap,
                         Host2NodesMap host2datanodeMap) {
    this.scopeUsageRatioRefreshIntervalMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_WITH_PLACEMENT_GROUP_REFRESH_SCOPE_USAGE_INTERVAL,
        DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_WITH_PLACEMENT_GROUP_REFRESH_SCOPE_USAGE_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS
    );
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
  }
  
  private String getLeastUsedScope() {
    if (scopeUsageRatioLastRefreshTimestamp + scopeUsageRatioRefreshIntervalMs < Time.now()) {
      synchronized (this) {
        // check again to make sure only one thread will update when needed
        // in case many thread waiting to acquire the lock
        if (scopeUsageRatioLastRefreshTimestamp + scopeUsageRatioRefreshIntervalMs < Time.now()) {
          computeLeastUsedScope();
        }
      }
    }
    return leastUsedScope;
  }
  
  private void computeLeastUsedScope() {
    HashMap<String, Long> scopeCapacity = new HashMap<>();
    HashMap<String, Long> scopeRemaining = new HashMap<>();
    for (Node node : clusterMap.getLeaves(NodeBase.ROOT)) {
      DatanodeDescriptor dn = (DatanodeDescriptor) node;
      String placementGroup = NetworkTopology.getFirstHalf(dn.getNetworkLocation());
      scopeCapacity.merge(placementGroup, dn.getCapacity(), Long::sum);
      scopeRemaining.merge(placementGroup, dn.getRemaining(), Long::sum);
    }
    HashMap<String, Float> scopeUsageRatio = new HashMap<>();
    scopeCapacity.forEach(
        //if remaining value missing, set usage to 100%
        (scope, capacity) -> scopeUsageRatio.put(
            scope,
            1 - scopeRemaining.getOrDefault(scope, capacity) / (float) capacity
        )
    );
    
    leastUsedScope = scopeUsageRatio
        .entrySet()
        .stream()
        .min(Entry.comparingByValue())
        .orElseThrow(() -> new RuntimeException("Cannot find any scope. This is unexpected"))
        .getKey();
  
    scopeUsageRatioLastRefreshTimestamp = Time.now();
  }
  
  //This is the same method as in BlockPlacementPolicyWithNodeGroup
  //The difference is only for a new block, placing the third replica on the least used scope
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
        String placementGroup0 = NetworkTopology.getFirstHalf(dn0.getNetworkLocation());
        DatanodeDescriptor localDn = getLeastUsedScope().equals(placementGroup0) ? dn0 : dn1;
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
