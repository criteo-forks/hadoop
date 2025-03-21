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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NetworkTopologyWithExcludedScope;
import org.apache.hadoop.service.ServiceStateException;

/**
 * The class is responsible for choosing the desired number of targets
 * for placing block replicas.
 * The strategy is that it tries its best to place the replicas to most racks.
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyRackFaultTolerantWithExcludedScope extends BlockPlacementPolicyRackFaultTolerant {
  
  @Override
  public void initialize(Configuration conf, FSClusterStats stats, NetworkTopology clusterMap,
                         Host2NodesMap host2datanodeMap) {
    if (!(clusterMap instanceof NetworkTopologyWithExcludedScope)) {
      throw new ServiceStateException(
          "BlockPlacementPolicyRackFaultTolerantWithExcludedRacks requires the use of NetworkTopologyWithExcludedScope"
      );
    }
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
  }
  
  // Same as in BlockPlacementPolicyRackFaultTolerant but checks if the node is from the excluded scope
  // It was not possible to factorise with BlockPlacementPolicyRackFaultTolerant
  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas) {
    if (locs == null)
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    if (!clusterMap.hasClusterEverBeenMultiRack()) {
      // only one rack
      return new BlockPlacementStatusDefault(1, 1, 1);
    }
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    // Make sure excluded racks are not accounted
    Set<String> racks = new TreeSet<>();
    for (DatanodeInfo dn : locs) {
      if(!((NetworkTopologyWithExcludedScope) clusterMap).isExcluded(dn)) {
        racks.add(dn.getNetworkLocation());
      }
    }
    return new BlockPlacementStatusDefault(racks.size(), numberOfReplicas,
        clusterMap.getNumOfRacks() - ((NetworkTopologyWithExcludedScope) clusterMap).getNumExcludedRacks()
    );
  }

  @Override
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    
    // if there are excluded racks, chose those otherwise use RackFaultTolerant semantics
    Set<DatanodeStorageInfo> excludedDatanodes = new HashSet<>();
    for(String rack : rackMap.keySet()) {
      if (((NetworkTopologyWithExcludedScope) clusterMap).isRackExcluded(rack)) {
        excludedDatanodes.addAll(rackMap.get(rack));
      }
    }
    
    if (excludedDatanodes.isEmpty()) {
      return super.pickupReplicaSet(moreThanOne, exactlyOne, rackMap);
    } else {
      return excludedDatanodes;
    }
  }
}
