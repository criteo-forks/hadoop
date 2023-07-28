package org.apache.hadoop.net;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

/*
 * This class is intended to be used exclusively with BlockPlacementPolicyRackFaultTolerantWithExcludedScope
 */
public class NetworkTopologyWithExcludedScope extends NetworkTopology implements Configurable {
  
  private Configuration conf;
  private String excludedScope;
  private final Set<Node> nodesFromExcludedScope = new HashSet<>();
  private final Map<String, Integer> excludeNodesCountPerRack = new HashMap<>();
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    //It is ok for excludedScope to be null as NetworkTopology handles null values for this parameter
    this.excludedScope = conf.get(CommonConfigurationKeysPublic.NET_TOPOLOGY_IMPL_KEY
        + "." + NetworkTopologyWithExcludedScope.class.getSimpleName() + ".excluded-scope");
  }
  
  @Override
  public Configuration getConf() {
    return this.conf;
  }
  
  // Note this method is called within the context of the lock.
  // The lock is already obtained by the public classes NetworkTopology.chooseRandom
  @Override
  protected Node chooseRandom(String scope, String excludedScope, Collection<Node> excludedNodes) {
    // if `excludedScope` is not provided by the caller set it
    if (excludedScope == null) {
      excludedScope = this.excludedScope;
    } else {
      // If `excludedScope` is provided by the caller, append all nodes from the excludedScope to the collection.
      // Note: This can slow down the search.
      //       However, instances where `excludedScope` is provided are
      //       not used by BlockPlacementPolicyRackFaultTolerantWithExcludedScope for which this class is designed.
      //       In case some other usage have not been foreseen, we still implement a functionally valid variant.
      Set<Node> newExcludedNodes = new HashSet<>(excludedNodes);
      newExcludedNodes.addAll(nodesFromExcludedScope);
      excludedNodes = newExcludedNodes;
    }
    
    return super.chooseRandom(scope, excludedScope, excludedNodes);
  }
  
  @Override
  public void add(Node node) {
    netlock.writeLock().lock(); // ok because the lock is reentrant
    try {
      super.add(node);
      if (!(node instanceof InnerNode) && isFromExcludedScope(node)) {
        this.nodesFromExcludedScope.add(node);
        this.excludeNodesCountPerRack.merge(node.getNetworkLocation(), 1, Integer::sum);
      }
    } finally {
      netlock.writeLock().unlock();
    }
  }
  
  @Override
  public void remove(Node node) {
    netlock.writeLock().lock(); // ok because the lock is reentrant
    try {
      super.remove(node);
      if (!(node instanceof InnerNode) && isFromExcludedScope(node)) {
        this.nodesFromExcludedScope.remove(node);
        this.excludeNodesCountPerRack.merge(node.getNetworkLocation(), 0, (total,i) -> total - i);
        if (this.excludeNodesCountPerRack.get(node.getNetworkLocation()) <= 0) {
          this.excludeNodesCountPerRack.remove(node.getNetworkLocation());
        }
      }
    } finally {
      netlock.writeLock().unlock();
    }
  }
  
  public boolean isExcluded(Node node) {
    netlock.readLock().lock();
    try {
      return this.nodesFromExcludedScope.contains(node);
    } finally {
      netlock.readLock().unlock();
    }
  }
  
  public boolean isRackExcluded(String rack) {
    netlock.readLock().lock();
    try {
      return this.excludeNodesCountPerRack.containsKey(rack);
    } finally {
      netlock.readLock().unlock();
    }
  }
  
  public int getNumExcludedRacks() {
    netlock.readLock().lock();
    try {
      return this.excludeNodesCountPerRack.size();
    } finally {
      netlock.readLock().unlock();
    }
  }
  
  private boolean isFromExcludedScope(Node node) {
    Node excludedScopeNode = getNode(excludedScope);
    if (excludedScopeNode == null)
      return false;
  
    while ((node = node.getParent()) != null) {
      if (node.equals(excludedScopeNode)) {
        return true;
      }
    }
    
    return false;
  }
  
}
