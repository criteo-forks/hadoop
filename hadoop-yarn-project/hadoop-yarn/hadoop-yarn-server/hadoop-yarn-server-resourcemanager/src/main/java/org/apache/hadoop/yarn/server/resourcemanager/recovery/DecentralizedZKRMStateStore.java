package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For all details https://criteo.atlassian.net/wiki/spaces/HAD/pages/2034204866/Managing+latency+with+the+resourcemanager+statestore
 *
 * State store is local for each RM instance. All state stores must be described in the configuration:
 * yarn.resourcemanager.distributed-zk-state-store.key.address|acl|num-retries|timeout-ms|retry-interval-ms
 *
 * Per resourcemanager instance, two configurations must also be set:
 * yarn.resourcemanager.distributed-zk-state-store.local.key with the value pointing to the local state store for this resourcemanager
 * yarn.resourcemanager.distributed-zk-state-store.remote.keys a comma-separated list of keys matching the other state stores
 *
 * This class will reuse ZKRMStateStore to store in the local state store.
 * The default ACL fencing mechanism is still in place, to avoid changing too much code, however it is pointless in the context of DecentralizedZKRMStateStore
 * Instead, an epoch based fencing is put in place on top of it.
 *
 * A connection to a global zookeeper is kept in place for leader election, epoch generation and source-of-truth storage.
 * It is also used to perform regular checks to verify current state of the resourcemanager, with the use of a dedicated fencing node.
 *
 * Upon leadership election, DecentralizedZKRMStateStore will do the following steps:
 *   * Get a new epoch from the global zookeeper
 *   * Try to fence the local store with the new epoch
 *   * Try to fence the remote stores asynchronously and non-blocking with the new epoch
 *   * Load data from the source-of-truth (or the freshest dump available across all available stores)
 *   * Dump data to the local state store
 *   * Set the new source-of-truth in the global zookeeper
 *
 * The new local state store will have the same hierarchy as described in ZKRMStateStore, however the ROOT_DIR_PATH is named StateStore_epoch_timestamp.
 * The resourcemanager is responsible for removing old epoch state stores.
 *
 * ZKRMRemoteStateStore classes are introduced to perform regular dumps of the state store.
 * Dumps have the exact same hierarchy as described in ZKRMStateStore, however the ROOT_DIR_PATH is named StateStore_dump_timestamp
 * so that freshness of the dump can be easily checked when reading data from a dump instead of the current source of truth.
 * The asynchronous dump to remote is responsible for removing old dumps.
 *
 * To sump up, here is the hierarchy of the global zookeeper:
 * DECENTRALIZED_ROOT_DIR_PATH
 * |--- EPOCH_NODE
 * |--- SOURCE_OF_TRUTH
 * |--- FENCING_NODE
 *
 * and the hierarchy of a local state store:
 * ROOT_DIR_PATH
 * |--- EPOCH_NODE
 * |--- StateStore_epoch_val1
 * |     |see ZKRMStateStore
 * |--- StateStore_epoch_val2
 * |     |see ZKRMStateStore
 * |--- StateStore_dump_timestamp3
 * |     |see ZKRMStateStore
 * |--- StateStore_dump_timestamp4
 * |     |see ZKRMStateStore
 */
public class DecentralizedZKRMStateStore extends RMStateStore {
  
  private static final Logger LOG = LoggerFactory.getLogger(DecentralizedZKRMStateStore.class);
  
  private static final String GLOBAL_EPOCH_ZNODE_NAME = "LeadershipElectionEpoch";
  private static final String GLOBAL_FENCING_ZNODE_NAME = "FencingNode";
  private static final String SOURCE_OF_TRUTH_ZNODE_NAME = "SourceOfTruth";
  private static final String LOCAL_EPOCH_ZNODE_NAME = "CurrentEpoch";
  private static final String DEFAULT_LOCAL_ROOT_DIR_PATH = "/decentralized-rmstore";
  private static final String DEFAULT_GLOBAL_ROOT_DIR_PATH = "/decentralized-rmstore";
  
  private static final String STATE_STORE_PATH_PREFIX = "StateStore_epoch_";
  
  private static final Version CURRENT_VERSION_INFO = ZKRMStateStore.CURRENT_VERSION_INFO;
  
  private ZKCuratorManager globalZkManager;
  private Thread verifyActiveStatusThread;
  
  private String localStateStoreKey;
  private ZKCuratorManager localZkManager;
  private ZKRMStateStore localStateStore;
  private Configuration localStateStoreConf;
  
  private Map<String, ZKRMRemoteStateStore> remoteStateStores;
  
  /* Global zookeeper znode paths */
  private String globalRootNodePath;
  private String globalEpochNodePath;
  private String sourceOfTruthNodePath;
  private String globalFencingNodePath;
  
  /* Local zookeeper znode paths */
  private String localMainRootDirPath;
  private String localEpochNodePath;
  
  /* Some znode version to perform fencing on transactions */
  private org.apache.zookeeper.Version globalEpochVersion;
  private org.apache.zookeeper.Version localEpochVersion;
  
  /* Epoch ZNode information to perform safe transactions */
  private GlobalLeadershipEpoch globalLeadershipEpoch;
  private LocalLeadershipEpoch localLeadershipEpoch;
  
  /* The rmState that is initially loaded, to be reused in method loadState() */
  private RMState initialRmState;
  
  private class ZKRMRemoteStateStore {
    
    private final ZKCuratorManager zkCuratorManager;
    private final String rootDirPath;
    private final String epochNodePath;
    
    //ZKRMStateStore instance to be used to call loadState()
    private final ZKRMStateStore zkrmStateStore;
    private final Configuration zkRMStateStoreConf;
    
    public ZKRMRemoteStateStore(Configuration conf, String rootDirPath) throws IOException {
      this.zkCuratorManager = new ZKCuratorManager(conf);
      this.rootDirPath = rootDirPath;
      this.epochNodePath = getNodePath(rootDirPath, LOCAL_EPOCH_ZNODE_NAME);
      this.zkrmStateStore = new ZKRMStateStore();
      this.zkrmStateStore.setZkManager(zkCuratorManager);
      this.zkRMStateStoreConf = new Configuration(conf);
    }
    
    public void close() {
      if (zkCuratorManager != null) {
        zkCuratorManager.close();
      }
    }
  
    public void trySetNewEpoch(int newEpoch) throws Exception {
      DecentralizedZKRMStateStore.trySetNewEpoch(zkCuratorManager.getCurator(), epochNodePath, newEpoch);
    }
  
    public RMState loadState(int epoch) throws Exception {
      try {
        String stateStorePath = getNodePath(rootDirPath, STATE_STORE_PATH_PREFIX + epoch);
        this.zkCuratorManager.start();
        this.zkRMStateStoreConf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, stateStorePath);
        this.zkrmStateStore.initInternal(zkRMStateStoreConf);
        //DO NOT CALL startInternal() !!!
        return this.zkrmStateStore.loadState();
      } finally {
        this.zkCuratorManager.close();
      }
    }
  }
  
  /**
   * Creates a new configuration object to be used by ZkCuratorManager. ZkCuratorManager was not designed
   * to be used with many configurable zookeeper clusters.
   * All properties are retrieved from the global conf for a specific ZkStateStore key and injected into
   * the expected global conf values for the ZkManager.
   */
  private Configuration createZkCuratorManagerConf(Configuration conf,
                                                   String key
  ) throws IOException {
    String zkAddress = conf.get("yarn.resourcemanager.distributed-zk-state-store." + key + ".address");
    if (zkAddress == null) {
      throw new IOException("yarn.resourcemanager.distributed-zk-state-store." + key + ".address is not configured");
    }
    String zkAclConf = conf.get("yarn.resourcemanager.distributed-zk-state-store." + key + ".acl",
        CommonConfigurationKeys.ZK_ACL_DEFAULT);
    int zkNumRetries = conf.getInt("yarn.resourcemanager.distributed-zk-state-store." + key + ".num-retries",
        CommonConfigurationKeys.ZK_NUM_RETRIES_DEFAULT);
    int zkSessionTimeoutMs = conf.getInt("yarn.resourcemanager.distributed-zk-state-store." + key + ".timeout-ms",
        CommonConfigurationKeys.ZK_TIMEOUT_MS_DEFAULT);
    int zkRetryIntervalMs = conf.getInt("yarn.resourcemanager.distributed-zk-state-store." + key + ".retry-interval-ms",
        CommonConfigurationKeys.ZK_RETRY_INTERVAL_MS_DEFAULT);
    Configuration zkCuratorManagerConf = new Configuration(conf);
    conf.set(CommonConfigurationKeys.ZK_ACL, zkAclConf);
    conf.set(CommonConfigurationKeys.ZK_ADDRESS, zkAddress);
    conf.setInt(CommonConfigurationKeys.ZK_NUM_RETRIES, zkNumRetries);
    conf.setInt(CommonConfigurationKeys.ZK_TIMEOUT_MS, zkSessionTimeoutMs);
    conf.setInt(CommonConfigurationKeys.ZK_RETRY_INTERVAL_MS, zkRetryIntervalMs);
    return zkCuratorManagerConf;
  }
  
  @Override
  protected void initInternal(Configuration conf) throws Exception {
    if (!HAUtil.isHAEnabled(getConfig()) || !conf.getBoolean(YarnConfiguration.RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_RM_RECOVERY_ENABLED)) {
      throw new ConfigurationException("HA and recovery must be enabled to use DecentralizedZKRMStateStore");
    }
    
    //Init local state store
    localStateStoreKey = conf.get("yarn.resourcemanager.distributed-zk-state-store.local.key");
    if (localStateStoreKey == null) {
      throw new ConfigurationException("yarn.resourcemanager.distributed-zk-state-store.local.key must be set");
    }
    
    //a ZKRMStateStore instance is created but not yet initialized. This is because
    //YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH will depend on the leadership epoch, thus
    //ZKRMStateStore can only be init during startInternal phase of this class
    localStateStore = new ZKRMStateStore();
    localStateStoreConf = createZkCuratorManagerConf(conf, localStateStoreKey);
    localZkManager = resourceManager.createAndStartZKManager(localStateStoreConf);
    
    //Init remote state stores
    String remoteStateStoreKeys = conf.get("yarn.resourcemanager.distributed-zk-state-store.remote.keys");
    if (remoteStateStoreKeys == null) {
      throw new ConfigurationException("yarn.resourcemanager.distributed-zk-state-store.remote.keys must be set");
    } else {
      remoteStateStores = new HashMap<>();
      for (String key : remoteStateStoreKeys.split(",")) {
        String rootDirPath = getLocalRootDirPath(conf, key);
        remoteStateStores.put(key, new ZKRMRemoteStateStore(createZkCuratorManagerConf(conf, key), rootDirPath));
      }
      if (remoteStateStores.size() == 0) {
        throw new ConfigurationException("At least one remote state store must be set");
      }
    }
  
    //Init the global zookeeper
    globalZkManager = resourceManager.getZKManager();
    if (globalZkManager == null) {
      globalZkManager = resourceManager.createAndStartZKManager(conf);
    }
    
    //create some path names to be reused
    globalRootNodePath = conf.get("yarn.resourcemanager.distributed-zk-state-store.parent-path",
        DEFAULT_GLOBAL_ROOT_DIR_PATH);
    globalEpochNodePath = getNodePath(globalRootNodePath, GLOBAL_EPOCH_ZNODE_NAME);
    globalFencingNodePath = getNodePath(globalRootNodePath, GLOBAL_FENCING_ZNODE_NAME);
    sourceOfTruthNodePath = getNodePath(globalRootNodePath, SOURCE_OF_TRUTH_ZNODE_NAME);
  
    localMainRootDirPath = getLocalRootDirPath(conf, localStateStoreKey);
    localEpochNodePath = getNodePath(localMainRootDirPath, LOCAL_EPOCH_ZNODE_NAME);
  }
  
  @Override
  protected void startInternal() throws Exception {
    //create root nodes if they don't exist
    globalZkManager.create(globalRootNodePath);
    localZkManager.create(localMainRootDirPath);
    
    //Retrieve a new epoch from the global zookeeper
    try {
      globalLeadershipEpoch = getNewEpoch();
    } catch (Exception e) {
      LOG.error("Impossible to get new epoch from global zookeeper", e);
      throw e;
    }
  
    LOG.info("New leadership epoch is " + globalLeadershipEpoch);
    
    LOG.info("Fencing local state store");
    try {
      localLeadershipEpoch = trySetNewEpoch(localZkManager.getCurator(), localEpochNodePath, globalLeadershipEpoch.getEpoch());
    } catch (Exception e) {
      LOG.error("Could no fence local state store, aborting leadership election", e);
      throw e;
    }
    
    LOG.info("Local state store fenced, trying to fence remote state stores");
    remoteStateStores.forEach((key, remoteStateStore) -> {
      try {
        remoteStateStore.trySetNewEpoch(globalLeadershipEpoch.getEpoch());
      } catch (Exception e) {
        LOG.warn("Could not set new epoch on remote state store ");
      }
    });
    
    LOG.info("Loading state store from source");
    StateStoreSource source = StateStoreSource.parseFromString(
        new String(globalZkManager.getData(sourceOfTruthNodePath), StandardCharsets.UTF_8)
    );
    if(!remoteStateStores.containsKey(source.getKey())) {
      throw new IOException("There is state store configured for source " + source);
    }
    ZKRMRemoteStateStore remoteStateStore = remoteStateStores.get(source.getKey());
    //TODO add managing dumps as source
    //we need to load the version of epoch -1 from the source
    initialRmState = remoteStateStore.loadState(source.getEpoch());
    LOG.info("StateStore loaded from source " + source);
    
    dump(localZkManager.getCurator(), initialRmState, localLeadershipEpoch.getEpoch());
    LOG.info("StateStore dumped to local state store");
    
    LOG.info("Setting new source-of-truth for epoch " + globalLeadershipEpoch);
    try {
      setNewSourceOfTruth(new StateStoreSource(localStateStoreKey, globalLeadershipEpoch.getEpoch()));
    } catch (Exception e) {
      LOG.error("Could not set new source of truth, could be due to race with another resourcemanager instance", e);
    }
    
    //At this point, we are the new leader
    
    conf.get(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH,
        YarnConfiguration.DEFAULT_ZK_RM_STATE_STORE_PARENT_PATH);
    
    verifyActiveStatusThread = new VerifyActiveStatusThread();
    verifyActiveStatusThread.start();
  
    //Loading and dumping the state store is part of a successful leader election, thus perform it at start
    //since we verify that recovery is enabled, this is consistent
    //
    //TODO loadVersion, getCurrentVersion, getAndIncrementEpoch, happens before load of the state store
    //They must reside on the global zookeeper OR maybe a better way is to perform the load/dump directly
    //because anyway we check that recovery is enable to use this class
    
    
  }
  
  private void dump(CuratorFramework curator, RMState rmStateSnapshot, int epoch) {
  
  }
  
  private String getLocalRootDirPath(Configuration conf, String key) {
    return conf.get("yarn.resourcemanager.distributed-zk-state-store." + key + ".parent-path",
        DEFAULT_LOCAL_ROOT_DIR_PATH);
  }
  
  private String getNodePath(String root, String nodeName) {
    return (root + "/" + nodeName);
  }
  
  /**
   *
   */
  private GlobalLeadershipEpoch getNewEpoch() throws Exception {
    //Init the epoch node if needed
    seedEpochNode(globalZkManager.getCurator(), globalEpochNodePath);
  
    Stat stat = new Stat();
    //loop until we can set a new epoch
    boolean newEpochGenerated = false;
    int newEpoch;
    do {
      byte[] currentData = globalZkManager.getCurator().getData().storingStatIn(stat).forPath(globalEpochNodePath);
      int currentEpoch = ByteBuffer.wrap(currentData).getInt();
      newEpoch = currentEpoch + 1;
      byte[] newData = new byte[4];
      ByteBuffer.wrap(newData).putInt(newEpoch);
      try {
        globalZkManager.getCurator().setData().withVersion(stat.getVersion()).forPath(globalEpochNodePath, newData);
        newEpochGenerated = true;
      } catch (KeeperException.BadVersionException ignore) {
      }
    } while(!newEpochGenerated);
    
    return new GlobalLeadershipEpoch(stat.getVersion() + 1, newEpoch);
  }
  
  /**
   * Will try to set a new epoch node only if the new epoch is higher than the current one.
   * Race conditions are resolved by checking of the epochNode changed between the moment we get its value
   * and the moment we set it (compare-and-swap schemantics).
   * Loop-try until until either:
   *   - new epoch is set
   *   - current epoch is too recent
   *   - an error occurs
   */
  private static LocalLeadershipEpoch trySetNewEpoch(CuratorFramework curator, String epochNodePath, int newEpoch) throws Exception {
    //Init the epoch node if needed
    seedEpochNode(curator, epochNodePath);
    
    Stat stat = new Stat();
  
    boolean set = false;
    while(!set) {
      byte[] currentData = curator.getData().storingStatIn(stat).forPath(epochNodePath);
      int currentEpoch = ByteBuffer.wrap(currentData).getInt();
  
      if (currentEpoch < newEpoch) {
        byte[] newData = new byte[4];
        ByteBuffer.wrap(newData).putInt(newEpoch);
        try {
          curator.setData().withVersion(stat.getVersion()).forPath(epochNodePath, newData);
          set = true;
        } catch (KeeperException.BadVersionException ignore) {
        }
      } else {
        throw new IOException("Cannot set epoch " + newEpoch + " as current epoch is higher (" + currentEpoch + ")");
      }
    }
    
    return new LocalLeadershipEpoch(stat.getVersion() + 1, newEpoch);
  }
  
  /**
   * New initial value for an epoch node is 0
   */
  private static void seedEpochNode(CuratorFramework curator, String znodePath) {
    byte[] seed = new byte[4];
    ByteBuffer.wrap(seed).putInt(0);
    try {
      curator.create().creatingParentContainersIfNeeded().forPath(znodePath, seed);
    } catch (Exception e){
      //ignore
    }
  }
  
  /**
   * Sets the new source of truth, transaction only valid if the current epoch is the one used to fence state stores,
   * prevents racy leader election issues.
   */
  private void setNewSourceOfTruth(StateStoreSource newSource) throws Exception {
    CuratorFramework curator = globalZkManager.getCurator();
  
    byte[] data = newSource.toByteArray();
    
    List<CuratorOp> ops = new ArrayList<>();
    ops.add(curator.transactionOp().check().withVersion(globalLeadershipEpoch.getVersion()).forPath(globalEpochNodePath));
    ops.add(curator.transactionOp().setData().forPath(sourceOfTruthNodePath, data));
    curator.transaction().forOperations(ops);
  }
  
  @Override
  public RMState loadState() throws Exception {
    //state has already been loading at startup, return the loaded object
    if(initialRmState != null) {
      RMState rmState = initialRmState;
      initialRmState = null;
      return rmState;
    } else {
      throw new RuntimeException("rmStateSnapshot is null, not expected, please check code");
    }
  }
  
  @Override
  protected void closeInternal() throws Exception {
    if (verifyActiveStatusThread != null) {
      verifyActiveStatusThread.interrupt();
      verifyActiveStatusThread.join(1000);
    }
    
    for(ZKRMRemoteStateStore remoteStateStore: remoteStateStores.values()) {
      remoteStateStore.close();
    }
    
    if (localStateStore != null) {
      localStateStore.closeInternal();
    }
    
    if (resourceManager.getZKManager() == null) {
      CuratorFramework curatorFramework = globalZkManager.getCurator();
      IOUtils.closeStream(curatorFramework);
    }
  }
  
  @Override
  protected Version loadVersion() throws Exception {
    return localStateStore.loadVersion();
  }
  
  @Override
  protected void storeVersion() throws Exception {
    localStateStore.storeVersion();
  }
  
  @Override
  protected Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }
  
  @Override
  public long getAndIncrementEpoch() throws Exception {
    return localStateStore.getAndIncrementEpoch();
  }
  
  @Override
  protected void storeApplicationStateInternal(ApplicationId appId, ApplicationStateData appStateData)
      throws Exception {
    localStateStore.storeApplicationStateInternal(appId, appStateData);
  }
  
  @Override
  protected void updateApplicationStateInternal(ApplicationId appId, ApplicationStateData appStateData)
      throws Exception {
    localStateStore.updateApplicationStateInternal(appId, appStateData);
  }
  
  @Override
  protected void storeApplicationAttemptStateInternal(ApplicationAttemptId attemptId,
                                                      ApplicationAttemptStateData attemptStateData) throws Exception {
    localStateStore.storeApplicationAttemptStateInternal(attemptId, attemptStateData);
  }
  
  @Override
  protected void updateApplicationAttemptStateInternal(ApplicationAttemptId attemptId,
                                                       ApplicationAttemptStateData attemptStateData) throws Exception {
    localStateStore.updateApplicationAttemptStateInternal(attemptId, attemptStateData);
  }
  
  @Override
  protected void storeRMDelegationTokenState(RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    localStateStore.storeRMDelegationTokenState(rmDTIdentifier, renewDate);
  }
  
  @Override
  protected void removeRMDelegationTokenState(RMDelegationTokenIdentifier rmDTIdentifier) throws Exception {
    localStateStore.removeRMDelegationTokenState(rmDTIdentifier);
  }
  
  @Override
  protected void updateRMDelegationTokenState(RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    localStateStore.updateRMDelegationTokenState(rmDTIdentifier, renewDate);
  }
  
  @Override
  protected void storeRMDTMasterKeyState(DelegationKey delegationKey) throws Exception {
    localStateStore.storeRMDTMasterKeyState(delegationKey);
  }
  
  @Override
  protected void storeReservationState(ReservationAllocationStateProto reservationAllocation, String planName,
                                       String reservationIdName) throws Exception {
    localStateStore.storeReservationState(reservationAllocation, planName, reservationIdName);
  }
  
  @Override
  protected void removeReservationState(String planName, String reservationIdName) throws Exception {
    localStateStore.removeReservationState(planName, reservationIdName);
  }
  
  @Override
  protected void removeRMDTMasterKeyState(DelegationKey delegationKey) throws Exception {
    localStateStore.removeRMDTMasterKeyState(delegationKey);
  }
  
  @Override
  protected void storeOrUpdateAMRMTokenSecretManagerState(AMRMTokenSecretManagerState amrmTokenSecretManagerState,
                                                          boolean isUpdate) throws Exception {
    localStateStore.storeOrUpdateAMRMTokenSecretManagerState(amrmTokenSecretManagerState, isUpdate);
  }
  
  @Override
  protected void removeApplicationStateInternal(ApplicationStateData appState) throws Exception {
    localStateStore.removeApplicationStateInternal(appState);
  }
  
  @Override
  protected void removeApplicationAttemptInternal(ApplicationAttemptId attemptId) throws Exception {
    localStateStore.removeApplicationAttemptInternal(attemptId);
  }
  
  @Override
  public void deleteStore() throws Exception {
    localStateStore.deleteStore();
  }
  
  @Override
  public void removeApplication(ApplicationId removeAppId) throws Exception {
    localStateStore.removeApplication(removeAppId);
  }
  
  @Override
  protected void storeProxyCACertState(X509Certificate caCert, PrivateKey caPrivateKey) throws Exception {
    localStateStore.storeProxyCACertState(caCert, caPrivateKey);
  }
  
  
  /**
   * Helper class that periodically attempts creating a znode to ensure that
   * this RM continues to be the Active.
   */
  private class VerifyActiveStatusThread extends Thread {
    VerifyActiveStatusThread() {
      super(DecentralizedZKRMStateStore.VerifyActiveStatusThread.class.getName());
    }
    
    @Override
    public void run() {
      try {
        while (!isFencedState()) {
          // Create and delete fencing node
          zkManager.createTransaction(zkAcl, fencingNodePath).commit();
          Thread.sleep(zkSessionTimeout);
        }
      } catch (InterruptedException ie) {
        LOG.info(getName() + " thread interrupted! Exiting!");
        interrupt();
      } catch (Exception e) {
        notifyStoreOperationFailed(new StoreFencedException());
      }
    }
  }
  
  private static abstract class LeaderShipEpoch {
    private final int version;
    private final int epoch;
  
    public LeaderShipEpoch(int version, int epoch) {
      this.version = version;
      this.epoch = epoch;
    }
  
    @Override
    public String toString() {
      return "LeaderShipEpoch{" +
          "version=" + version +
          ", epoch=" + epoch +
          '}';
    }
  
    public int getVersion() {
      return version;
    }
    
    public int getEpoch() {
      return epoch;
    }
  }
  
  private static class GlobalLeadershipEpoch extends LeaderShipEpoch{
    public GlobalLeadershipEpoch(int version, int epoch) {
      super(version, epoch);
    }
  }
  
  private static class LocalLeadershipEpoch extends LeaderShipEpoch{
    public LocalLeadershipEpoch(int version, int epoch) {
      super(version, epoch);
    }
  }
  
  private static class StateStoreSource {
    private final String key;
    private final int epoch;
  
    public StateStoreSource(String key, int epoch) {
      this.key = key;
      this.epoch = epoch;
    }
    
    public byte[] toByteArray() {
      return (key + "-epoch-" + epoch).getBytes(StandardCharsets.UTF_8);
    }
    
    public static StateStoreSource parseFromString(String s) {
      String[] splits = s.split("-epoch-");
      return new StateStoreSource(splits[0], Integer.parseInt(splits[1]));
    }
  
    public String getKey() {
      return key;
    }
    
    public int getEpoch() {
      return epoch;
    }
  
    @Override
    public String toString() {
      return "StateStoreSource{" +
          "key='" + key + '\'' +
          ", epoch=" + epoch +
          '}';
    }
  }
}
