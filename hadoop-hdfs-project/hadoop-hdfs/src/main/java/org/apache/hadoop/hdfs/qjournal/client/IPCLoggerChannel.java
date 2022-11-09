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
package org.apache.hadoop.hdfs.qjournal.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalOutOfSyncException;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.qjournal.server.GetJournalEditServlet;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.StopWatch;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.net.InetAddresses;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.UncaughtExceptionHandlers;

/**
 * Channel to a remote JournalNode using Hadoop IPC.
 * All of the calls are run on a separate thread, and return
 * {@link ListenableFuture} instances to wait for their result.
 * This allows calls to be bound together using the {@link QuorumCall}
 * class.
 */
@InterfaceAudience.Private
public class IPCLoggerChannel implements AsyncLogger {

  private final Configuration conf;
  protected final InetSocketAddress addr;
  private QJournalProtocol proxy;

  /**
   * Executes tasks submitted to it serially, in FIFO order
   * (generally used for write tasks that should not be reordered).
   */
  private final FifoExecutor fifoExecutor;
  /**
   * Executes tasks submitted to it in parallel with each other and with those
   * submitted to singleThreadExecutor (generally used for read tasks that can
   * be safely reordered and interleaved with writes).
   */
  private final ListeningExecutorService parallelExecutor;
  private long ipcSerial = 0;
  private long epoch = -1;
  private long committedTxId = HdfsServerConstants.INVALID_TXID;
  
  private final String journalId;
  private final String nameServiceId;

  private final NamespaceInfo nsInfo;

  private URL httpServerURL;

  private final IPCLoggerChannelMetrics metrics;
  
  /**
   * The number of bytes of edits data still in the queue.
   */
  private int queuedEditsSizeBytes = 0;
  
  /**
   * The highest txid that has been successfully logged on the remote JN.
   */
  private long highestAckedTxId = 0;

  /**
   * Nanotime of the last time we successfully journaled some edits
   * to the remote node.
   */
  private long lastAckNanos = 0;

  /**
   * Nanotime of the last time that committedTxId was update. Used
   * to calculate the lag in terms of time, rather than just a number
   * of txns.
   */
  private long lastCommitNanos = 0;
  
  /**
   * The maximum number of bytes that can be pending in the queue.
   * This keeps the writer from hitting OOME if one of the loggers
   * starts responding really slowly. Eventually, the queue
   * overflows and it starts to treat the logger as having errored.
   */
  private final int queueSizeLimitBytes;

  /**
   * If this logger misses some edits, or restarts in the middle of
   * a segment, the writer won't be able to write any more edits until
   * the beginning of the next segment. Upon detecting this situation,
   * the writer sets this flag to true to avoid sending useless RPCs.
   */
  private boolean outOfSync = false;
  
  /**
   * Stopwatch which starts counting on each heartbeat that is sent
   */
  private final StopWatch lastHeartbeatStopwatch = new StopWatch();
  
  private static final long HEARTBEAT_INTERVAL_MILLIS = 1000;

  private static final long WARN_JOURNAL_MILLIS_THRESHOLD = 1000;
  
  static final Factory FACTORY = new AsyncLogger.Factory() {
    @Override
    public AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
        String journalId, String nameServiceId, InetSocketAddress addr) {
      return new IPCLoggerChannel(conf, nsInfo, journalId, nameServiceId, addr);
    }
  };

  public IPCLoggerChannel(Configuration conf,
      NamespaceInfo nsInfo,
      String journalId,
      InetSocketAddress addr) {
    this(conf, nsInfo, journalId, null, addr);
  }

  public IPCLoggerChannel(Configuration conf,
                          NamespaceInfo nsInfo,
                          String journalId,
                          String nameServiceId,
                          InetSocketAddress addr) {
    this.conf = conf;
    this.nsInfo = nsInfo;
    this.journalId = journalId;
    this.nameServiceId = nameServiceId;
    this.addr = addr;
    this.queueSizeLimitBytes = 1024 * 1024 * conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_QUEUE_SIZE_LIMIT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_QUEUE_SIZE_LIMIT_DEFAULT);
    
    boolean mergeEdits = conf.getBoolean(
        DFSConfigKeys.DFS_QJOURNAL_MERGE_SEND_EDIT_REQUESTS_KEY,
        DFSConfigKeys.DFS_QJOURNAL_MERGE_SEND_EDIT_REQUESTS_DEFAULT
    );

    fifoExecutor = createFifoExecutor(mergeEdits);

    parallelExecutor = MoreExecutors.listeningDecorator(
        createParallelExecutor());
    
    metrics = IPCLoggerChannelMetrics.create(this);
  }
  
  interface FifoExecutor {
    ListenableFuture<?> submit(Runnable r);
    <R> ListenableFuture<R> submit(Callable<R> c);
    <R> ListenableFuture<R> submit(FifoExecutorTask<R> c);
    void shutdown();
  }

  interface FifoExecutorTask<R> {
    R run() throws Exception;
    boolean canMergeWith(FifoExecutorTask<?> other);
    <U> void merge(FifoExecutorTask<U> other) throws Exception;
  }

  static class MergingTaskFifoExecutor extends Thread implements FifoExecutor {

    //special task used to stop the consumer thread
    private static FutureTask<Void> SHUTDOWN = new FutureTask<>(null);

    private final boolean isSynchronous;
    private final long syncTimeoutMs;

    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition notEmpty = lock.newCondition();
    private final Condition empty = lock.newCondition();

    private boolean isShuttingDown = false;

    private final LinkedList<FutureTask<?>> tasks = new LinkedList<>();

    public MergingTaskFifoExecutor() {
      this(false, 0);
    }

    public MergingTaskFifoExecutor(boolean isSynchronous, long timeoutMs) {
      this.isSynchronous = isSynchronous;
      this.syncTimeoutMs = timeoutMs;
    }

    public ListenableFuture<Void> submit(Runnable r) {
      return submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          r.run();
          return null;
        }
      });
    }

    public <T> ListenableFuture<T> submit(Callable<T> c) {
      return submit(new CallableTask<>(c));
    }

    public <T> ListenableFuture<T> submit(FifoExecutorTask<T> mt) {

      FutureTask<T> ft = new FutureTask<>(mt);

      lock.lock();

      try {

        if(isShuttingDown)
          throw new RejectedExecutionException("Cannot submit task after calling shutdown");

        FutureTask<?> lastInQueue = tasks.peekLast();
        if(lastInQueue != null && mt.canMergeWith(lastInQueue.mt)) {
          try {
            lastInQueue.mt.merge(mt);
            ft.future.setFuture((ListenableFuture<? extends T>) lastInQueue.future);
          } catch (Exception e) {
            //cannot merge so just queue the task
            tasks.add(ft);
          }
        } else {
          tasks.add(ft);
        }

        notEmpty.signal();

      } finally {
        lock.unlock();
      }

      //Useful for testing, make this synchronous
      //by waiting the future after submission
      if(isSynchronous) {
        try {
          ft.future.get(syncTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
          //do nothing let the caller managed exceptions
        }
      }

      return ft.future;
    }

    @Override
    public void run() {
      while (!isInterrupted()) {
        lock.lock();

        FutureTask<?> ft = null;
        try {
            try {
              while((ft = tasks.poll()) == null) {
                  empty.signal();
                  notEmpty.await();
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
        } finally {
          lock.unlock();
        }

        //ft can be null if thread is interrupted
        if(ft != null) {
          if (ft == SHUTDOWN)
            return;

          ft.execute();
        }
      }
    }

    public void shutdown() {
      lock.lock();
      try {
       isShuttingDown = true;

       try {
          while(tasks.size() > 0) {
            empty.await();
          }
       } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
       } finally {
         //tell the producer thread to stop
         tasks.add(SHUTDOWN);
         notEmpty.signal();
       }

      } finally {
        lock.unlock();
      }

      try {
        this.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    private static class FutureTask<R> {

      private FifoExecutorTask<R> mt;
      private SettableFuture<R> future;

      private FutureTask(FifoExecutorTask<R> mt) {
        this.mt = mt;
        this.future = SettableFuture.create();
      }

      private void execute() {
        try {
          R res = mt.run();
          future.set(res);
        } catch (Throwable t) {
          future.setException(t);
        }
      }

    }

    private static class CallableTask<R> implements FifoExecutorTask<R> {

      private final Callable<R> callable;

      public CallableTask(Callable<R> c) {
        this.callable = c;
      }

      @Override
      public R run() throws Exception {
        return callable.call();
      }

      @Override
      public boolean canMergeWith(FifoExecutorTask<?> other) {
        return false;
      }

      @Override
      public <U> void merge(FifoExecutorTask<U> other) {
      }
    }
  }

  static class ExecutorServiceFifoExecutor implements FifoExecutor {

    private final ListeningExecutorService executorService;

    public ExecutorServiceFifoExecutor(ExecutorService executorService) {
      this.executorService = MoreExecutors.listeningDecorator(executorService);
    }

    @Override
    public ListenableFuture<?> submit(Runnable r) {
      return executorService.submit(r);
    }

    @Override
    public <R> ListenableFuture<R> submit(Callable<R> c) {
      return executorService.submit(c);
    }

    @Override
    public <R> ListenableFuture<R> submit(FifoExecutorTask<R> c) {
      return executorService.submit(new Callable<R>() {
        @Override
        public R call() throws Exception {
          return c.run();
        }
      });
    }

    @Override
    public void shutdown() {
      executorService.shutdown();
    }
  }

  private final class SendEditsFifoExecutorTask implements FifoExecutorTask<Void> {

    private final byte[] originalData;
    private final long segmentTxId;
    private final long firstTxnId;

    //mutated fields for merge operation
    private int numTxns;
    private long lastSubmitNanos;
    private final DataOutputBuffer buf;

    private SendEditsFifoExecutorTask(long segmentTxId, long firstTxnId, int numTxns, byte[] data, long submitNanos) throws IOException {
      this.segmentTxId = segmentTxId;
      this.firstTxnId = firstTxnId;
      this.numTxns = numTxns;
      this.originalData = data;
      this.buf = new DataOutputBuffer(data.length);
      this.buf.write(data);
      this.lastSubmitNanos = submitNanos;
    }

    @Override
    public boolean canMergeWith(FifoExecutorTask<?> other) {
      return other instanceof SendEditsFifoExecutorTask;
    }

    @Override
    public <U> void merge(FifoExecutorTask<U> other) throws IOException {
      SendEditsFifoExecutorTask otherSendEdits = (SendEditsFifoExecutorTask) other;

      Preconditions.checkArgument(otherSendEdits.segmentTxId == this.segmentTxId,
              "cannot merge edit stream from a different segmentTxId");
      Preconditions.checkArgument(otherSendEdits.firstTxnId > this.firstTxnId,
              "cannot merge edit stream with smaller firstTxnId");

      this.numTxns += otherSendEdits.numTxns;
      this.lastSubmitNanos = otherSendEdits.lastSubmitNanos;
      this.buf.write(otherSendEdits.originalData);
    }

    @Override
    public Void run() throws Exception {
      throwIfOutOfSync();

      ByteArrayOutputStream baos = new ByteArrayOutputStream(this.buf.size());
      this.buf.writeTo(baos);
      byte[] data = baos.toByteArray();

      long rpcSendTimeNanos = System.nanoTime();
      try {
        getProxy().journal(createReqInfo(),
                segmentTxId, firstTxnId, numTxns, data);
      } catch (IOException e) {
        QuorumJournalManager.LOG.warn(
                "Remote journal " + IPCLoggerChannel.this + " failed to " +
                        "write txns " + firstTxnId + "-" + (firstTxnId + numTxns - 1) +
                        ". Will try to write to this JN again after the next " +
                        "log roll.", e);
        synchronized (IPCLoggerChannel.this) {
          outOfSync = true;
        }
        throw e;
      } finally {
        long now = System.nanoTime();
        long rpcTime = TimeUnit.MICROSECONDS.convert(
                now - rpcSendTimeNanos, TimeUnit.NANOSECONDS);
        long endToEndTime = TimeUnit.MICROSECONDS.convert(
                now - lastSubmitNanos, TimeUnit.NANOSECONDS);
        metrics.addWriteEndToEndLatency(endToEndTime);
        metrics.addWriteRpcLatency(rpcTime);
        if (rpcTime / 1000 > WARN_JOURNAL_MILLIS_THRESHOLD) {
          QuorumJournalManager.LOG.warn(
                  "Took " + (rpcTime / 1000) + "ms to send a batch of " +
                          numTxns + " edits (" + data.length + " bytes) to " +
                          "remote journal " + IPCLoggerChannel.this);
        }
      }
      synchronized (IPCLoggerChannel.this) {
        highestAckedTxId = firstTxnId + numTxns - 1;
        lastAckNanos = lastSubmitNanos;
      }

      return null;
    }

  }
  
  @Override
  public synchronized void setEpoch(long epoch) {
    this.epoch = epoch;
  }
  
  @Override
  public synchronized void setCommittedTxId(long txid) {
    Preconditions.checkArgument(txid >= committedTxId,
        "Trying to move committed txid backwards in client " +
         "old: %s new: %s", committedTxId, txid);
    this.committedTxId = txid;
    this.lastCommitNanos = System.nanoTime();
  }
  
  @Override
  public void close() {
    // No more tasks may be submitted after this point.
    fifoExecutor.shutdown();
    parallelExecutor.shutdown();
    if (proxy != null) {
      // TODO: this can hang for quite some time if the client
      // is currently in the middle of a call to a downed JN.
      // We should instead do this asynchronously, and just stop
      // making any more calls after this point (eg clear the queue)
      RPC.stopProxy(proxy);
    }
  }
  
  protected QJournalProtocol getProxy() throws IOException {
    if (proxy != null) return proxy;
    proxy = createProxy();
    return proxy;
  }
  
  protected QJournalProtocol createProxy() throws IOException {
    final Configuration confCopy = new Configuration(conf);
    
    // Need to set NODELAY or else batches larger than MTU can trigger
    // 40ms nagling delays.
    confCopy.setBoolean(
        CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
        true);
    
    RPC.setProtocolEngine(confCopy,
        QJournalProtocolPB.class, ProtobufRpcEngine2.class);
    return SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<QJournalProtocol>() {
          @Override
          public QJournalProtocol run() throws IOException {
            RPC.setProtocolEngine(confCopy,
                QJournalProtocolPB.class, ProtobufRpcEngine2.class);
            QJournalProtocolPB pbproxy = RPC.getProxy(
                QJournalProtocolPB.class,
                RPC.getProtocolVersion(QJournalProtocolPB.class),
                addr, confCopy);
            return new QJournalProtocolTranslatorPB(pbproxy);
          }
        });
  }
  
  
  /**
   * Separated out for easy overriding in tests.
   */
  @VisibleForTesting
  protected FifoExecutor createFifoExecutor(boolean mergeEdits) {
    if (mergeEdits) {
      return createMergingTaskFifoExecutor();
    } else {
      return createExecutorServiceFifoExecutor();
    }
  }

  private FifoExecutor createMergingTaskFifoExecutor() {
    MergingTaskFifoExecutor mergingTaskFifoExecutor = new MergingTaskFifoExecutor();
    mergingTaskFifoExecutor.setDaemon(true);
    mergingTaskFifoExecutor.setName("Logger channel (from single-thread executor) to " + addr);
    mergingTaskFifoExecutor.start();
    return mergingTaskFifoExecutor;
  }

  private FifoExecutor createExecutorServiceFifoExecutor() {
    ExecutorService executorService = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("Logger channel (from single-thread executor) to " +
                            addr)
                    .setUncaughtExceptionHandler(
                            UncaughtExceptionHandlers.systemExit())
                    .build());
    return new ExecutorServiceFifoExecutor(executorService);
  }

  /**
   * Separated out for easy overriding in tests.
   */
  @VisibleForTesting
  protected ExecutorService createParallelExecutor() {
    int numThreads =
        conf.getInt(DFSConfigKeys.DFS_QJOURNAL_PARALLEL_READ_NUM_THREADS_KEY,
            DFSConfigKeys.DFS_QJOURNAL_PARALLEL_READ_NUM_THREADS_DEFAULT);
    return new ThreadPoolExecutor(1, numThreads, 60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("Logger channel (from parallel executor) to " + addr)
            .setUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit())
            .build());
  }
  
  @Override
  public URL buildURLToFetchLogs(long segmentTxId) {
    Preconditions.checkArgument(segmentTxId > 0,
        "Invalid segment: %s", segmentTxId);
    Preconditions.checkState(hasHttpServerEndPoint(), "No HTTP/HTTPS endpoint");
    
    try {
      String path = GetJournalEditServlet.buildPath(
          journalId, segmentTxId, nsInfo, true);
      return new URL(httpServerURL, path);
    } catch (MalformedURLException e) {
      // should never get here.
      throw new RuntimeException(e);
    }
  }

  private synchronized RequestInfo createReqInfo() {
    Preconditions.checkState(epoch > 0, "bad epoch: " + epoch);
    return new RequestInfo(journalId, nameServiceId,
        epoch, ipcSerial++, committedTxId);
  }

  @VisibleForTesting
  synchronized long getNextIpcSerial() {
    return ipcSerial;
  }

  public synchronized int getQueuedEditsSize() {
    return queuedEditsSizeBytes;
  }
  
  public InetSocketAddress getRemoteAddress() {
    return addr;
  }

  /**
   * @return true if the server has gotten out of sync from the client,
   * and thus a log roll is required for this logger to successfully start
   * logging more edits.
   */
  public synchronized boolean isOutOfSync() {
    return outOfSync;
  }
  
  @VisibleForTesting
  void waitForAllPendingCalls() throws InterruptedException {
    try {
      fifoExecutor.submit(new Runnable() {
        @Override
        public void run() {
        }
      }).get();
    } catch (ExecutionException e) {
      // This can't happen!
      throw new AssertionError(e);
    }
  }

  @Override
  public ListenableFuture<Boolean> isFormatted() {
    return fifoExecutor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return getProxy().isFormatted(journalId, nameServiceId);
      }
    });
  }

  @Override
  public ListenableFuture<GetJournalStateResponseProto> getJournalState() {
    return fifoExecutor.submit(new Callable<GetJournalStateResponseProto>() {
      @Override
      public GetJournalStateResponseProto call() throws IOException {
        GetJournalStateResponseProto ret =
            getProxy().getJournalState(journalId, nameServiceId);
        constructHttpServerURI(ret);
        return ret;
      }
    });
  }

  @Override
  public ListenableFuture<NewEpochResponseProto> newEpoch(
      final long epoch) {
    return fifoExecutor.submit(new Callable<NewEpochResponseProto>() {
      @Override
      public NewEpochResponseProto call() throws IOException {
        return getProxy().newEpoch(journalId, nameServiceId, nsInfo, epoch);
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> sendEdits(
      final long segmentTxId, final long firstTxnId,
      final int numTxns, final byte[] data) {
    try {
      reserveQueueSpace(data.length);
    } catch (LoggerTooFarBehindException e) {
      return Futures.immediateFailedFuture(e);
    }
    
    // When this batch is acked, we use its submission time in order
    // to calculate how far we are lagging.
    final long submitNanos = System.nanoTime();

    try {
      ListenableFuture<Void> ret = fifoExecutor.submit(new SendEditsFifoExecutorTask(segmentTxId, firstTxnId, numTxns, data, submitNanos));
      
      // It was submitted to the queue, so adjust the length
      // once the call completes, regardless of whether it
      // succeeds or fails.
      Futures.addCallback(ret, new FutureCallback<Void>() {
        @Override
        public void onFailure(Throwable t) {
          unreserveQueueSpace(data.length);
        }
      
        @Override
        public void onSuccess(Void t) {
          unreserveQueueSpace(data.length);
        }
      }, MoreExecutors.directExecutor());
    
      return ret;
    } catch (IOException e) {
      unreserveQueueSpace(data.length);
      return Futures.immediateFailedFuture(e);
    }
  
  }

  private void throwIfOutOfSync()
      throws JournalOutOfSyncException, IOException {
    if (isOutOfSync()) {
      // Even if we're out of sync, it's useful to send an RPC
      // to the remote node in order to update its lag metrics, etc.
      heartbeatIfNecessary();
      throw new JournalOutOfSyncException(
          "Journal disabled until next roll");
    }
  }

  /**
   * When we've entered an out-of-sync state, it's still useful to periodically
   * send an empty RPC to the server, such that it has the up to date
   * committedTxId. This acts as a sanity check during recovery, and also allows
   * that node's metrics to be up-to-date about its lag.
   * 
   * In the future, this method may also be used in order to check that the
   * current node is still the current writer, even if no edits are being
   * written.
   */
  private void heartbeatIfNecessary() throws IOException {
    if (lastHeartbeatStopwatch.now(TimeUnit.MILLISECONDS)
        > HEARTBEAT_INTERVAL_MILLIS || !lastHeartbeatStopwatch.isRunning()) {
      try {
        getProxy().heartbeat(createReqInfo());
      } finally {
        // Don't send heartbeats more often than the configured interval,
        // even if they fail.
        lastHeartbeatStopwatch.reset().start();
      }
    }
  }

  private synchronized void reserveQueueSpace(int size)
      throws LoggerTooFarBehindException {
    Preconditions.checkArgument(size >= 0);
    if (queuedEditsSizeBytes + size > queueSizeLimitBytes &&
        queuedEditsSizeBytes > 0) {
      QuorumJournalManager.LOG.warn("Pending edits to " + IPCLoggerChannel.this
          + " is going to exceed limit size: " + queueSizeLimitBytes
          + ", current queued edits size: " + queuedEditsSizeBytes
          + ", will silently drop " + size + " bytes of edits!");
      throw new LoggerTooFarBehindException();
    }
    queuedEditsSizeBytes += size;
  }
  
  private synchronized void unreserveQueueSpace(int size) {
    Preconditions.checkArgument(size >= 0);
    queuedEditsSizeBytes -= size;
  }

  @Override
  public ListenableFuture<Void> format(final NamespaceInfo nsInfo,
      final boolean force) {
    return fifoExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        getProxy().format(journalId, nameServiceId, nsInfo, force);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> startLogSegment(final long txid,
      final int layoutVersion) {
    return fifoExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().startLogSegment(createReqInfo(), txid, layoutVersion);
        synchronized (IPCLoggerChannel.this) {
          if (outOfSync) {
            outOfSync = false;
            QuorumJournalManager.LOG.info(
                "Restarting previously-stopped writes to " +
                IPCLoggerChannel.this + " in segment starting at txid " +
                txid);
          }
        }
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> finalizeLogSegment(
      final long startTxId, final long endTxId) {
    return fifoExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        throwIfOutOfSync();
        
        getProxy().finalizeLogSegment(createReqInfo(), startTxId, endTxId);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> purgeLogsOlderThan(final long minTxIdToKeep) {
    return fifoExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        getProxy().purgeLogsOlderThan(createReqInfo(), minTxIdToKeep);
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<GetJournaledEditsResponseProto> getJournaledEdits(
      long fromTxnId, int maxTransactions) {
    return parallelExecutor.submit(
        new Callable<GetJournaledEditsResponseProto>() {
          @Override
          public GetJournaledEditsResponseProto call() throws IOException {
            return getProxy().getJournaledEdits(journalId, nameServiceId,
                fromTxnId, maxTransactions);
          }
        });
  }

  @Override
  public ListenableFuture<RemoteEditLogManifest> getEditLogManifest(
      final long fromTxnId, final boolean inProgressOk) {
    return parallelExecutor.submit(new Callable<RemoteEditLogManifest>() {
      @Override
      public RemoteEditLogManifest call() throws IOException {
        GetEditLogManifestResponseProto ret = getProxy().getEditLogManifest(
            journalId, nameServiceId, fromTxnId, inProgressOk);
        // Update the http port, since we need this to build URLs to any of the
        // returned logs.
        constructHttpServerURI(ret);
        return PBHelper.convert(ret.getManifest());
      }
    });
  }

  @Override
  public ListenableFuture<PrepareRecoveryResponseProto> prepareRecovery(
      final long segmentTxId) {
    return fifoExecutor.submit(new Callable<PrepareRecoveryResponseProto>() {
      @Override
      public PrepareRecoveryResponseProto call() throws IOException {
        if (!hasHttpServerEndPoint()) {
          // force an RPC call so we know what the HTTP port should be if it
          // haven't done so.
          GetJournalStateResponseProto ret = getProxy().getJournalState(
              journalId, nameServiceId);
          constructHttpServerURI(ret);
        }
        return getProxy().prepareRecovery(createReqInfo(), segmentTxId);
      }
    });
  }

  @Override
  public ListenableFuture<Void> acceptRecovery(
      final SegmentStateProto log, final URL url) {
    return fifoExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().acceptRecovery(createReqInfo(), log, url);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> doPreUpgrade() {
    return fifoExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().doPreUpgrade(journalId);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> doUpgrade(final StorageInfo sInfo) {
    return fifoExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().doUpgrade(journalId, sInfo);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> doFinalize() {
    return fifoExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().doFinalize(journalId, nameServiceId);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Boolean> canRollBack(final StorageInfo storage,
      final StorageInfo prevStorage, final int targetLayoutVersion) {
    return fifoExecutor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return getProxy().canRollBack(journalId, nameServiceId,
            storage, prevStorage, targetLayoutVersion);
      }
    });
  }

  @Override
  public ListenableFuture<Void> doRollback() {
    return fifoExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().doRollback(journalId, nameServiceId);
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Void> discardSegments(final long startTxId) {
    return fifoExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().discardSegments(journalId, nameServiceId, startTxId);
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Long> getJournalCTime() {
    return fifoExecutor.submit(new Callable<Long>() {
      @Override
      public Long call() throws IOException {
        return getProxy().getJournalCTime(journalId, nameServiceId);
      }
    });
  }

  @Override
  public String toString() {
    return InetAddresses.toAddrString(addr.getAddress()) + ':' +
        addr.getPort();
  }

  @Override
  public synchronized void appendReport(StringBuilder sb) {
    sb.append("Written txid ").append(highestAckedTxId);
    long behind = getLagTxns();
    if (behind > 0) {
      if (lastAckNanos != 0) {
        long lagMillis = getLagTimeMillis();
        sb.append(" (" + behind + " txns/" + lagMillis + "ms behind)");
      } else {
        sb.append(" (never written");
      }
    }
    if (outOfSync) {
      sb.append(" (will try to re-sync on next segment)");
    }
  }
  
  public synchronized long getLagTxns() {
    return Math.max(committedTxId - highestAckedTxId, 0);
  }
  
  public synchronized long getLagTimeMillis() {
    return TimeUnit.MILLISECONDS.convert(
        Math.max(lastCommitNanos - lastAckNanos, 0),
        TimeUnit.NANOSECONDS);
  }

  private void constructHttpServerURI(GetEditLogManifestResponseProto ret) {
    if (ret.hasFromURL()) {
      URI uri = URI.create(ret.getFromURL());
      httpServerURL = getHttpServerURI(uri.getScheme(), uri.getPort());
    } else {
      httpServerURL = getHttpServerURI("http", ret.getHttpPort());;
    }
  }

  private void constructHttpServerURI(GetJournalStateResponseProto ret) {
    if (ret.hasFromURL()) {
      URI uri = URI.create(ret.getFromURL());
      httpServerURL = getHttpServerURI(uri.getScheme(), uri.getPort());
    } else {
      httpServerURL = getHttpServerURI("http", ret.getHttpPort());;
    }
  }

  /**
   * Construct the http server based on the response.
   *
   * The fromURL field in the response specifies the endpoint of the http
   * server. However, the address might not be accurate since the server can
   * bind to multiple interfaces. Here the client plugs in the address specified
   * in the configuration and generates the URI.
   */
  private URL getHttpServerURI(String scheme, int port) {
    try {
      return new URL(scheme, addr.getHostName(), port, "");
    } catch (MalformedURLException e) {
      // Unreachable
      throw new RuntimeException(e);
    }
  }

  private boolean hasHttpServerEndPoint() {
   return httpServerURL != null;
  }

}
