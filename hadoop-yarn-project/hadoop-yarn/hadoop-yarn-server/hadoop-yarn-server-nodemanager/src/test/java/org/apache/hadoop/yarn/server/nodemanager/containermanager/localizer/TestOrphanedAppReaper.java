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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEventType;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the selection rules of {@link OrphanedAppReaper}: which application
 * directories found on disk are handed to the application-finish cleanup
 * chain, and which are left alone.
 */
public class TestOrphanedAppReaper {

  private static final long TIMESTAMP = 314159265358979L;

  /** Anything modified longer ago than this may be selected. */
  private static final long MIN_AGE_MS = 10 * 1000;

  /** Comfortably older than MIN_AGE_MS. */
  private static final long OLD_MS = 10 * 60 * 1000;

  private static final int NO_CAP = 100;

  private final File basedir =
      new File("target", TestOrphanedAppReaper.class.getName());

  private Configuration conf;
  private FileContext lfs;
  private Context context;
  private ConcurrentMap<ApplicationId, Application> applications;
  private LocalDirsHandlerService dirsHandler;
  private List<ApplicationEvent> dispatched;
  private Dispatcher dispatcher;
  private NodeManagerMetrics metrics;
  private RecordingReowner reowner;

  @BeforeEach
  public void setup() throws IOException {
    FileUtils.deleteDirectory(basedir);
    assertTrue(basedir.mkdirs());

    conf = new Configuration();
    lfs = FileContext.getLocalFSFileContext(conf);

    applications = new ConcurrentHashMap<>();
    context = mock(Context.class);
    when(context.getApplications()).thenReturn(applications);
    when(context.getConf()).thenReturn(conf);
    when(context.getApplicationACLsManager())
        .thenReturn(new ApplicationACLsManager(conf));
    when(context.getNMStateStore()).thenReturn(new NMNullStateStoreService());
    metrics = mock(NodeManagerMetrics.class);
    when(context.getNodeManagerMetrics()).thenReturn(metrics);

    dirsHandler = mock(LocalDirsHandlerService.class);

    // Stands in for the privileged helper, which cannot run from a unit test.
    reowner = new RecordingReowner();

    // Records what the reaper puts on the bus instead of running the chain, so
    // that selection can be asserted on its own.
    dispatched = new ArrayList<>();
    dispatcher = mock(Dispatcher.class);
    when(dispatcher.getEventHandler()).thenReturn(new EventHandler<Event>() {
      @Override
      public void handle(Event event) {
        dispatched.add((ApplicationEvent) event);
      }
    });
  }

  @AfterEach
  public void cleanup() throws IOException {
    FileUtils.deleteDirectory(basedir);
  }

  @Test
  public void testUntrackedAppDirIsSelected() throws Exception {
    File localDir = localDirs(1).get(0);
    appCacheDir(localDir, "alice", appId(1).toString(), OLD_MS);
    nmPrivateDir(localDir, appId(1).toString(), OLD_MS);

    assertEquals(1, reaper(NO_CAP).reap());
    assertEquals(Arrays.asList(appId(1)), cleanedUp());

    // Registered before the dispatch, otherwise the event would be routed to
    // an absent application and dropped.
    Application app = applications.get(appId(1));
    assertNotNull(app, "Application was not registered with the context");
    assertEquals("alice", app.getUser());
  }

  @Test
  public void testTrackedAppIsSkipped() throws Exception {
    File localDir = localDirs(1).get(0);
    appCacheDir(localDir, "alice", appId(1).toString(), OLD_MS);

    Application live = mock(Application.class);
    applications.put(appId(1), live);

    assertEquals(0, reaper(NO_CAP).reap());
    assertTrue(dispatched.isEmpty());
    // putIfAbsent never clobbers a live application.
    assertEquals(live, applications.get(appId(1)));
  }

  @Test
  public void testRecentlyModifiedAppDirIsSkipped() throws Exception {
    File localDir = localDirs(1).get(0);
    appCacheDir(localDir, "alice", appId(1).toString(), 0);

    assertEquals(0, reaper(NO_CAP).reap());
    assertTrue(dispatched.isEmpty());
    assertTrue(applications.isEmpty());
  }

  /**
   * An application whose directories are old under one local dir but fresh
   * under another is held back: the newest modification time wins.
   */
  @Test
  public void testNewestModificationTimeWins() throws Exception {
    List<File> dirs = localDirs(2);
    appCacheDir(dirs.get(0), "alice", appId(1).toString(), OLD_MS);
    appCacheDir(dirs.get(1), "alice", appId(1).toString(), 0);

    assertEquals(0, reaper(NO_CAP).reap());
    assertTrue(dispatched.isEmpty());
  }

  /**
   * Nothing below a filecache directory may ever be selected. Those resources
   * are replayed from the state store at recovery without checking that the
   * file still exists, so removing one behind the NodeManager's back would
   * leave the tracker handing containers a path to a missing file.
   */
  @Test
  public void testFilecacheIsNeverSelected() throws Exception {
    File localDir = localDirs(1).get(0);

    // A directory named like the public cache sitting where a user directory
    // is expected must not be treated as a user.
    File asUser = new File(localDir, ContainerLocalizer.USERCACHE + "/"
        + ContainerLocalizer.FILECACHE + "/" + appId(1));
    assertTrue(asUser.mkdirs());
    touch(asUser, OLD_MS);

    // The per-user private cache is not below appcache, so it is not scanned.
    File privateCache = new File(localDir, ContainerLocalizer.USERCACHE
        + "/alice/" + ContainerLocalizer.FILECACHE + "/" + appId(2));
    assertTrue(privateCache.mkdirs());
    touch(privateCache, OLD_MS);

    // The public cache at the top of the local dir is not scanned either.
    File publicCache =
        new File(localDir, ContainerLocalizer.FILECACHE + "/" + appId(3));
    assertTrue(publicCache.mkdirs());
    touch(publicCache, OLD_MS);

    appCacheDir(localDir, "alice", appId(4).toString(), OLD_MS);

    assertEquals(1, reaper(NO_CAP).reap());
    assertEquals(Arrays.asList(appId(4)), cleanedUp());
  }

  @Test
  public void testUnparseableDirectoryNamesAreSkipped() throws Exception {
    File localDir = localDirs(1).get(0);
    for (String name : Arrays.asList("garbage", "application", "application_",
        "application_notanumber_0001", "application_" + TIMESTAMP,
        "application_" + TIMESTAMP + "_0001_extra", "usercache")) {
      appCacheDir(localDir, "alice", name, OLD_MS);
      nmPrivateDir(localDir, name, OLD_MS);
    }

    assertEquals(0, reaper(NO_CAP).reap());
    assertTrue(dispatched.isEmpty());
    assertTrue(applications.isEmpty());
  }

  @Test
  public void testMaxPerIntervalIsRespected() throws Exception {
    File localDir = localDirs(1).get(0);
    for (int id = 1; id <= 5; id++) {
      appCacheDir(localDir, "alice", appId(id).toString(), OLD_MS);
    }

    assertEquals(2, reaper(2).reap());
    // Ordered by application id, so the oldest leaks go first and the rest are
    // picked up by later scans.
    assertEquals(Arrays.asList(appId(1), appId(2)), cleanedUp());
    assertEquals(2, applications.size());
  }

  @Test
  public void testUserIsDerivedFromUsercachePath() throws Exception {
    File localDir = localDirs(1).get(0);
    appCacheDir(localDir, "alice", appId(1).toString(), OLD_MS);
    appCacheDir(localDir, "bob", appId(2).toString(), OLD_MS);

    assertEquals(2, reaper(NO_CAP).reap());
    assertEquals("alice", applications.get(appId(1)).getUser());
    assertEquals("bob", applications.get(appId(2)).getUser());
  }

  /**
   * With no usercache directory left there is no user to derive, so the
   * NodeManager user is used. nmPrivate is owned by the NodeManager and is
   * removed as that user anyway.
   */
  @Test
  public void testNmPrivateOnlyUsesNodeManagerUser() throws Exception {
    File localDir = localDirs(1).get(0);
    nmPrivateDir(localDir, appId(1).toString(), OLD_MS);

    assertEquals(1, reaper(NO_CAP).reap());
    assertEquals(UserGroupInformation.getCurrentUser().getShortUserName(),
        applications.get(appId(1)).getUser());
  }

  /**
   * A local dir that cannot be listed yields no candidates rather than an
   * empty view being acted upon, and does not stop the scan of the others.
   */
  @Test
  public void testUnreadableDirFailsClosed() throws Exception {
    List<File> dirs = localDirs(2);
    appCacheDir(dirs.get(0), "alice", appId(1).toString(), OLD_MS);
    appCacheDir(dirs.get(1), "bob", appId(2).toString(), OLD_MS);

    FileContext failing = mock(FileContext.class);
    Path unreadable = new Path(dirs.get(0).getAbsolutePath(),
        ContainerLocalizer.USERCACHE);
    when(failing.listStatus(any(Path.class))).thenAnswer(invocation -> {
      Path path = invocation.getArgument(0);
      if (path.equals(unreadable)) {
        throw new IOException("Permission denied: " + path);
      }
      return lfs.listStatus(path);
    });

    OrphanedAppReaper reaper = new OrphanedAppReaper(context, dispatcher,
        dirsHandler, failing, MIN_AGE_MS, NO_CAP, reowner);

    assertEquals(1, reaper.reap());
    assertEquals(Arrays.asList(appId(2)), cleanedUp());
    assertNull(applications.get(appId(1)),
        "A dir that could not be listed produced a candidate");
  }

  /**
   * Ownership is restored for every selected candidate, not only for those whose
   * application directory looks root-owned. Root-owned content also appears at
   * arbitrary depth - a container process running as root writing into a
   * bind-mounted host directory - below a correctly owned application directory,
   * so there is nothing cheap to test on the application directory itself.
   */
  @Test
  public void testReownIsInvokedForEverySelectedCandidate() throws Exception {
    List<File> dirs = localDirs(2);
    appCacheDir(dirs.get(0), "alice", appId(1).toString(), OLD_MS);
    appCacheDir(dirs.get(1), "bob", appId(2).toString(), OLD_MS);

    assertEquals(2, reaper(NO_CAP).reap());
    assertEquals(Arrays.asList("alice " + appId(1), "bob " + appId(2)),
        reowner.calls);
  }

  /**
   * Candidates that are filtered out must not reach the helper: each call is a
   * sudo fork and a walk of the tree.
   */
  @Test
  public void testReownIsNotInvokedForFilteredCandidates() throws Exception {
    File localDir = localDirs(1).get(0);
    appCacheDir(localDir, "alice", appId(1).toString(), OLD_MS);
    appCacheDir(localDir, "bob", appId(2).toString(), 0);
    applications.put(appId(1), mock(Application.class));

    assertEquals(0, reaper(NO_CAP).reap());
    assertTrue(reowner.calls.isEmpty(),
        "A live or too-young application was handed to the helper");
  }

  /**
   * When ownership cannot be restored the application must not enter the
   * cleanup chain at all. The chain reports success without checking that the
   * files were removed, so registering the application would advance it to
   * FINISHED with its directories still on disk, and the next scan would find
   * them again - the wasted cycle this exists to prevent.
   */
  @Test
  public void testReownFailureSkipsCleanupEntirely() throws Exception {
    File localDir = localDirs(1).get(0);
    appCacheDir(localDir, "alice", appId(1).toString(), OLD_MS);
    reowner.succeed = false;

    assertEquals(0, reaper(NO_CAP).reap());
    assertEquals(Arrays.asList("alice " + appId(1)), reowner.calls);
    assertTrue(dispatched.isEmpty(),
        "Cleanup was dispatched for a directory that cannot be deleted");
    assertTrue(applications.isEmpty(),
        "A synthetic application was left in the applications map");
    verify(metrics).orphanedAppDirsReownFailure();
  }

  /**
   * A failure for one application must not hold back the others.
   */
  @Test
  public void testReownFailureDoesNotBlockOtherApplications()
      throws Exception {
    File localDir = localDirs(1).get(0);
    appCacheDir(localDir, "alice", appId(1).toString(), OLD_MS);
    appCacheDir(localDir, "bob", appId(2).toString(), OLD_MS);

    reowner = new RecordingReowner() {
      @Override
      public boolean reown(String user, ApplicationId appId) {
        super.reown(user, appId);
        return !"alice".equals(user);
      }
    };

    assertEquals(1, reaper(NO_CAP).reap());
    assertEquals(Arrays.asList(appId(2)), cleanedUp());
  }

  /**
   * For an application with only an nmPrivate directory the helper is asked to
   * re-own as the NodeManager user, because that directory is deleted in-process
   * as the NodeManager user rather than through the application user.
   */
  @Test
  public void testReownUsesNodeManagerUserForNmPrivateOnly() throws Exception {
    File localDir = localDirs(1).get(0);
    nmPrivateDir(localDir, appId(1).toString(), OLD_MS);

    assertEquals(1, reaper(NO_CAP).reap());
    assertEquals(Arrays.asList(
        UserGroupInformation.getCurrentUser().getShortUserName() + " "
            + appId(1)),
        reowner.calls);
  }

  /**
   * A scheduled task that throws is never rescheduled, which would disable the
   * scan for the lifetime of the NodeManager.
   */
  @Test
  public void testRunSwallowsFailures() {
    when(dirsHandler.getLocalDirs())
        .thenThrow(new RuntimeException("disk check exploded"));
    reaper(NO_CAP).run();
    assertTrue(dispatched.isEmpty());
  }

  /**
   * Runs a full cycle over a dispatcher wired like the NodeManager's, to check
   * that the applicationsRunning gauge returns to where it started. The reaper
   * increments it because the last transition of the chain decrements it
   * unconditionally when the application removes itself from the map.
   */
  @Test
  public void testMetricsBalancedAcrossFullCycle() throws Exception {
    File localDir = localDirs(1).get(0);
    appCacheDir(localDir, "alice", appId(1).toString(), OLD_MS);

    NodeManagerMetrics metrics = NodeManagerMetrics.create();
    int before = metrics.getRunningApplications();
    when(context.getNodeManagerMetrics()).thenReturn(metrics);
    when(context.getNMTokenSecretManager())
        .thenReturn(mock(NMTokenSecretManagerInNM.class));

    DrainDispatcher realDispatcher = new DrainDispatcher();
    realDispatcher.init(conf);
    wireChain(realDispatcher);
    realDispatcher.start();
    try {
      OrphanedAppReaper reaper = new OrphanedAppReaper(context,
          realDispatcher, dirsHandler, lfs, MIN_AGE_MS, NO_CAP, reowner);
      assertEquals(1, reaper.reap());
      realDispatcher.await();

      assertTrue(applications.isEmpty(),
          "Application was not removed from the applications map");
      assertEquals(before, metrics.getRunningApplications());
    } finally {
      realDispatcher.stop();
    }
  }

  /**
   * Registers the handlers the cleanup chain relies on, reproducing what the
   * NodeManager does for an application it does not know:
   * ResourceLocalizationService replies APPLICATION_RESOURCES_CLEANEDUP as soon
   * as the deletion tasks are submitted, and both log handlers reply
   * APPLICATION_LOG_HANDLING_FAILED because they never saw the application.
   */
  @SuppressWarnings("unchecked")
  private void wireChain(final DrainDispatcher bus) {
    bus.register(ApplicationEventType.class,
        new EventHandler<ApplicationEvent>() {
          @Override
          public void handle(ApplicationEvent event) {
            Application app = applications.get(event.getApplicationID());
            if (app != null) {
              app.handle(event);
            }
          }
        });
    bus.register(LocalizationEventType.class,
        new EventHandler<LocalizationEvent>() {
          @Override
          public void handle(LocalizationEvent event) {
            if (LocalizationEventType.DESTROY_APPLICATION_RESOURCES
                .equals(event.getType())) {
              ApplicationId id = ((ApplicationLocalizationEvent) event)
                  .getApplication().getAppId();
              bus.getEventHandler().handle(new ApplicationEvent(id,
                  ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
            }
          }
        });
    bus.register(AuxServicesEventType.class,
        new EventHandler<AuxServicesEvent>() {
          @Override
          public void handle(AuxServicesEvent event) {
            // AuxServices swallows anything a service throws, so the chain
            // cannot be broken here.
          }
        });
    bus.register(LogHandlerEventType.class,
        new EventHandler<LogHandlerEvent>() {
          @Override
          public void handle(LogHandlerEvent event) {
            if (LogHandlerEventType.APPLICATION_FINISHED
                .equals(event.getType())) {
              ApplicationId id =
                  ((LogHandlerAppFinishedEvent) event).getApplicationId();
              bus.getEventHandler().handle(new ApplicationEvent(id,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED));
            }
          }
        });
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  private static ApplicationId appId(int id) {
    return ApplicationId.newInstance(TIMESTAMP, id);
  }

  /** Declares {@code n} local dirs under basedir and returns them. */
  private List<File> localDirs(int n) {
    List<File> dirs = new ArrayList<>();
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      File dir = new File(basedir, "local-" + i);
      assertTrue(dir.mkdirs());
      dirs.add(dir);
      paths.add(dir.getAbsolutePath());
    }
    when(dirsHandler.getLocalDirs()).thenReturn(paths);
    return dirs;
  }

  /**
   * Creates {@code <localDir>/usercache/<user>/appcache/<name>} with one
   * container work dir and one APPLICATION-visibility resource inside it.
   *
   * @param ageMs how long ago the application directory was last modified.
   */
  private File appCacheDir(File localDir, String user, String name,
      long ageMs) throws IOException {
    File appDir = new File(localDir, ContainerLocalizer.USERCACHE
        + "/" + user + "/" + ContainerLocalizer.APPCACHE + "/" + name);
    assertTrue(new File(appDir, "container_1").mkdirs());
    assertTrue(new File(appDir, ContainerLocalizer.FILECACHE).mkdirs());
    return touch(appDir, ageMs);
  }

  /** Creates {@code <localDir>/nmPrivate/<name>}. */
  private File nmPrivateDir(File localDir, String name, long ageMs) {
    File appDir = new File(localDir,
        ResourceLocalizationService.NM_PRIVATE_DIR + "/" + name);
    assertTrue(appDir.mkdirs());
    return touch(appDir, ageMs);
  }

  /**
   * Backdates a directory. Must be called after its children exist, since
   * creating a child updates the parent's modification time.
   */
  private File touch(File dir, long ageMs) {
    assertTrue(dir.setLastModified(System.currentTimeMillis() - ageMs),
        "Unable to backdate " + dir);
    return dir;
  }

  private OrphanedAppReaper reaper(int maxPerInterval) {
    return new OrphanedAppReaper(context, dispatcher, dirsHandler, lfs,
        MIN_AGE_MS, maxPerInterval, reowner);
  }

  /**
   * Stands in for the privileged re-own helper. Records every request so the
   * order relative to selection can be asserted, and can be made to fail.
   */
  private static class RecordingReowner implements AppDirReowner {

    private final List<String> calls = new ArrayList<>();
    private boolean succeed = true;

    @Override
    public boolean reown(String user, ApplicationId appId) {
      calls.add(user + " " + appId);
      return succeed;
    }
  }

  /** The application ids the reaper asked to be cleaned up, in order. */
  private List<ApplicationId> cleanedUp() {
    List<ApplicationId> ids = new ArrayList<>();
    for (ApplicationEvent event : dispatched) {
      assertEquals(ApplicationEventType.CLEANUP_ORPHANED_APPLICATION,
          event.getType());
      ids.add(event.getApplicationID());
    }
    return ids;
  }
}
