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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl.FlowContext;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reaps local application directories that the NodeManager no longer tracks.
 *
 * An {@code appcache/<app-id>} directory is removed by exactly one event chain,
 * triggered by {@code applicationsToCleanup} in the ResourceManager heartbeat
 * response. That signal is fire-and-forget on both ends, and
 * {@link ResourceLocalizationService} dispatches
 * {@code APPLICATION_RESOURCES_CLEANEDUP} as soon as the deletion tasks are
 * submitted, without ever checking their result. The application therefore
 * reaches {@code FINISHED} and is dropped from
 * {@link Context#getApplications()} whether or not the files were actually
 * removed - after which no NodeManager code path can reach those directories,
 * and because
 * {@link YarnConfiguration#NM_LOCALIZER_CACHE_TARGET_SIZE_MB} excludes
 * resources with APPLICATION visibility, they are never size-capped either.
 *
 * This class closes that gap without duplicating any deletion logic: it
 * registers a synthetic {@link ApplicationImpl} for each orphaned application
 * id and pushes it straight to the cleanup state with
 * {@link ApplicationEventType#CLEANUP_ORPHANED_APPLICATION}, so resource
 * destruction, aux-service notification, token cleanup and self-removal all run
 * through the code that already executes on every normal application finish.
 *
 * Nothing is persisted along that path, so the reap is idempotent and
 * crash-safe: a crash mid-sequence leaves no state behind and the next scan
 * simply finds the directory again.
 *
 * Resources under a {@code filecache} directory are never selected. Those are
 * replayed from the state store at recovery <em>without</em> checking that the
 * file still exists, so removing one behind the NodeManager's back would leave
 * the tracker handing containers a path to a missing file, across restarts.
 */
class OrphanedAppReaper implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(OrphanedAppReaper.class);

  private static final String APP_ID_PREFIX = "application_";

  private final Context nmContext;
  private final Dispatcher dispatcher;
  private final LocalDirsHandlerService dirsHandler;
  private final FileContext lfs;
  private final long minAgeMs;
  private final int maxPerInterval;
  private final AppDirReowner appDirReowner;

  OrphanedAppReaper(Context nmContext, Dispatcher dispatcher,
      LocalDirsHandlerService dirsHandler, FileContext lfs, long minAgeMs,
      int maxPerInterval, AppDirReowner appDirReowner) {
    this.nmContext = nmContext;
    this.dispatcher = dispatcher;
    this.dirsHandler = dirsHandler;
    this.lfs = lfs;
    this.minAgeMs = minAgeMs;
    this.maxPerInterval = maxPerInterval;
    this.appDirReowner = appDirReowner;
  }

  @Override
  public void run() {
    try {
      reap();
    } catch (Throwable t) {
      // A scheduled task that throws is never rescheduled, which would
      // silently disable the reaper for the lifetime of the NodeManager.
      LOG.error("Orphaned application directory scan failed", t);
    }
  }

  /**
   * Scans the local directories and pushes every orphaned application through
   * the application-finish cleanup chain.
   *
   * @return the number of applications for which cleanup was dispatched.
   */
  @VisibleForTesting
  int reap() {
    long now = System.currentTimeMillis();

    // Read straight from the live map: an application is orphaned only if the
    // NodeManager no longer tracks it. This never depends on the filesystem,
    // so a failing disk cannot be mistaken for "no live applications".
    //
    // This map is the authority on whether the local directories are still
    // needed, including for an application that has no container left on this
    // node but is still serving shuffle data to other nodes. An application
    // stays in it with zero containers: ApplicationImpl transitions RUNNING to
    // RUNNING on APPLICATION_CONTAINER_FINISHED, and the only arc out of
    // RUNNING is FINISH_APPLICATION, which the NodeManager raises solely from
    // the applicationsToCleanup list of a ResourceManager heartbeat response,
    // that is when the application itself completes. The entry is removed by
    // AppLogsAggregatedTransition alone, at the very end of that same chain,
    // whose entry point - handleAppFinishWithContainersCleanedup - is also
    // what tells the auxiliary services to stop the application. The set read
    // here is therefore exactly the set of applications whose shuffle data the
    // NodeManager still considers live, and this reaper cannot be more
    // aggressive than the NodeManager already is. Anything that adds an arc
    // out of RUNNING has to revisit that reasoning.
    Set<ApplicationId> live = nmContext.getApplications().keySet();

    // Keyed and ordered by application id, so that the oldest leaks are
    // reaped first when the per-interval cap kicks in.
    Map<ApplicationId, Candidate> observed = new TreeMap<>();
    List<String> unparsed = new ArrayList<>();
    for (String localDir : dirsHandler.getLocalDirs()) {
      scanUserCache(localDir, observed, unparsed);
      scanNmPrivate(localDir, observed, unparsed);
    }

    int skippedLive = 0;
    int skippedYoung = 0;
    List<Candidate> selected = new ArrayList<>();
    for (Candidate candidate : observed.values()) {
      if (live.contains(candidate.appId)) {
        skippedLive++;
        LOG.debug("Skipping {}: still tracked by the NodeManager",
            candidate.appId);
      } else if (now - candidate.newestMtime < minAgeMs) {
        skippedYoung++;
        LOG.info("Skipping orphaned application {}: local directories were"
            + " modified {} ms ago, less than the configured minimum age"
            + " of {} ms", candidate.appId, now - candidate.newestMtime,
            minAgeMs);
      } else {
        selected.add(candidate);
      }
    }

    int capped = 0;
    if (selected.size() > maxPerInterval) {
      capped = selected.size() - maxPerInterval;
      selected = selected.subList(0, maxPerInterval);
    }

    int dispatched = 0;
    int skippedNoUser = 0;
    int skippedReown = 0;
    for (Candidate candidate : selected) {
      String user = resolveUser(candidate);
      if (user == null) {
        skippedNoUser++;
        continue;
      }

      // Must precede registration: root-owned content in the tree makes it
      // undeletable by the application user, and the cleanup chain reports
      // success regardless, so the application would reach FINISHED with its
      // files still on disk and the next scan would select it again.
      if (!appDirReowner.reown(user, candidate.appId)) {
        skippedReown++;
        NodeManagerMetrics metrics = nmContext.getNodeManagerMetrics();
        if (metrics != null) {
          metrics.orphanedAppDirsReownFailure();
        }
        continue;
      }

      if (cleanUpOrphanedApp(candidate, user)) {
        dispatched++;
      } else {
        skippedLive++;
      }
    }

    LOG.info("Orphaned application directory scan: {} application directories"
        + " found, {} still tracked, {} too young, {} deferred to a later scan"
        + " by the per-interval cap of {}, {} with no resolvable user, {} whose"
        + " directories could not be made deletable, cleanup dispatched for {}",
        observed.size(), skippedLive, skippedYoung, capped, maxPerInterval,
        skippedNoUser, skippedReown, dispatched);
    if (!unparsed.isEmpty()) {
      LOG.info("Ignored {} local directories whose name is not an application"
          + " id: {}", unparsed.size(), unparsed);
    }
    return dispatched;
  }

  /**
   * The user the directories of {@code candidate} belong to.
   *
   * @return the user derived from the usercache path, the NodeManager user when
   *         only an nmPrivate directory was found, or {@code null} when neither
   *         could be determined.
   */
  private String resolveUser(Candidate candidate) {
    if (candidate.user != null) {
      return candidate.user;
    }
    // Only an nmPrivate directory was found for this application, so no user
    // could be derived from a usercache path. That directory is owned by the
    // NodeManager and is deleted as the NodeManager user regardless, and the
    // usercache path built from the fallback cannot exist for an application
    // that is already gone.
    String user = nodeManagerUser();
    if (user == null) {
      LOG.warn("Skipping orphaned application {}: no usercache directory was"
          + " found for it and the NodeManager user could not be determined",
          candidate.appId);
      return null;
    }
    LOG.info("No usercache directory found for orphaned application {},"
        + " cleaning up its nmPrivate directory as {}", candidate.appId, user);
    return user;
  }

  /**
   * Registers a synthetic application and dispatches its cleanup.
   *
   * @return whether cleanup was dispatched.
   */
  @SuppressWarnings("unchecked")
  private boolean cleanUpOrphanedApp(Candidate candidate, String user) {
    ApplicationId appId = candidate.appId;

    ApplicationImpl app = new ApplicationImpl(dispatcher, user,
        defaultFlowContext(appId), appId, new Credentials(), nmContext);

    // Must precede the dispatch: ContainerManagerImpl's application event
    // dispatcher resolves the application through getApplications(), and
    // warns and drops the event when it is absent. putIfAbsent so that a live
    // application registered since the scan is never clobbered.
    Application existing =
        nmContext.getApplications().putIfAbsent(appId, app);
    if (existing != null) {
      LOG.info("Skipping orphaned application {}: it was registered with the"
          + " NodeManager during the scan", appId);
      return false;
    }

    // Balances the unconditional endRunningApplication() performed when the
    // application removes itself from the map at the end of the chain.
    NodeManagerMetrics metrics = nmContext.getNodeManagerMetrics();
    if (metrics != null) {
      metrics.runningApplication();
    }

    LOG.info("Dispatching cleanup of orphaned application {} for user {}",
        appId, user);
    dispatcher.getEventHandler().handle(new ApplicationEvent(appId,
        ApplicationEventType.CLEANUP_ORPHANED_APPLICATION));
    return true;
  }

  private void scanUserCache(String localDir,
      Map<ApplicationId, Candidate> observed, List<String> unparsed) {
    Path userCache = new Path(localDir, ContainerLocalizer.USERCACHE);
    for (FileStatus userStatus : listDirs(userCache)) {
      String user = userStatus.getPath().getName();
      if (ContainerLocalizer.FILECACHE.equals(user)) {
        // Never descend into a filecache directory.
        continue;
      }
      Path appCache =
          new Path(userStatus.getPath(), ContainerLocalizer.APPCACHE);
      for (FileStatus appStatus : listDirs(appCache)) {
        observe(observed, unparsed, appStatus, user);
      }
    }
  }

  private void scanNmPrivate(String localDir,
      Map<ApplicationId, Candidate> observed, List<String> unparsed) {
    Path nmPrivate =
        new Path(localDir, ResourceLocalizationService.NM_PRIVATE_DIR);
    for (FileStatus appStatus : listDirs(nmPrivate)) {
      observe(observed, unparsed, appStatus, null);
    }
  }

  private void observe(Map<ApplicationId, Candidate> observed,
      List<String> unparsed, FileStatus appStatus, String user) {
    String name = appStatus.getPath().getName();
    ApplicationId appId = parseAppId(name);
    if (appId == null) {
      unparsed.add(appStatus.getPath().toString());
      return;
    }
    Candidate candidate = observed.get(appId);
    if (candidate == null) {
      candidate = new Candidate(appId);
      observed.put(appId, candidate);
    }
    candidate.observe(user, appStatus.getModificationTime());
  }

  /**
   * Lists the sub-directories of {@code dir}.
   *
   * Fails closed: a directory that cannot be listed yields no candidates and a
   * warning, rather than an empty or partial view being acted upon.
   */
  private List<FileStatus> listDirs(Path dir) {
    List<FileStatus> dirs = new ArrayList<>();
    try {
      RemoteIterator<FileStatus> it = lfs.listStatus(dir);
      while (it != null && it.hasNext()) {
        FileStatus status = it.next();
        if (status.isDirectory()) {
          dirs.add(status);
        }
      }
    } catch (FileNotFoundException e) {
      // Normal: the layout below a local dir is created on demand.
      LOG.debug("No directory at {}", dir);
      return Collections.emptyList();
    } catch (UnsupportedFileSystemException e) {
      LOG.warn("Local dir " + dir + " is an unsupported filesystem", e);
      return Collections.emptyList();
    } catch (IOException e) {
      LOG.warn("Unable to list " + dir + ", skipping it for this scan", e);
      return Collections.emptyList();
    }
    return dirs;
  }

  private static ApplicationId parseAppId(String name) {
    if (!name.startsWith(APP_ID_PREFIX)) {
      return null;
    }
    try {
      return ApplicationId.fromString(name);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * The {@link ApplicationImpl} constructor rejects a null flow context when
   * timeline service v2 is enabled, so supply the same default that
   * application recovery uses.
   */
  private static FlowContext defaultFlowContext(ApplicationId appId) {
    return new FlowContext(TimelineUtils.generateDefaultFlowName(null, appId),
        YarnConfiguration.DEFAULT_FLOW_VERSION, appId.getClusterTimestamp());
  }

  private static String nodeManagerUser() {
    try {
      return UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.warn("Unable to determine the NodeManager user", e);
      return null;
    }
  }

  /**
   * An application id found on disk, together with the user derived from its
   * usercache path and the most recent modification time observed across all
   * of its directories.
   *
   * That modification time is the one of the directory named after the
   * application id, and it is deliberately not taken any deeper. A directory's
   * mtime moves when a direct child is created or removed, and container work
   * directories are exactly the direct children of
   * {@code appcache/<app-id>}, so this timestamp is when the last container
   * work directory on this node was created or deleted - the closest the local
   * filesystem gets to "when the NodeManager last used this tree". Once the
   * last container is gone nothing writes below it either: serving shuffle
   * data to other nodes is read-only, and reads move no modification time. So
   * a deeper walk would cost a listing per candidate and return this same
   * value.
   */
  private static final class Candidate {
    private final ApplicationId appId;
    private String user;
    private long newestMtime = Long.MIN_VALUE;

    private Candidate(ApplicationId appId) {
      this.appId = appId;
    }

    private void observe(String observedUser, long mtime) {
      if (observedUser != null) {
        this.user = observedUser;
      }
      // Newest wins, so an application whose directories were touched
      // recently under any local dir is held back by the minimum-age guard.
      this.newestMtime = Math.max(this.newestMtime, mtime);
    }
  }
}
