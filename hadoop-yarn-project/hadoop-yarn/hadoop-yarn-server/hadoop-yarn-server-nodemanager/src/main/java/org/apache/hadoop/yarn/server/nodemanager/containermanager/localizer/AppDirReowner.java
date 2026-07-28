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

import java.io.IOException;

import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes the local directories of an orphaned application deletable by the
 * application user.
 *
 * Content owned by {@code root} appears under
 * {@code usercache/<user>/appcache/<app-id>} from two directions, neither of
 * which the NodeManager controls:
 *
 * <ol>
 *   <li>dockerd re-creating a missing bind-mount source. The application
 *   directory is mounted read-write into every container, and when that source
 *   is gone at container start dockerd creates it with {@code mkdir(0755)}
 *   followed by {@code chown(0,0)}.</li>
 *   <li>a process running as root <em>inside</em> a container writing to a
 *   bind-mounted host directory, typically the container's {@code /tmp}. This
 *   produces root-owned content at arbitrary depth below a directory whose own
 *   ownership is correct.</li>
 * </ol>
 *
 * Deletion runs as the application user, which cannot unlink the children of a
 * directory it has no write bit on, so such a tree is undeletable and would be
 * re-selected by {@link OrphanedAppReaper} on every scan forever. Restoring
 * ownership needs root, hence an external privileged helper.
 */
interface AppDirReowner {

  /**
   * Restores ownership of the local directories of {@code appId} so that
   * {@code user} can delete them.
   *
   * Implementations must be idempotent: the reaper calls this for every
   * candidate it is about to clean up, whether or not root-owned content is
   * actually present.
   *
   * @param user the application user.
   * @param appId the orphaned application.
   * @return whether the directories can now be deleted as {@code user}. When
   *         this is {@code false} the caller must not enter the cleanup chain,
   *         because the deletion would fail silently.
   */
  boolean reown(String user, ApplicationId appId);

  /**
   * Used when no helper is configured. Reports success so that applications
   * whose directories are already owned correctly - the common case - are
   * still collected.
   */
  AppDirReowner NOOP = new AppDirReowner() {
    @Override
    public boolean reown(String user, ApplicationId appId) {
      return true;
    }

    @Override
    public String toString() {
      return "disabled";
    }
  };

  /**
   * Runs a privileged helper through {@code sudo}.
   *
   * The helper takes only a user name and an application id: no filesystem
   * path crosses the process boundary, and it derives the directories itself
   * from a root-owned configuration. See {@code yarn-reown-orphan-app-dir} and
   * the deployment section of {@code ORPHANED_APPCACHE_CONFLUENCE.md} for the
   * ownership and sudoers requirements, which are load-bearing.
   */
  final class SudoAppDirReowner implements AppDirReowner {

    private static final Logger LOG =
        LoggerFactory.getLogger(SudoAppDirReowner.class);

    /** Argument validation failure, as reported by the helper. */
    private static final int EXIT_USAGE = 64;
    /** The helper refused to read a configuration file it does not trust. */
    private static final int EXIT_UNTRUSTED_CONFIG = 65;
    /** The helper found no usable local directory to work on. */
    private static final int EXIT_NOTHING_PROCESSED = 67;

    private final String command;
    private final long timeoutMs;

    SudoAppDirReowner(String command, long timeoutMs) {
      this.command = command;
      this.timeoutMs = timeoutMs;
    }

    @Override
    public boolean reown(String user, ApplicationId appId) {
      // -n so that a missing sudoers rule fails immediately instead of
      // blocking on a password prompt the NodeManager can never answer.
      String[] argv = {"sudo", "-n", command, user, appId.toString()};
      Shell.ShellCommandExecutor shexec =
          new Shell.ShellCommandExecutor(argv, null, null, timeoutMs);
      try {
        shexec.execute();
        String output = oneLine(shexec.getOutput());
        if (!output.isEmpty()) {
          LOG.info("Re-owned the local directories of {}: {}", appId, output);
        }
        return true;
      } catch (IOException e) {
        int exitCode = shexec.getExitCode();
        // The helper reports what went wrong on stderr, which Shell surfaces
        // only as the message of the exception; getOutput() carries stdout,
        // which on a failing run is usually empty.
        String reason = oneLine(e.getMessage());
        if (shexec.isTimedOut()) {
          LOG.warn("Timed out after {} ms re-owning the local directories of"
              + " {} for user {}; skipping it. Command: {}",
              timeoutMs, appId, user, command, e);
        } else if (exitCode == EXIT_UNTRUSTED_CONFIG) {
          LOG.warn("Re-owning the local directories of {} failed: {} refused"
              + " to read its configuration because it is not root-owned or is"
              + " group/other-writable. Fix the configuration ownership rather"
              + " than weakening the check. Reason: {}", appId, command,
              reason, e);
        } else if (exitCode == EXIT_USAGE) {
          LOG.warn("Re-owning the local directories of {} failed: {} rejected"
              + " user '{}' or the application id. Reason: {}", appId, command,
              user, reason, e);
        } else if (exitCode == EXIT_NOTHING_PROCESSED) {
          LOG.warn("Re-owning the local directories of {} failed: {} found no"
              + " usable directory among the configured local dirs, so nothing"
              + " was re-owned. Check that yarn.nodemanager.local-dirs in the"
              + " root-owned yarn-site.xml the helper reads names the same"
              + " directories this NodeManager uses. Reason: {}", appId,
              command, reason, e);
        } else {
          LOG.warn("Re-owning the local directories of {} for user {} failed"
              + " with exit code {}. Verify the deployment with"
              + " {}-verify-setup. Reason: {}", appId, user, exitCode, command,
              reason, e);
        }
        return false;
      }
    }

    /**
     * Collapses the helper's per-directory report into a single line, so that
     * one application produces one log entry.
     */
    private static String oneLine(String text) {
      if (text == null) {
        return "";
      }
      return text.trim().replaceAll("\\s*\\R\\s*", "; ");
    }

    @Override
    public String toString() {
      return command + " (timeout " + timeoutMs + " ms)";
    }
  }
}
