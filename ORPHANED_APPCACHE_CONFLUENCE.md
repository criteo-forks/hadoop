# NodeManager: collecting orphaned `appcache` directories

## 1. Summary

On a NodeManager, `usercache/<user>/appcache/application_*` directories can survive the
application they belong to. Nothing in the NodeManager ever looks at them again: the single
event chain that deletes them is fire-and-forget, and the localized-resource cache size limit
explicitly excludes them. Disk usage therefore grows until an operator intervenes.

This change lets the NodeManager collect those directories itself, in-process and incrementally,
by **re-entering its own application-finish chain**. A synthetic `ApplicationImpl` is registered
for an orphaned application id and pushed straight to the cleanup state, so resource destruction,
aux-service notification, token cleanup and self-removal all run through code that already
executes on every normal application finish. No deletion logic is duplicated and no new state is
persisted.

Collecting them is only half the problem. Many of those directories contain content owned by
`root`, created either by the Docker daemon or by container processes running as root, and deletion
runs as the application user — so the files cannot be removed and the same directories are selected
on every scan forever. This change therefore also invokes a small privileged helper, through `sudo`,
that restores ownership of `root`-owned entries before the application enters the cleanup chain. If
that cannot be done the application is skipped and counted, rather than pushed through a chain that
would report success while leaving the files on disk.

Both parts are **off by default**. The scan is enabled per cluster after canary validation; the
helper needs a deployment step of its own (section 8) whose file ownership and `sudo` rule are
security requirements, not conventions.

---

## 2. Symptom

What operators see:

* `usercache` under `yarn.nodemanager.local-dirs` keeps growing on long-lived NodeManagers.
* The applications owning those directories are **absent from the NodeManager UI** and from
  `/ws/v1/node/apps` — the NodeManager does not know about them any more.
* **Restarting the NodeManager does not help.** With recovery enabled the local dirs are left
  alone on purpose, so the directories survive the restart untouched.
* **A decommission cycle does help** — which is why the leak is usually "fixed" by draining the
  node. That works for the wrong reason and costs the whole localized-resource cache (see
  section 4).

---

## 3. Root cause

### What leaks, and what must not be touched

```
<yarn.nodemanager.local-dirs>/
├── filecache/                     PUBLIC  resources  ─┐  LRU-evicted by CacheCleanup
├── usercache/<user>/                                  │  every cleanup.interval-ms once
│   ├── filecache/                 PRIVATE resources  ─┘  over target-size-mb
│   └── appcache/
│       └── application_1_0001/    <<< LEAKS >>>  never size-capped, never LRU-evicted
│           ├── filecache/               APPLICATION-visibility resources
│           └── container_.../           container work dirs
└── nmPrivate/
    └── application_1_0001/        <<< LEAKS with it >>>
```

`yarn.nodemanager.localizer.cache.target-size-mb` explicitly **excludes** APPLICATION-visibility
resources, which is why `appcache` never shrinks on its own. The only thing that removes an
`appcache/<app_id>` directory is the application-finish chain — and that chain is fire-and-forget
at both ends.

### Where the chain breaks today

```
 RM                                     NM
  │
  │ heartbeat response
  │   applicationsToCleanup = [app]        RMNodeImpl
  │   list cleared immediately  ── fire & forget, never resent
  ▼
NodeStatusUpdaterImpl
  └─ CMgrCompletedAppsEvent
        ▼
ContainerManagerImpl
  ├─ app not in getApplications() ─────► DROPPED  "couldn't find application ..."
  ├─ a container isRecovering()   ─────► DROPPED  "drop FINISH_APPS event ..."
  └─ ApplicationFinishEvent
        ▼
ApplicationImpl : FINISH_APPLICATION
  └─ handleAppFinishWithContainersCleanedup()
       ├─ DESTROY_APPLICATION_RESOURCES ──► rm appcache/<app> + nmPrivate/<app>
       └─ AuxServicesEvent(APPLICATION_STOP)
        ▼
APPLICATION_RESOURCES_CLEANEDUP   dispatched unconditionally      RLS
        ▼                          (deletion result never checked)
     FINISHED ──► log handling ──► removed from getApplications()
                                          │
                                          └─► app forgotten. Any directory still on
                                              disk is now unreachable by every NM
                                              code path except the full wipe.
```

Three independent properties combine into the leak:

1. **The ResourceManager side is fire-and-forget.** `RMNodeImpl` clears its `finishedApplications`
   list as soon as it has been put into the heartbeat response. The signal is never resent.
2. **The NodeManager side silently drops it.** `ContainerManagerImpl` ignores the request if the
   application is not in `context.getApplications()`, or if any container is still recovering.
3. **The deletion result is never checked.** `ResourceLocalizationService`
   `handleDestroyApplicationResources` dispatches `APPLICATION_RESOURCES_CLEANEDUP`
   unconditionally, right after *submitting* the asynchronous deletion tasks. The application
   therefore advances to `FINISHED` and is removed from `context.getApplications()` whether or not
   the files were actually deleted.

Once the application is out of that map, **no NodeManager code path can reach those directories.**
Upstream is aware of the gap — `ResourceLocalizationService` carries the comment
`// TODO: remove untracked directories in local filesystem`.

### Why the deletion failed in the first place: root-owned content

Re-entering the chain is not enough on its own. Deletion runs **as the application user**, and a
significant share of the leaked directories contain content owned by `root`. A process running as
the application user cannot unlink the children of a directory it has no write bit on, so
`delete_path` descends, fails on every entry, and `rmdir` ends with `ENOTEMPTY`. That failure is
logged and swallowed (`LinuxContainerExecutor.deleteAsUser`), the application advances to `FINISHED`
regardless, and the next scan selects the same directory again — forever.

Two different mechanisms produce that content, and the **setgid bit tells them apart**:

| observed | where | who created it |
|---|---|---|
| `drwxr-sr-x root root` (2755) | the `appcache/<app_id>` directory itself | The Docker daemon re-creating a missing bind-mount source. Every container gets `<local-dir>/usercache/<user>/appcache/<app_id>` bind-mounted read-write; when that source has already been deleted at container start, dockerd creates it with `mkdir(0755)` followed by `chown(0,0)`. Setgid is inherited from the setgid `appcache` parent. |
| `drwxr-xr-x root root` (0755, **no** setgid) | any depth, typically `<container>/private_slash_tmp/*` | A process running as **root inside the container**, writing to a bind-mounted host directory — most often the container's `/tmp`. `container-executor` deliberately clears setgid on `private_slash_tmp` and `private_var_slash_tmp`, so directories root creates there get group `root` rather than inheriting the NodeManager group. |

Real example of the second case, which is the one that matters most:

```
# ls -al /yarn/data/usercache/rivers/appcache/application_.../container_.../private_slash_tmp
drwx--x---  6 rivers yarn  .
drwx--s---  5 rivers yarn  ..
drwxr-xr-x  2 root   root  container_centos_release_observability
drwxr-xr-x  2 root   root  container_hdp_client_check_observability
drwxr-xr-x  2 root   root  hadoop_class_observability
drwxr-xr-x  2 rivers prod  hsperfdata_rivers
```

Note that the application directory three levels above is owned correctly by `rivers`. **Any check
on the ownership of the application directory would therefore miss this**, which is why the helper
described in section 8 always walks the whole tree and filters on the ownership of each entry
instead.

### Hard invariant: never touch anything under `filecache/`

`recoverTrackerResources` replays localized resources from the state store **without checking that
the file still exists**. Removing a cached resource behind the NodeManager's back therefore leaves
the tracker handing containers a path to a missing file — and it does so across restarts, because
the tracker state is persisted.

`appcache/<app_id>` has no such tracking once the application is forgotten: the tracker entry was
dropped and the per-resource store entries removed as part of the finish chain. That asymmetry is
what makes collecting `appcache` safe and collecting `filecache` unsafe.

---

## 4. Why a restart does not help but a decommission does

```
NM restart, recovery enabled
  └─ state store recovered, isNewlyCreated() == false
       └─ cleanUpLocalDirs() SKIPPED  ──► leaked dirs survive untouched

Decommission, then start
  └─ NodeManager.stopRecoveryStore()  ──► "Removing state store due to decommission"
       └─ recovery store deleted
            └─ next start: isNewlyCreated() == true
                 └─ cleanUpLocalDirs()  ──► ALL local dirs renamed and deleted
                       ├─ leaked appcache dirs        (the intent)
                       ├─ public  filecache           (collateral)
                       └─ private filecache per user  (collateral)
```

The decommission path works, but it is an all-or-nothing wipe: it destroys the entire
localized-resource cache along with the leak. Every application landing on the node afterwards
re-downloads everything. That makes it unusable as a targeted garbage collector, and it is the
reason this change exists.

---

## 5. The fix

### One new arc into the state machine

The application must **not** be revived through `INIT_APPLICATION`. `AppInitTransition` registers
ACLs and dispatches `LogHandlerAppStartedEvent`, which makes `LogAggregationService` create the
remote application log dir on HDFS and start an aggregator thread for a dead application with no
credentials; `AppLogInitDoneTransition` would then also persist the revival via `storeApplication`.
Nor can a fresh `ApplicationImpl` simply be sent `FINISH_APPLICATION`: from `NEW` the only legal
events are `INIT_APPLICATION` / `INIT_CONTAINER`, so the event would raise
`InvalidStateTransitionException`, be swallowed by `handle()`, and leave an immortal entry in the
applications map.

Instead, a single arc is added from `NEW` directly to the cleanup state:

```
   ┌─────┐  INIT_APPLICATION   ┌─────────┐   APPLICATION_INITED  ┌─────────┐
   │ NEW │────────────────────►│ INITING │──────────────────────►│ RUNNING │
   └──┬──┘                     └────┬────┘                       └────┬────┘
      │                             │                                 │
      │                             │        FINISH_APPLICATION        │
      │  CLEANUP_ORPHANED_APPLICATION        (existing arcs)           │
      │        << NEW ARC >>        └──────────────┬──────────────────┘
      │                                           │
      │        ┌──────────────────────────────────▼───────────────────┐
      └───────►│         APPLICATION_RESOURCES_CLEANINGUP             │
               │  handleAppFinishWithContainersCleanedup()            │
               │    ├─ DESTROY_APPLICATION_RESOURCES                  │
               │    │    └─ appcache/<app> + nmPrivate/<app> over     │
               │    │       getLocalDirsForCleanup()  (incl full/bad) │
               │    └─ AuxServicesEvent(APPLICATION_STOP)             │
               └──────────────────────┬───────────────────────────────┘
                                      │ APPLICATION_RESOURCES_CLEANEDUP
                                      ▼
                                ┌──────────┐   AppCompletelyDoneTransition
                                │ FINISHED │     ├─ LogHandlerAppFinishedEvent
                                └────┬─────┘     ├─ NMTokenSecretManager.appFinished
                                     │           └─ timeline collector cleanup
                                     │ APPLICATION_LOG_HANDLING_FAILED
                                     ▼
                    removed from getApplications() + state store
```

The new transition body does nothing but log and call the **existing**
`handleAppFinishWithContainersCleanedup()`. Every downstream transition is reused unchanged.

**No log handler changes are required.** Both handlers already answer for an application they never
saw, which is exactly what closes the chain:

* `LogAggregationService.stopApp` — no aggregator for the application, so it warns
  `Log aggregation is not initialized for <appId>, did it fail to start?` and dispatches
  `APPLICATION_LOG_HANDLING_FAILED`.
* `NonAggregatingLogHandler` — no entry in `appOwners`, so it logs
  `Unable to locate user for <appId>` and dispatches `APPLICATION_LOG_HANDLING_FAILED`.

`FINISHED` + `APPLICATION_LOG_HANDLING_FAILED` runs `AppLogsAggregatedTransition`, which removes
the application from the map and calls `NMStateStore.removeApplication` (a no-op delete for an
absent key). The synthetic application collects itself. Because this path never reaches
`AppLogInitDoneTransition`, **nothing is persisted**: a crash mid-sequence leaves no zombie entry,
and the next scan simply finds the directory again. The operation is idempotent and crash-safe.

`AuxServices.handle(APPLICATION_STOP)` wraps each `stopApplication` in `try/catch Throwable`, so a
misbehaving auxiliary service cannot break the chain either.

### Scan flow

```
OrphanedAppReaper                      (runs on the ResourceLocalizationService
  │                                     cache-cleanup scheduler, every interval-ms)
  │
  ├─ 1. live  = nmContext.getApplications().keySet()
  ├─ 2. scan  = <localdir>/usercache/*/appcache/application_*   for localdir in getLocalDirs()
  │            + <localdir>/nmPrivate/application_*
  ├─ 3. keep candidates where:  id parses  AND  id NOT in live
  │                             AND  mtime older than min-age-ms
  └─ 4. cap at max-per-interval, then for each candidate:
           user  ◄── usercache/<user>/ path segment, else the NodeManager user
           │         (nmPrivate-only candidate); unresolvable ──► SKIP, log, next
           │
           ├─ AppDirReowner.reown(user, appId)           ──► sudo <reown.command> user appId
           │       │                                         makes root-owned content deletable
           │       └─ false? SKIP, log, bump
           │          orphanedAppDirsReownFailures, next  ──► never enters the chain
           │
           ├─ new ApplicationImpl(dispatcher, user, flowCtx, appId, new Credentials(), nmContext)
           ├─ getApplications().putIfAbsent(appId, app) ──► already present? SKIP, log, next
           ├─ metrics.runningApplication()               ──► balances the unconditional
           │                                                 endRunningApplication() at the
           │                                                 end of the chain
           └─ dispatch ApplicationEvent(appId, CLEANUP_ORPHANED_APPLICATION)
                    │
                    └─► existing chain does the rest (schema above)
```

Insertion into the map must precede the dispatch: `ContainerManagerImpl`'s
`ApplicationEventDispatcher` resolves the application through `context.getApplications().get(...)`
and warns-and-drops when it is absent.

The re-own step must precede insertion into the map, for the same reason the chain is unreliable in
the first place: it reports success without checking that the files were removed. Registering an
application whose directories cannot be deleted would advance it to `FINISHED` with the files still
on disk, and the next scan would select it again — the wasted cycle this whole change exists to
stop. When no helper is configured, `AppDirReowner.NOOP` reports success, so applications whose
directories are already owned correctly are still collected.

### An application with no containers left on the node is still live

The one thing that must never happen is reaping an application that has released every container
on this node but is **still running and still serving shuffle data** from
`usercache/<user>/appcache/<app-id>/` — a Spark job under dynamic allocation, or a MapReduce job
whose maps finished here while reduces elsewhere still fetch their output. Reaping one is doubly
destructive: the cleanup chain deletes the shuffle files *and* sends `APPLICATION_STOP` to the
auxiliary services, dropping the shuffle secret with it.

`getApplications()` covers that case, and does so by construction rather than by accident:

* In `RUNNING`, `APPLICATION_CONTAINER_FINISHED` transitions `ApplicationImpl` **`RUNNING` →
  `RUNNING`**. There is no arc out of `RUNNING` on "the last container is gone" — the container is
  simply dropped from `app.containers` and the application stays exactly where it was.
* The only way out of `RUNNING` is `FINISH_APPLICATION`, which the NodeManager raises solely from
  the `applicationsToCleanup` list of a ResourceManager heartbeat response — that is, when the
  *application* completes, not when its containers do.
* `AppLogsAggregatedTransition` holds the **only** `context.getApplications().remove(...)` in the
  NodeManager, and it is reachable only from `FINISHED`.
* The entry point of that chain, `handleAppFinishWithContainersCleanedup()`, is also what
  dispatches `AuxServicesEvent(APPLICATION_STOP)`. Local-directory destruction and shuffle-service
  teardown are the *same* transition.

So the set this scan reads is precisely the set of applications whose shuffle data the NodeManager
still considers live, and the reaper cannot be more aggressive than the NodeManager already is.
Upstream `TestApplication#testAppRunningAfterContainersComplete` already pins the `RUNNING`-with-
zero-containers behaviour. Anything that adds an arc out of `RUNNING` has to revisit this.

Startup ordering is safe for the same reason. `ContainerManagerImpl.serviceInit` ends with
`super.serviceInit(conf); recover();`, and `recoverApplication` repopulates the map synchronously
for every application in the state store, containers or not. The reaper is only scheduled in
`ResourceLocalizationService.serviceStart()`, with an initial delay of a full `interval-ms`
(1 h by default), so it can never observe a pre-recovery map. With recovery disabled,
`serviceInit` wipes the local directories outright before anything starts.

**The live map is the only protection here, and that is not a gap to be patched.** Once the last
container on the node exits, the tree goes read-only: serving shuffle data to other nodes is pure
reads, and reads move no modification time anywhere in it. So no amount of filesystem inspection —
mtime at any depth, size, entry counts — can distinguish an application still serving shuffle from
one that finished months ago. The two look identical on disk *by construction*. `min-age-ms` is
not a weaker version of the live check; it answers a different question ("did a container end here
just as the scan ran?") and there is no filesystem-derived signal that answers the real one.

Nor is there a second in-memory authority to cross-check against: `AuxServices` keeps no set of
active applications, and adding a query to the `AuxiliaryService` interface would be a public-API
change that out-of-tree services such as Spark's external shuffle service would not implement.
`getApplications()` is load-bearing and single, which is exactly why the reasoning above spells out
why it holds rather than leaving it to be rediscovered.

### Safety properties

| Property | How it is enforced |
|---|---|
| A live application is never disturbed | `putIfAbsent` only; skip and log if an entry already exists |
| An application with no container left on the node, but still serving shuffle data, is never selected | it is still in `getApplications()`: `ApplicationImpl` stays in `RUNNING` on `APPLICATION_CONTAINER_FINISHED`, and the map entry is removed only at the end of the chain that also stops the auxiliary services. See the section above |
| A directory the NodeManager was still using is never selected | minimum-age guard on the application directory's mtime, which is when its last container work directory was created or removed — see the note in section 7 |
| Nothing under `filecache/` is ever selected | only `usercache/<user>/appcache/` and `nmPrivate/` are scanned; a `filecache` path segment where a user is expected is skipped |
| A failing disk is never read as "no live applications" | the live set comes from the in-memory map, never from the filesystem |
| An unlistable directory yields no candidates | fail closed: warn and skip that directory for this scan |
| A first scan on a badly-leaked node cannot cause a deletion storm | `max-per-interval` cap; the rest is picked up by later scans |
| A directory that cannot be made deletable is never marked as cleaned | the re-own step precedes registration; on failure the application is skipped, counted in `orphanedAppDirsReownFailures`, and left for a later scan. The helper reports failure rather than success whenever it processed no directory at all, so "nothing to do" and "nothing was attempted" cannot be confused |
| The privileged helper cannot be pointed at an arbitrary tree | it takes only a user name and an application id, derives every path from a root-owned `yarn-site.xml`, and only ever chowns entries currently owned by `root:root` (section 8) |
| A failure cannot silently disable the scan | `run()` catches `Throwable`, since a scheduled task that throws is never rescheduled |
| A slow helper cannot stall cache cleanup | `reown.timeout-ms` bounds each invocation, on the same two-thread scheduler |
| Container launches are never delayed | the filesystem walk runs on the cache-cleanup scheduler (core size raised to 2), and only per-application events are dispatched; no walk ever lands on the localization dispatcher thread |

---

## 6. Scope boundaries

### Log directories are *not* part of this leak (verified)

Nothing is added for log dirs, because log-dir deletion does not share the fragility of `appcache`
deletion:

* `NonAggregatingLogHandler` on `APPLICATION_FINISHED` **persists** a
  `LogDeleterProto{user, deletionTime}` via `stateStore.storeLogDeleter`, and the
  `LogDeleterRunnable` it schedules carries only `(user, appId)` — no `Application` object and no
  map lookup.
* `recover()` reloads every persisted deleter at startup and reschedules it with
  `deletionTime - now`; a negative delay runs immediately, and a `RejectedExecutionException` falls
  back to running it inline.
* `LogDeleterRunnable.run()` enumerates `dirsHandler.getLogDirsForCleanup()` (full and bad dirs
  included), deletes as the application user, then clears the store entry.
* On the aggregation path, `AppLogAggregatorImpl.doAppLogAggregationPostCleanUp()` deletes the local
  application log dirs after aggregation, from both the normal and the abort path.

In short: log deletion is **persisted, restart-safe and independent of `context.getApplications()`**,
whereas `appcache` deletion is a one-shot in-memory event whose result is never checked.

For the leak addressed here — where the application *did* complete its state machine, hence is
absent from the map and from the NodeManager UI — `AppCompletelyDoneTransition` had already
dispatched `LogHandlerAppFinishedEvent`, so the log deleter was scheduled and the log dirs were
removed after `yarn.nodemanager.log.retain-seconds`.

The single narrow residual case is an application that never received `FINISH_APPLICATION` at all,
where neither deletion was scheduled. If the NodeManager has **not** restarted since, `appOwners`
(in-memory only, never persisted) still holds the user, so this change's chain schedules the log
deleter too and the log dirs are cleaned as a **free bonus**. Only if the NodeManager restarted in
the meantime does the handler take the null-user branch and skip log deletion.

### What this does *not* fix

This collects the residue and makes it deletable. It does **not** stop the residue being created.
Three separate mechanisms keep producing it, all outside the scope of this change:

* **The fire-and-forget destroy racing an in-flight container launch.**
  `handleDestroyApplicationResources` dispatches `APPLICATION_RESOURCES_CLEANEDUP` before any
  deletion has run, so an application directory can be deleted while a container of that
  application is still being launched — after which dockerd re-creates it as `root`. Fixing this
  properly means not destroying application resources while a launch is in flight.
* **Leftover Docker containers.** A container that was never reaped can be restarted by a dockerd
  restart, which re-resolves its bind mounts and re-creates the deleted source directory as `root`.
* **Containers running as root** writing into bind-mounted host directories. Nothing on the
  NodeManager side can prevent this; it is a property of the images being run.

One residual risk is accepted rather than solved, and is documented as E3 in section 8: a process
running as root inside a container can hardlink a `root`-owned file from elsewhere on the same
filesystem into the tree, and the helper would re-own it. The blast radius is bounded to
`root`-owned files reachable through that container's own bind mounts, and the application is dead
and idle for `min-age-ms` before the helper runs.

---

## 7. Configuration reference

| Property | Default | Recommended in production |
|---|---|---|
| `yarn.nodemanager.orphaned-app-dirs.cleanup.enabled` | `false` | `true` after canary validation |
| `yarn.nodemanager.orphaned-app-dirs.cleanup.interval-ms` | `3600000` (1 h) | `3600000` |
| `yarn.nodemanager.orphaned-app-dirs.min-age-ms` | `21600000` (6 h) | `21600000` |
| `yarn.nodemanager.orphaned-app-dirs.max-per-interval` | `50` | `50` |
| `yarn.nodemanager.orphaned-app-dirs.reown.command` | *(empty)* | `/usr/libexec/hadoop-yarn/yarn-reown-orphan-app-dir` once section 8 is done |
| `yarn.nodemanager.orphaned-app-dirs.reown.timeout-ms` | `60000` (1 min) | `60000` |

Notes:

* `min-age-ms` is compared against the **modification time of the application directory**, taking
  the most recent value observed across all local dirs. Worth being precise about what that
  timestamp is, because it is not "when the tree was last written to": a directory's mtime moves
  when a *direct child* is created or removed, and container work directories are exactly the
  direct children of `appcache/<app-id>`, so it is **when the last container work directory on this
  node was created or deleted**. That is the closest the local filesystem gets to "when the
  NodeManager last used this tree", and it is the reason no deeper scan is performed: once the last
  container is gone, nothing writes below the application directory either — serving shuffle data
  to other nodes is read-only, and reads move no modification time — so walking deeper costs a
  listing per candidate and returns the same answer.
* `min-age-ms` is therefore defence in depth against a container that ended just as the scan ran,
  not the protection against reaping a live application. That protection is absence from
  `getApplications()`, and it is exact. There is no reason to lower `min-age-ms` below an hour.
* `max-per-interval` bounds the first scan on a node that has already leaked heavily. With the
  defaults, a node holding 500 orphaned applications drains over ten scans, i.e. about ten hours.
  Raise it if you want a badly-leaked fleet to converge faster, having first confirmed the deletion
  load is acceptable.
* `reown.command` is empty by default, which means no helper runs. The scan still works, but every
  application whose directories hold `root`-owned content is skipped — so on a fleet where that is
  the dominant cause, enabling the scan alone will collect very little.
* `reown.timeout-ms` bounds a single helper invocation. The scan shares the two-thread cache-cleanup
  scheduler, so an unbounded call would stall cache cleanup.
* Changing any of these requires a NodeManager restart; they are read in `serviceInit`.

---

## 8. Making root-owned directories deletable

### What the helper is

`yarn-reown-orphan-app-dir` is a small shell script, run as root through `sudo`, invoked once per
application the reaper is about to clean up. It takes **only a user name and an application id** —
no filesystem path crosses the process boundary — and derives the directories itself from
`yarn.nodemanager.local-dirs` read out of a root-owned `yarn-site.xml`.

The entire operation is one command:

```bash
chown -Rh --from=0 "$user:$nm_group" -- "$dir"
```

`--from=0` is what makes this safe: only entries **currently owned by `root`** are touched, at
any depth, so nothing else in the tree can be re-targeted even if path validation were wrong. It is
also the only approach that handles the deep case from section 3, where the application directory
itself is owned correctly. `chown -R` defaults to `-P` and uses `fts` with `openat`, so it is
race-resistant inside the tree and never dereferences a symlink — including one handed to it as its
argument.

The group is deliberately **not** constrained. `--from=0:0` would look tighter and is not: a root
process inside the container can `chgrp` anything it hardlinks in, so filtering on the group stops
no attacker, while it does skip the benign `root:<nm-group>` entries that a root process produces
whenever it writes under a directory that still carries the setgid bit. Those would then be reported
as "nothing to do" and left undeletable forever.

**At most, the helper can:** `chown` `root:root` entries to `<validated-user>:<nm-group>` under two
derived paths per configured local dir. It never deletes, never creates, never writes file content
and never changes modes.

Two paths are handled per local dir, with different targets:

* `<local-dir>/usercache/<user>/appcache/<app_id>` → `<user>:<nm-group>`, because it is deleted as
  the application user.
* `<local-dir>/nmPrivate/<app_id>` → `<nm-user>:<nm-group>`, because that one is deleted **in
  process** as the NodeManager user, not through `container-executor`. Re-owning it to the
  application user would just move the failure.

An application whose `usercache` directory is already gone has no user to derive, so the reaper
passes the NodeManager's own — which the banned-user check would otherwise reject on every scan,
forever. The script recognises that case by comparing against `SUDO_USER`, which `sudo` sets and the
caller cannot forge, and then handles **only** the `nmPrivate` path. It grants nothing: `nmPrivate`
is re-owned to the invoking uid on every call whatever the user argument is, whereas re-owning
`usercache/<invoking-user>` would point the helper at a tree the caller itself controls.

### Why `sudo` and not a setuid script

**The Linux kernel ignores setuid and setgid bits on `#!` scripts**, for any interpreter. Deploying
the script `6050 root:yarn` — the mode `container-executor` uses — would silently leave it running as
`yarn`, with `chown` returning `EPERM` and nothing explaining why. For a script the mechanism has to
be `sudo`, whose authorization comes from `sudoers` rather than from file modes.

That is also why the script can be mode `0700`: `yarn` needs neither read nor execute permission on
it. `sudo` is itself setuid-root, becomes root, and `execve`s the script as root, so **root** is the
identity that opens the file and runs the interpreter. Do not "fix" this to `0755`.

### Deployment

| What | Where | Owner | Mode |
|---|---|---|---|
| re-own script | `/usr/libexec/hadoop-yarn/yarn-reown-orphan-app-dir` | `root:root` | `0700` |
| verification script | `/usr/libexec/hadoop-yarn/yarn-reown-orphan-app-dir-verify-setup` | `root:root` | `0700` |
| sudoers drop-in | `/etc/sudoers.d/yarn-reown-orphan-app-dir` | `root:root` | `0440` |

Every owner and mode above is a security requirement, each mapping to a row in the threat model
below. Enforce them from packaging (`%attr(0700,root,root)` in the rpm spec, or the puppet `file`
resource), never by hand, so that a redeploy cannot silently relax them.

Both scripts live in `hadoop-yarn-project/hadoop-yarn/bin/` in the source tree and are **not** part
of any assembly, so they are absent from the distribution tarball. That is deliberate — a 0700
root-owned file has no business being unpacked from a `yarn`-owned tarball — but it means the
packaging step has to take them from a source checkout, and that a Hadoop upgrade does not update
them on its own.

* **Do not install under `$HADOOP_HOME/bin`.** That tree is `yarn`-owned in the common layout, and
  `sudo` running a `yarn`-writable script as root hands `yarn` a root shell.
* `/usr`, `/usr/libexec` and `/usr/libexec/hadoop-yarn` must all be root-owned with no group or
  other write bit. A writable ancestor means the script can be replaced wholesale.

Sudoers content:

```
Defaults:yarn !requiretty
yarn ALL=(root) NOPASSWD: /usr/libexec/hadoop-yarn/yarn-reown-orphan-app-dir
```

Validate with `visudo -cf /etc/sudoers.d/yarn-reown-orphan-app-dir` **before** the file lands — a
broken drop-in can break `sudo` for every user on the node — and confirm `#includedir
/etc/sudoers.d` is active in `/etc/sudoers`. The `Defaults` line is only needed where `requiretty`
is set globally (RHEL-family historically) and is harmless elsewhere.

Restricting the arguments in `sudoers` (`... yarn-reown-orphan-app-dir [a-z]* application_*`) is
reasonable defence in depth, but it is **not** a substitute for the script's own validation: a
command spec without arguments permits any arguments, and `sudo` passes argv rather than a shell
string.

### Pre-flight

* **`/etc/hadoop/conf/yarn-site.xml` and every ancestor must be root-owned with no group or other
  write bit.** The script refuses to read a configuration it does not trust, because
  `local-dirs` decides which trees get chowned. This is the check most likely to fail on a first
  deployment — some layouts ship Hadoop configuration as `yarn`-owned. **Fix the ownership; do not
  weaken the check.**
* `xmllint` must be present (package `libxml2`). The script aborts rather than regex-parsing XML.
* `realpath` must be present (package `coreutils`). The configuration path is resolved before its
  ancestors are checked, so that a symlinked component — `/etc/hadoop/conf` pointing at a versioned
  or alternatives-managed directory — is checked as the directory it really is, rather than as a
  link whose own `0777` mode would fail the check.
* `yarn.nodemanager.local-dirs` must be written as plain absolute paths. The script reads
  `yarn-site.xml` directly and does not expand Hadoop's `${...}` property references; a value
  containing one is refused rather than skipped, because "skipped" and "nothing to do" must not
  look alike to the NodeManager.
* The NodeManager must not run as root; the script derives the `nmPrivate` owner from `SUDO_UID` and
  rejects `0`.

An optional root-owned `/etc/yarn-reown-orphan-app-dir.conf` may override `YARN_SITE`,
`NM_GROUP_FALLBACK`, `MIN_UID` and `BANNED_USERS` as `KEY=value` lines. It is parsed, not sourced, so
it cannot execute code, and it is subject to the same ownership check. The configuration location is
never taken from the environment: anything `sudo` lets through is caller-controlled.

### Verification

One command, as root:

```
/usr/libexec/hadoop-yarn/yarn-reown-orphan-app-dir-verify-setup
```

It is **read-only** — it checks and reports, never fixes — so it is safe to run on a production node
and safe to re-run. It exits `0` only when every check passed, so it can gate a puppet run or a
canary rollout, and prints remediation under each failure. It verifies: the script's ownership, mode
and ancestors; the sudoers file's ownership, mode and `visudo` validity; that `#includedir` is
active and that `sudo -l -U yarn` actually resolves the rule with `NOPASSWD`; `yarn-site.xml`
trust and that at least one `local-dirs` entry exists; the override file's trust, if it is present;
`xmllint` and `realpath`; that the NodeManager user resolves and is
not uid 0; that `reown.command` matches the installed path (a mismatch here is the likeliest silent
failure); and a **non-mutating end-to-end call** through the real `sudo` path against
`application_0_0`, which cannot exist. `--scan` additionally counts `root`-owned entries under
`usercache` and `nmPrivate` in each local dir, as a baseline of the backlog.

That end-to-end call is the check that matters: it is the only one that exercises the whole path
rather than its parts. It needs a `usercache/<user>` directory to name, so **run the verification on
a node that has actually run containers**. If it cannot be made, the script fails rather than
warning — a green run that quietly skipped it is exactly how a broken deployment gets promoted.

Only then set `yarn.nodemanager.orphaned-app-dirs.reown.command` and restart the NodeManager.

### Threat model

The helper only ever runs against an application **absent from `getApplications()`** — which, as
shown above, still contains any application with a container left, and any application with none
left that is still serving shuffle data. That check, not the age guard, is what keeps a live
application out of the helper's way. On top of it, no container work directory may have been
created or removed in the tree for `min-age-ms` (default 6 h).

| # | Exploit | How it is prevented |
|---|---|---|
| E1 | The application user owns `appcache/`, so it can swap `application_X` for a symlink to `/etc` between the reaper's decision and the chown | `chown -R` defaults to `-P` and does not descend a symlink given as its argument; `-h` makes it act on the link inode. Plus an explicit symlink test before acting |
| E2 | A root process inside the container plants symlinks deep in the tree (it controls `private_slash_tmp`) | `fts` `FTS_PHYSICAL` never dereferences; only the link inode is chowned, which is inert — symlink ownership only matters for sticky-directory unlink rules |
| E3 | A root process inside the container hardlinks a `root`-owned file from elsewhere on the same device into the tree | **Residual, accepted.** Bounded to `root`-owned files on the same filesystem reachable through that container's own bind mounts, and `--from=0` limits the effect to files that are already `root`-owned. `fs.protected_hardlinks=1` does not help against real root, but the application is dead and idle for `min-age-ms` before we act |
| E4 | A leftover bind mount or tmpfs under the tree, so `chown -R` crosses into a live filesystem | `chown` has no `--one-file-system`, so any tree containing a mount point per `/proc/self/mountinfo` is refused outright. This is treated as a failure, not a skip, so the metric surfaces it |
| E5 | `yarn` rewrites `yarn.nodemanager.local-dirs` in `yarn-site.xml` to redirect the chown at an arbitrary tree | `yarn-site.xml` and every ancestor must be root-owned and not group/other-writable before it is read. Abort otherwise |
| E6 | `yarn` replaces the script that `sudo` runs as root | `sudoers` names an absolute path that must live outside any `yarn`-writable tree; `0700 root:root` additionally denies `yarn` read and execute, making `sudo` the only route in. Both scripts use an absolute `#!/bin/bash` rather than `/usr/bin/env bash`, so the interpreter is not resolved through `PATH` |
| E7 | Path traversal or injection through `<user>` / `<app_id>` | Both are regex-validated before use (`^application_[0-9]+_[0-9]+$`; a user name that resolves, has uid ≥ `MIN_UID` and is not in `BANNED_USERS` — the same shape as `check_user` in `container-executor`, but **not** the same source: the script carries its own `MIN_UID` and `BANNED_USERS` rather than reading `min.user.id`, `banned.users` and `allowed.system.users` from `container-executor.cfg`, so a cluster that tunes any of those must mirror the change in the override file). No path comes from argv, and `sudo` passes argv rather than a shell string |
| E8 | Chowning a live application's directories | The reaper only selects applications absent from `getApplications()`, which still holds an application that has released every container on the node but is serving shuffle data. On top of that, no container work directory may have been created or removed in the tree for `min-age-ms`. The chown happens before the synthetic application is registered, and `putIfAbsent` still guards the race |
| E9 | A `root`-owned setuid binary in the tree becomes setuid-application-user | Linux clears `S_ISUID`/`S_ISGID` on `chown` of a non-directory |
| E10 | Environment manipulation through `sudo` | `env_reset` is `sudo`'s default, both scripts pin `PATH` themselves, `sudo -n` never prompts or reads a tty, and the configuration location is never read from the environment |

### Rollback

Set `reown.command` to `""` and restart, or remove the sudoers file. Either degrades to the previous
behaviour: `root`-owned trees are simply not collected. Removing the sudoers file while the
configuration still points at the script shows up as `orphanedAppDirsReownFailures` climbing, which
is the intended alert.

---

## 9. Rollout and rollback

1. **Deploy the helper** (section 8) and confirm `yarn-reown-orphan-app-dir-verify-setup` exits `0`
   on the canary nodes. Do this first: without it the canary will select very little and you will
   draw the wrong conclusion about the scan.
2. **Canary.** Enable on a handful of NodeManagers known to be leaking. Confirm from the logs
   (section 10) that the applications selected are genuinely gone, that `usercache` shrinks, and that
   `orphanedAppDirsReownFailures` stays flat.
3. **Per-cluster enablement.** Roll out cluster by cluster, leaving the defaults for interval,
   minimum age and cap.
4. **Rollback.** Set `yarn.nodemanager.orphaned-app-dirs.cleanup.enabled=false` and restart the
   NodeManager. No persisted state is introduced by this feature, so there is nothing to undo. To
   keep the scan but stop the privileged helper, clear `reown.command` instead.

---

## 10. Ops runbook

### Confirm the feature is active

At NodeManager startup, exactly one of these is logged:

```
Cleanup of orphaned application directories is disabled; set
yarn.nodemanager.orphaned-app-dirs.cleanup.enabled to true to enable it

Cleanup of orphaned application directories is enabled, scanning every 3600000 ms
for application directories unmodified for at least 21600000 ms, at most 50
applications per scan
```

and, **only when the scan is enabled**, exactly one of these for the privileged helper — the
helper is resolved as part of scheduling the scan, so a disabled scan logs neither line:

```
No re-own helper configured (yarn.nodemanager.orphaned-app-dirs.reown.command is unset),
so orphaned application directories containing root-owned content will not be collected

Orphaned application directories will be made deletable with
/usr/libexec/hadoop-yarn/yarn-reown-orphan-app-dir, timing out after 60000 ms
```

A `reown.command` that is not an absolute path is refused at startup, because `sudoers` names an
absolute path and `sudo` could only ever reject it:

```
yarn.nodemanager.orphaned-app-dirs.reown.command must be an absolute path but is
'yarn-reown-orphan-app-dir'; the re-own helper is disabled, so orphaned application
directories containing root-owned content will not be collected
```

### Confirm a scan ran, and what it decided

One summary line per scan:

```
Orphaned application directory scan: 12 application directories found, 3 still tracked,
2 too young, 0 deferred to a later scan by the per-interval cap of 50, 0 with no resolvable
user, 1 whose directories could not be made deletable, cleanup dispatched for 6
```

The second-to-last count is the one to watch. It is mirrored by the
`orphanedAppDirsReownFailures` counter in the NodeManager metrics, and a sustained increase means
the helper is misdeployed — most often a missing `sudo` rule, or a `yarn-site.xml` the helper does
not trust. Those applications are **not** pushed through the cleanup chain, so nothing is silently
marked as cleaned; they simply stay on disk until the deployment is fixed. Re-run
`yarn-reown-orphan-app-dir-verify-setup` on the node, which will name the problem.

The last count is the other one to watch, and for the opposite reason: **it should fall.** A
`cleanup dispatched for N` that stays at the same non-zero N scan after scan, with `usercache` not
shrinking and the failure counter flat at zero, means the chain is reporting success while the
files stay on disk — the same application selected again on every pass. That is the signature of a
re-own that did nothing but claimed it had. Check the helper by hand on one of the named
applications; it now exits non-zero rather than 0 in every case where it processed nothing.

When the helper does find something, it logs what it changed, per application:

```
Re-owned the local directories of application_1_0001: yarn-reown-orphan-app-dir: re-owned 4
root-owned entries under appcache /yarn/data/usercache/alice/appcache/application_1_0001 to alice:yarn
```

and when there was nothing to fix — the common case for an application that leaked for other
reasons — the helper's own `no root-owned entries under ...` lines appear at the same place. The
helper reports one line per directory it looked at and the NodeManager joins them with `; `, so a
node with three local dirs produces one log line holding six reports.

Then, per selected application:

```
Dispatching cleanup of orphaned application application_1_0001 for user alice
Cleaning up local directories of orphaned application application_1_0001 for user alice
Application application_1_0001 transitioned from NEW to APPLICATION_RESOURCES_CLEANINGUP
```

followed by the pre-existing chain lines, which are the same as on any normal application finish:

```
Log aggregation is not initialized for application_1_0001, did it fail to start?   (or)
Unable to locate user for application_1_0001
Application application_1_0001 transitioned from APPLICATION_RESOURCES_CLEANINGUP to FINISHED
```

Per-decision detail:

```
Skipping orphaned application application_1_0001: local directories were modified N ms ago,
  less than the configured minimum age of 21600000 ms
Skipping orphaned application application_1_0001: it was registered with the NodeManager
  during the scan
Ignored 2 local directories whose name is not an application id: [...]
No usercache directory found for orphaned application application_1_0001, cleaning up its
  nmPrivate directory as yarn
```

At DEBUG only:

```
Skipping application_1_0001: still tracked by the NodeManager
```

### Force a scan

Lower `yarn.nodemanager.orphaned-app-dirs.cleanup.interval-ms` and restart the NodeManager. The
first scan runs after one full interval, not at startup. There is no way to trigger one on demand.

### If something goes wrong

```
Orphaned application directory scan failed
Unable to list <path>, skipping it for this scan
```

The first means one scan aborted; scanning continues on the next tick, since `run()` never lets an
exception escape. The second is the fail-closed path for an unlistable directory — that directory
contributes no candidates for that scan.

Helper failures are logged with the reason spelled out, because each one has a different fix. The
`Reason:` each line carries is the helper's own stderr, which is the only channel Hadoop's `Shell`
surfaces for a failing command:

```
Re-owning the local directories of <app> failed: <script> refused to read its configuration
  because it is not root-owned or is group/other-writable                        (exit 65)
    -> fix the ownership of yarn-site.xml (section 8, pre-flight). Do not weaken the check.

Re-owning the local directories of <app> failed: <script> rejected user '<user>' or the
  application id                                                                 (exit 64)
    -> the user is banned, below MIN_UID, or does not resolve on this node.

Re-owning the local directories of <app> failed: <script> found no usable directory among
  the configured local dirs, so nothing was re-owned                             (exit 67)
    -> the yarn-site.xml the helper reads names local dirs that do not exist on this node,
       or writes them with ${...} property references it cannot expand. This is the one that
       used to look like success; the application is now skipped instead of being marked
       cleaned with its files still on disk.

Re-owning the local directories of <app> for user <user> failed with exit code 1
    -> almost always the missing sudoers rule; sudo itself exits 1 when it refuses.
       Run yarn-reown-orphan-app-dir-verify-setup.

Re-owning the local directories of <app> for user <user> failed with exit code 66
    -> a chown failed, or a mount point exists at or below the tree and it was refused
       rather than chowned across a filesystem boundary.

Timed out after 60000 ms re-owning the local directories of <app> for user <user>
    -> a very large tree, or a hung filesystem. The application is skipped and retried on the
       next scan; raise reown.timeout-ms only after checking the disk is healthy.
```

In every case the application is **skipped, not cleaned**, and `orphanedAppDirsReownFailures` is
incremented.

### Signs you are looking at the old full-wipe path instead

These lines are **pre-existing** and indicate the decommission wipe of section 4, not this feature:

```
Removing state store due to decommission
usercache path : <path>
```

If you see those, the node lost its entire localized-resource cache — which is exactly what this
change is meant to make unnecessary.
