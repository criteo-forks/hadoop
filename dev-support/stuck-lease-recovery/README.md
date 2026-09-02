<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Stuck open files with an all-corrupt last block

`hdfs stuckleases` replaces the manual metasave / fsck / `recoverLease` pipeline
used to unstick open files whose last block has every replica reporting
`GENSTAMP_MISMATCH`.

## Why these files get stuck

A writer that dies inside pipeline recovery leaves the NameNode's stored
generation stamp for the last block ahead of every replica, so all replicas are
flagged corrupt. Two consequences:

* The block is not COMPLETE, so `BlockManager.updateNeededReconstructions`
  drops it and it never enters `QUEUE_WITH_CORRUPT_BLOCKS`. It is therefore
  **invisible** to the `MissingBlocks` metric, to `hdfs fsck`, and to
  `listCorruptFileBlocks`. The `Corrupt Blocks:` section of a metasave is the
  only place it shows up — which is why the old runbook started there.
* Recovery itself works, but the cadence does not.
  `FSNamesystem.internalReleaseLease` renews the lease after every failed
  attempt, while `LeaseManager` only selects leases past the hard limit
  (20 min in this tree), so a block needing several rounds gets one round per
  20 minutes. `recoverLease` with `force=true` bypasses the expiry gate and
  starts a round immediately. That is the only thing the manual loop was doing.

Recovery is **asynchronous**: the first `recoverLease` returns `false` having
only queued `DNA_RECOVERBLOCK`. The primary datanode picks it up on its next
heartbeat and answers with `commitBlockSynchronization`, which force-completes
the block and closes the file — usually within seconds. The extra
`recoverLease` calls in the old runbook were almost always probes observing a
closure the *first* call had already caused, and each one took the
FSNamesystem write lock and journalled a lease reassignment for nothing.

## Usage

Runs on a NameNode host, under an elevated keytab (the ambient Kerberos context
is used as is — no `-principal`/`-keytab`). It checks whether the local
NameNode is ACTIVE and exits 0 on a standby, so the same cron entry can be
installed on every NameNode. Under federation it binds to the local
nameservice; see below.

```
hdfs stuckleases [-ns <nameservice>] [-path <prefix>] [-execute]
                 [-includeLiveHolders] [-full] [-logDir <dir>]
                 [-attempts <n>] [-pollSec <n>] [-reissueSec <n>]
                 [-issueDelayMs <n>] [-fsckBatch <n>] [-maxFiles <n>]
```

### Federation

Every RPC is bound to the **local** NameNode's own address, not to
`fs.defaultFS` — under federation that is a mount table or some other
nameservice entirely. The nameservice, and within it the NameNode, is
identified by matching configured RPC addresses against this host's addresses;
a host serving more than one nameservice needs `-ns` to disambiguate. Paths in
the report, and the `-path` prefix, are relative to that namespace.

Addressing the NameNode directly rather than through a logical HA URI is
deliberate: `metaSave` writes into the log dir of whichever NameNode serves it,
and the tool reads that file locally. A failover mid-pass then surfaces as a
`StandbyException` and stops the pass, instead of silently continuing against a
host whose log dir cannot be read.

Each pass runs as rounds across the whole working set rather than one file at
a time, so it costs `attempts * (files * issueDelayMs + reissueSec)` at worst
however many files it is working on, instead of that per file. Every
`recoverLease` takes the FSNamesystem write lock and logSyncs two edit ops, so
`-issueDelayMs` defaults to 500ms; bound a first run against an accumulated
backlog with `-maxFiles` rather than by tightening it.

Two detectors, joined on path:

* **Cheap pass (always).** `listOpenFiles`, bucketed by lease holder. Catches
  files the NameNode has already taken the lease for.
* **Full pass (`-full`).** `metaSave` parsed for the `Corrupt Blocks:` section,
  then one batched in-process `fsck -blockId` call per `-fsckBatch` ids to map
  block to path. Needed to see corruption, but `metaSave` takes the FSN read
  lock and dumps every low-redundancy queue and datanode, so keep it on a slow
  schedule.

  Note that `metaSave` prints `blk_<id>_<gs>` while `fsck -blockId` accepts
  only `blk_<id>` — `Block.getBlockId` matches `blk_<id>` or
  `blk_<id>_<gs>.meta`, and given the bare `blk_<id>_<gs>` returns 0, so fsck
  silently reports that block 0 does not exist. The generation stamp is
  stripped before the fsck call. It would be wrong to report anyway: the value
  metaSave prints is the *first reported* stamp, not the NameNode's stored one.

| Class | Meaning | Default action |
|---|---|---|
| `NN_STUCK` | Lease already reassigned to `HDFS_NameNode-*` | recover |
| `SWEEPER_RETRY` | Carries this tool's `HDFS_StuckLeaseSweeper-*` holder | recover |
| `LIVE_HOLDER_CORRUPT` | Lease still held by a client, last block all-corrupt | report only |
| `OPEN_HEALTHY` | Ordinary in-flight writer | counted, not listed |
| `CLOSED_CORRUPT` | Corrupt but not open: real corruption | report |

`NN_STUCK` is safe by construction: reaching `reassignLease` requires the
original client to have stopped renewing for a whole hard-limit period *and*
the NameNode to have started recovery, so that writer is finished either way.
`LIVE_HOLDER_CORRUPT` is the one case where acting would be a judgement call
about a possibly live client, so it needs `-includeLiveHolders` explicitly.

Outcomes are `RECOVERED`, `RETRY_NEXT_PASS`, `NEEDS_HUMAN` and `REPORT_ONLY`.
`RETRY_NEXT_PASS` is **not** a failure — those files keep the sweeper's holder
and are picked up by the next pass as `SWEEPER_RETRY`. Exit status is non-zero
only when something is `NEEDS_HUMAN`.

## Operating it

```sh
# 1. Look before touching anything.
hdfs stuckleases -full

# 2. Try one file.
hdfs stuckleases -full -execute -path /some/one/file -maxFiles 1

# 3. Cron: cheap pass often, full pass slowly.
*/5  * * * *  hdfs stuckleases -execute
*/30 * * * *  hdfs stuckleases -full -execute
```

Watch the `CorruptBlocks` JMX metric, **not** `MissingBlocks`, which by design
never counts these blocks.

## Dead ends the tool reports instead of retrying

`NEEDS_HUMAN` is reserved for cases where more attempts provably cannot help:

* **Penultimate block below min storage.** `internalReleaseLease` throws
  `AlreadyBeingCreatedException` ("Committed blocks are waiting to be minimally
  replicated") *before* attempting any recovery.
* **Path gone** — `FileNotFoundException`.

Two more dead ends cannot be distinguished after a single pass, and show up as
a growing attempt count across passes:

* **Zero expected locations with non-zero length.**
  `BlockUnderConstructionFeature.initializeBlockRecovery` logs the misleading
  `"No blocks found, lease removed."`, issues no datanode command and returns
  false forever. HDFS-8344, whose whole purpose was to force-close here, is
  reverted in this tree.
* **All expected locations dead** — `primary == null`, no datanode command and
  no warning.

Log lines worth grepping on the NameNode:
`"recoverLease: ... from client"`,
`"Lease recovery is in progress. RecoveryId ="`,
`"Committed blocks are waiting to be minimally replicated"`,
`"No blocks found, lease removed."`,
`"Block recovery attempt for ... rejected"`.
On the datanodes: `"recover Block: ... FAILED"`,
`"Ignored replica with invalid generation stamp or length"`,
`"All datanodes failed"`.

## Known NameNode-side defects, not fixed here

* `internalReleaseLease` renews the lease on every failed attempt, so a lease
  under active recovery cannot be re-examined faster than the hard limit.
* A recovery attempt suppressed by `addBlockRecoveryAttempt` still renews the
  lease, costing a full hard-limit period for a pass that did no work.
* `pendingRecoveryBlocks` only shrinks via `successfulBlockRecovery`, called
  from `commitBlockSynchronization` alone. Nothing removes the entry when a
  block is deleted or recovery is abandoned, so every block that never recovers
  leaves a permanent entry.
* These blocks are invisible to `MissingBlocks` and `listCorruptFileBlocks`.
  Exposing "open files stuck in recovery" would remove the need for the
  metasave + fsck pass entirely.
