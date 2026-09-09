# Persistent Durable Journaling

Status: proposal for maintainer review. This document changes no runtime
behavior or on-disk format and does not claim a measured performance gain.

## Motivation

Keep one producer-owned writer open across transactions, group durable writes,
and let readers advance independently through committed history. The storage
layer should own ordering, durability, recovery, and retention so applications
do not need a second journal to supply those guarantees.

The proposed contract is stronger than the current API. At
[v0.18.2](https://github.com/mojura/kiroku/tree/v0.18.2), commit
`e39bd0824af398edbd5e642f4498a1c46f4c8f55`:

- `Producer.transaction` opens a temporary file per transaction, ignores the
  writer's `Close` error, renames it, and wakes the watcher. There is no explicit
  file or directory sync barrier in that path.
- `batcher.Batch` returns after its callback runs, before the enclosing
  transaction completes. Later transaction errors cannot reach that caller.
- A chunk is a transaction boundary. `Reader.ForEach` reads encoded blocks to
  EOF; consumers process and remove entire chunks.

Consequently, replacing the temporary file with a shared append file is not
compatible: a complete encoded block does not prove a complete transaction,
and existing consumers must never remove an active writer's file.

## Ownership and Commit Contract

One producer owns a journal directory under an operating-system ownership lock
held until shutdown finishes. A second producer must fail before modifying
files. The lock must cover generation changes; locking only a file that rotation
replaces is insufficient. A crash releases process ownership without requiring
PID-based lock stealing.

One writer owns the active file descriptor and buffer. Transaction callbacks
produce isolated transaction data; callback failure publishes nothing. The
writer assigns a monotonic sequence and appends complete transaction frames.
Multiple admitted transactions may share one durability barrier while retaining
their individual transaction boundaries.

Commit completion is ordered as follows:

1. Append transaction frames and a group commit record covering their sequence
   range and byte extent.
2. Flush the buffer and sync the same held descriptor. Persist newly created
   directory entries when required by the supported filesystem contract.
3. Publish the committed offset to readers and complete successful waiters.

Admission, callback completion, reader notification, and remote export are not
durable acknowledgments. Existing admission-style batching must not silently
claim the stronger contract; an explicit durable completion API or documented
API transition is required.

Before admission, cancellation means no transaction was accepted. After
admission, cancellation cannot promise rollback: return a transaction identity
and an unresolved outcome, or continue waiting for its definitive completion.
An I/O error may leave a transaction on disk even when its acknowledgment is
lost. Failures must reach all affected waiters, stop further acknowledgments,
and preserve the identity needed to resolve the result after recovery. Do not
silently retry callbacks or report an uncertain commit as rolled back.

## Format and Recovery

Use a versioned journal format with transaction identity, payload length,
integrity checks, and an explicit committed group boundary. Validate lengths
before allocating. The exact encoding needs maintainer agreement; raw Enkodo
block concatenation alone cannot represent these boundaries.

Recovery acquires producer ownership first and validates ordered frames and
commit records. Complete committed groups replay once in sequence. A proven
incomplete tail may be discarded. Interior corruption or an invalid committed
group must fail explicitly, not skip records or guess a new boundary.

The committed cursor is derived from validated journal bytes on reopen. Do not
introduce a separately authoritative sidecar cursor. A complete group may
survive without its caller receiving success; transaction identity makes that
outcome discoverable without repeating its effects.

Shutdown stops admission, resolves accepted work, joins writer/export activity,
and reports flush, sync, and close failures before releasing ownership. Rotation
seals a committed generation and durably establishes the next one without
overlapping writer ownership or invalidating readers of the sealed generation.

## Read Concurrency and Compatibility

Readers have independent offsets and consume immutable committed extents.
They must not hold the writer's mutex while decoding or invoking consumer code.
Visibility is limited to the committed offset, including when the underlying
file already contains bytes from the next group. Rotation and retention must
respect active readers.

Keep active journal generations invisible to legacy chunk watchers. Preserve
legacy `Reader`, `Consumer`, `Source`, and snapshot behavior by sealing committed
transactions into compatible export chunks. Those chunks are reproducible
transport artifacts, not another write authority. Their identities must be
stable across retries so interrupted export cannot create duplicate history.

Reclaim journal generations only after the configured retention/recovery
contract is satisfied. A snapshot must name an exact committed sequence and be
durable before history required to restore it is removed. Local-only retention
must not treat the absence of a remote source as proof of export. Reader leases,
snapshot references, and export progress cannot silently authorize data loss.

The first implementation should be opt-in with an explicit format/version
boundary. Existing stores must open without automatic rewriting. Unsupported
downgrades must refuse explicitly. Conversion requires a verified snapshot and
replay path; it is not accomplished by changing a dependency pin.

## Streambuf and Backend Integration

`streambuf` v0.8.0 holds file descriptors, but its public API has no `Sync`,
`Flush`, or caller-owned-file constructor. Waking its readers does not prove
durability. Preferred prerequisite: a maintainer-approved durability operation
on its held descriptor. If that abstraction is not intended, Kiroku can own
`os.File` and `bufio.Writer` directly. Do not reopen a pathname to sync a file
whose generation may have changed, or maintain both writer implementations.

There is also a cross-library commit decision. Mojura v0.23.3 runs the Kiroku
transaction inside the backend transaction callback, so history publication
precedes the outer backend commit result. Kiroku alone cannot observe a later
backend failure.

Proposed direction: the journal is the durable commit authority; Mojura applies
committed transactions and the corresponding applied sequence in one backend
transaction. Backend failure after journal commit is recoverable pending work,
not a journal rollback. Success from a synchronous database API must wait for
the promised durable commit and read visibility. Replay must not duplicate
effects. This requires an explicit Mojura contract and integration tests before
any application removes its existing recovery path. If the backend remains the
authority instead, its atomic commit must provide the recovery handoff; simply
swapping the order of two independent commits is insufficient.

## Required Proof

- Hold the sync barrier during concurrent submissions: no durable waiter or
  committed reader advances early; multiple transactions share one barrier.
- Inject callback, encode, partial-write, flush, sync, directory-sync, and close
  failures. Assert each caller's outcome and the exact recovered transaction set.
- Kill an owned child process at frame, commit, rotation, and shutdown
  boundaries. Reopen and verify every acknowledged transaction, ordering, and
  uncertain-outcome resolution. Include partial tails and interior corruption.
- Exercise live-owner exclusion, crash/reopen, concurrent readers, slow readers,
  cancellation, and shutdown under the race detector.
- Run existing chunk, source, snapshot, and consumer fixtures unchanged; prove
  export retry identity and snapshot-based retention across restart.
- Inject a real backend rollback after journal completion. Verify database
  acknowledgment semantics, applied-sequence atomicity, and restart convergence.

A process-kill test alone is not power-loss proof: the operating system's page
cache survives the process. Sync ordering/error tests and documented platform
filesystem guarantees are also required.

Measure the same payloads and durability semantics before and after: throughput,
latency distribution, allocations, peak resident memory, file operations, syncs
per transaction, and concurrent-reader latency. Include single-call and grouped
loads, slow storage, rotation, and restart. Report exact commits, host,
filesystem, toolchain, commands, and raw results. Batch thresholds should follow
those measurements; no claimed optimum or fixed throughput target is assumed.

## Maintainer Decisions Before Implementation

1. Approve the durable completion and ambiguous-outcome contract, including how
   it coexists with the current batching API.
2. Select the writer primitive and journal framing/versioning approach.
3. Agree on legacy export identities, snapshot retention, and supported
   filesystem durability guarantees.
4. Agree with Mojura on commit authority and backend recovery integration.

After those decisions, implement and prove the journal in Kiroku, then prove the
Mojura integration, release versioned dependencies, and migrate consumers.
