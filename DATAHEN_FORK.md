# DataHen fork of Apache iceberg-rust

This repository is a fork of [`apache/iceberg-rust`](https://github.com/apache/iceberg-rust), maintained by DataHen.
It is **not** an Apache Software Foundation release and is not endorsed by the ASF. The upstream `LICENSE` and
`NOTICE` files are kept unchanged and continue to apply; everything here is under the Apache License, Version 2.0.

**It is a generic Apache Iceberg implementation.** Nothing in it is specific to any DataHen product: every
change is Iceberg-specification behaviour that mirrors the Java reference implementation or the REST catalog
specification.

## What this fork is

```text
apache/iceberg-rust  main @ 776648bee4c52ece8a057cfcf0d7756416299ba8   (2026-10-08)
  + seven change-sets taken from unmerged apache/iceberg-rust pull requests, with their authors' commits kept
  + four change-sets written by DataHen
```

## Changes relative to the upstream base

Taken from upstream pull requests that were not merged at the base commit (original commits preserved in history;
merge commits record the conflict resolutions and compile fixes made here):

| pull request | what it adds |
|---|---|
| apache/iceberg-rust#3253 | the snapshot producer reports removed files and rewrites a manifest under its own schema |
| apache/iceberg-rust#3300 | `OverwriteAction` |
| apache/iceberg-rust#3046 | `MergingSnapshotProducer`, `RewriteFilesAction` |
| apache/iceberg-rust#3232 | `DeleteVector::serialize` |
| apache/iceberg-rust#3038 | `unreferenced_files` for expire-snapshots |
| apache/iceberg-rust#2709 | `ManageSnapshotsAction` |
| apache/iceberg-rust#2543 | `RewriteManifestsAction` |

Written by DataHen (commits prefixed `datahen(carry):`):

| change | files |
|---|---|
| delete files in the snapshot producer | `crates/iceberg/src/transaction/snapshot.rs` (modified) |
| deletion-vector file writer and reader | `crates/iceberg/src/writer/deletion_vector.rs` (new), `crates/iceberg/src/writer/mod.rs` (modified) |
| `RowDeltaAction`, `Transaction::replace_files()` | `crates/iceberg/src/transaction/row_delta.rs` (new), `crates/iceberg/src/transaction/mod.rs` (modified) |
| exact-base commit, `Transaction::with_exact_base()` | `crates/iceberg/src/transaction/mod.rs` (modified) |

Files modified by DataHen carry a `// Modified by DataHen` line under their licence header; files written by
DataHen carry a DataHen copyright header. Upstream-PR files modified here only to resolve conflicts or follow an
upstream API change are identified by the merge and `stack:` commits in history.

## Versioning

Tags are `datahen-v<upstream crate version>-dh.<n>`. Each tag's message records the Apache base commit and the
fork-delta ledger version. Crate names and versions are not changed.

## Updating from upstream

This fork does not follow upstream `main` automatically. It is advanced only by the controlled procedure in the
fork-delta ledger kept by its maintainers: fetch upstream, check whether upstream now provides a carried
capability, drop a carry that upstream replaces and that passes the conformance tests, re-apply the rest, and run
the complete test suite and the Java ↔ Rust interoperability tests before any dependent pins the new revision.
