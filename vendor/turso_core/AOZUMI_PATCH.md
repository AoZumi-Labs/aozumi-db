# turso_core 0.8.3, patched for aozumi-db

This is turso_core 0.8.3 from crates.io (MIT, see `LICENSE.md`), used in place
of the published crate through `[patch.crates-io]` in the top-level
`Cargo.toml`. `aozumi.patch` is the whole difference from the published
crate.

## Why

Group commit (`AOZUMI_DB_SYNCHRONOUS=group`, DESIGN.md D33) writes commits
without flushing them, and flushes many at once. The engine makes a commit
visible as soon as it is written, so without this patch a reader could see a
commit that is not yet on disk, and the server would have to hold its answer
until the next flush.

## What

- `ReadCap` (storage/wal.rs, exported as `turso_core::ReadCap`): a frame
  position, `(checkpoint_seq, max_frame)`, that the server sets to the last
  commit it flushed.
- `Connection::set_read_cap(Option<Arc<ReadCap>>)`: a connection with a cap
  starts its read transactions at `min(shared max_frame, cap)`, never below
  `nbackfills` (frames checkpointed into the database file were flushed
  first). A cap from an older WAL generation means only the database file.
- The snapshot is pinned with the usual read marks, at the capped frame, so
  a checkpoint does not copy frames past it into the database file while the
  reader holds it. The shared snapshot is still the one validated after the
  mark is taken, so a concurrent commit or checkpoint retries as before.
- A capped connection's snapshot differs from the shared one, so it cannot
  upgrade to a writer while frames past the cap exist: `begin_write_tx`
  returns `BusySnapshot`, as for any stale reader. The server caps only
  requests that read outside a transaction.
- The multi-process coordination ignores the cap (its default method).
  aozumi-db opens databases in one process only.

Also: the `[[bench]]` targets and `benches/` are removed (they are not in
the package's build), and upstream's own compiler warnings are allowed, as
crates.io builds cap them.

## Updating turso_core

Apply `aozumi.patch` to the new release, check that the read-mark selection
in `InProcessWalCoordination::try_begin_read_tx_capped` still matches the
upstream `try_begin_read_tx`, and run `cargo test` here (src/durability.rs
covers capped readers, checkpoints and the stale-writer refusal).
