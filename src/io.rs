//! The storage engine's file layer, with one change: on Linux, syncing a
//! database or its WAL is `fdatasync`, not `fsync`, as SQLite itself does
//! there. Everything else goes to the engine's own platform layer.
//!
//! A commit with `synchronous = FULL` syncs the WAL. `fsync` also writes the
//! file's metadata, its modification time included, which on ext4 is a
//! second journal flush; `fdatasync` writes only what reading the data back
//! needs, the size included when it changed. With the WAL reused in place
//! (checkpoints restart it instead of truncating it, see
//! `SyncMethod::DataSync`), a commit's sync is one flush. Measured on EBS
//! gp3 with ext4: 3.05 ms per append + `fsync`, 0.94 ms per overwrite +
//! `fdatasync` (BENCHMARKS.md, "The path apps use").

use std::ptr::NonNull;
use std::sync::Arc;

use turso_core::io::clock::{MonotonicInstant, WallClockInstant};
use turso_core::io::{Clock, Completion, File, FileId, FileSyncType, MemoryIO, OpenFlags, SharedWalLockKind, SharedWalMappedRegion, IO};
use turso_core::{Buffer, Result};

/// How a commit reaches the disk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SyncMethod {
    /// On Linux, `fdatasync`, and checkpoints that restart the WAL in place,
    /// so commits overwrite it instead of growing it. The default.
    #[default]
    DataSync,
    /// The engine's own `fsync`, and checkpoints that truncate the WAL: how
    /// the server synced before. For comparing the two, or a filesystem
    /// whose `fdatasync` cannot be trusted.
    Fsync,
}

/// The engine's platform layer, with syncs as `SyncMethod::DataSync` makes them.
pub struct DataSyncIo {
    inner: Arc<dyn IO>,
}

impl DataSyncIo {
    pub fn new(inner: Arc<dyn IO>) -> Self {
        Self { inner }
    }
}

impl Clock for DataSyncIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for DataSyncIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(DataSyncFile::new(inner, path)?))
    }

    fn open_shared_wal_file(&self, path: &str) -> Result<Arc<dyn File>> {
        self.inner.open_shared_wal_file(path)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        self.inner.supports_shared_wal_coordination()
    }

    fn step(&self) -> Result<()> {
        self.inner.step()
    }

    fn cancel(&self, c: &[Completion]) -> Result<()> {
        self.inner.cancel(c)
    }

    fn drain_completions(&self, completions: &[Completion]) -> Result<()> {
        self.inner.drain_completions(completions)
    }

    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        self.inner.wait_for_completion(c)
    }

    fn generate_random_number(&self) -> i64 {
        self.inner.generate_random_number()
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.inner.fill_bytes(dest)
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        self.inner.get_memory_io()
    }

    fn register_fixed_buffer(&self, ptr: NonNull<u8>, len: usize) -> Result<u32> {
        self.inner.register_fixed_buffer(ptr, len)
    }

    fn yield_now(&self) {
        self.inner.yield_now()
    }

    fn sleep(&self, duration: std::time::Duration) {
        self.inner.sleep(duration)
    }

    fn file_id(&self, path: &str) -> Result<FileId> {
        self.inner.file_id(path)
    }
}

/// One of the engine's files, synced through a descriptor of our own: a
/// sync acts on the file, not the descriptor, and the engine's writes
/// complete before it asks for one (its platform layer is synchronous).
struct DataSyncFile {
    inner: Arc<dyn File>,
    #[cfg(target_os = "linux")]
    sync: std::fs::File,
}

impl DataSyncFile {
    fn new(inner: Arc<dyn File>, path: &str) -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let sync = std::fs::File::open(path).map_err(|err| turso_core::io_error(err, "open"))?;
            Ok(Self { inner, sync })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Ok(Self { inner })
        }
    }
}

impl File for DataSyncFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.inner.pread(pos, c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.inner.pwrite(pos, buffer, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        #[cfg(target_os = "linux")]
        {
            let _ = sync_type;
            self.sync.sync_data().map_err(|err| turso_core::io_error(err, "fdatasync"))?;
            c.complete(0);
            Ok(c)
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.inner.sync(c, sync_type)
        }
    }

    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        self.inner.pwritev(pos, buffers, c)
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.inner.truncate(len, c)
    }

    fn has_hole(&self, pos: usize, len: usize) -> Result<bool> {
        self.inner.has_hole(pos, len)
    }

    fn punch_hole(&self, pos: usize, len: usize) -> Result<()> {
        self.inner.punch_hole(pos, len)
    }

    fn shared_wal_lock_byte(&self, offset: u64, exclusive: bool, kind: SharedWalLockKind) -> Result<()> {
        self.inner.shared_wal_lock_byte(offset, exclusive, kind)
    }

    fn shared_wal_try_lock_byte(&self, offset: u64, exclusive: bool, kind: SharedWalLockKind) -> Result<bool> {
        self.inner.shared_wal_try_lock_byte(offset, exclusive, kind)
    }

    fn shared_wal_probe_exclusive_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<bool> {
        self.inner.shared_wal_probe_exclusive_byte(offset, kind)
    }

    fn shared_wal_probe_exclusive_while_shared_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<bool> {
        self.inner.shared_wal_probe_exclusive_while_shared_byte(offset, kind)
    }

    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        self.inner.shared_wal_unlock_byte(offset, kind)
    }

    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        self.inner.shared_wal_set_len(len)
    }

    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        self.inner.shared_wal_map(offset, len)
    }
}
