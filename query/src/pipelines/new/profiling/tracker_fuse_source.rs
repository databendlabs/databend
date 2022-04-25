use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Instant;

use common_exception::Result;

use crate::pipelines::new::profiling::profiling_processor::FuseTableProcessInfo;
use crate::pipelines::new::profiling::profiling_processor::ProcessInfo;

pub struct FuseSourceTracker {
    pub s3_elapse_ns: AtomicUsize,
    pub read_data_block_rows: AtomicUsize,
    pub read_data_block_bytes: AtomicUsize,
    pub read_compressed_bytes: AtomicUsize,
    pub deserialize_elapse_ns: AtomicUsize,

    s3_instant: Instant,
    deserialize_duration: Instant,
}

impl FuseSourceTracker {
    pub fn create() -> FuseSourceTracker {
        FuseSourceTracker {
            s3_elapse_ns: AtomicUsize::new(0),
            deserialize_elapse_ns: AtomicUsize::new(0),
            read_compressed_bytes: AtomicUsize::new(0),
            read_data_block_rows: AtomicUsize::new(0),
            read_data_block_bytes: AtomicUsize::new(0),
            s3_instant: Instant::now(),
            deserialize_duration: Instant::now(),
        }
    }

    pub fn restart_deserialize(&mut self) {
        self.deserialize_duration = Instant::now();
    }

    pub fn deserialize(&self, rows: usize, bytes: usize) {
        let elapse_nanos = self.deserialize_duration.elapsed().as_nanos() as usize;
        self.read_data_block_rows.fetch_add(rows, Ordering::Relaxed);
        self.read_data_block_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.deserialize_elapse_ns
            .fetch_add(elapse_nanos, Ordering::Relaxed);
    }

    pub fn restart_s3_download(&mut self) {
        self.s3_instant = Instant::now();
    }

    pub fn s3_download(&self, files_size: usize) {
        let elapse_nanos = self.s3_instant.elapsed().as_nanos() as usize;
        self.s3_elapse_ns.fetch_add(elapse_nanos, Ordering::Relaxed);
        self.read_compressed_bytes
            .fetch_add(files_size, Ordering::Relaxed);
    }

    pub fn fetch_increment(&self, id: usize) -> Result<Box<dyn ProcessInfo>> {
        Ok(FuseTableProcessInfo::create(
            id,
            self.s3_elapse_ns.load(Ordering::Relaxed),
            self.read_data_block_rows.load(Ordering::Relaxed),
            self.read_data_block_bytes.load(Ordering::Relaxed),
            self.read_compressed_bytes.load(Ordering::Relaxed),
            self.deserialize_elapse_ns.load(Ordering::Relaxed),
        ))
    }
}
