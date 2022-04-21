use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use crate::pipelines::new::profiling::profiling_processor::{FuseTableProcessInfo, ProcessInfo};
use common_exception::Result;

pub struct FuseSourceTracker {
    pub s3_elapse_ns: AtomicUsize,
    pub read_data_block_rows: AtomicUsize,
    pub read_data_block_bytes: AtomicUsize,
    pub read_compressed_bytes: AtomicUsize,
    pub deserialize_elapse_ns: AtomicUsize,

    s3_duration: Duration,
    deserialize_duration: Duration,
}

impl FuseSourceTracker {
    pub fn create() -> FuseSourceTracker {
        FuseSourceTracker {
            s3_elapse_ns: AtomicUsize::new(0),
            deserialize_elapse_ns: AtomicUsize::new(0),
            read_compressed_bytes: AtomicUsize::new(0),
            read_data_block_rows: AtomicUsize::new(0),
            read_data_block_bytes: AtomicUsize::new(0),
            s3_duration: Duration::new(0, 0),
            deserialize_duration: Duration::new(0, 0),
        }
    }

    pub fn restart_deserialize(&mut self) {
        self.deserialize_duration = Instant::now().elapsed();
    }

    pub fn deserialize(&self, rows: usize, bytes: usize) {
        let now = Instant::now().elapsed();
        let elapse_nanos = (now - self.s3_duration).as_nanos() as usize;
        self.read_data_block_rows.fetch_add(rows, Ordering::Relaxed);
        self.read_data_block_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.deserialize_elapse_ns.fetch_add(elapse_nanos, Ordering::Relaxed);
    }

    pub fn restart_s3_download(&mut self) {
        self.s3_duration = Instant::now().elapsed();
    }

    pub fn s3_download(&self, files_size: usize) {
        let now = Instant::now().elapsed();
        let elapse_nanos = (now - self.s3_duration).as_nanos() as usize;
        self.s3_elapse_ns.fetch_add(elapse_nanos, Ordering::Relaxed);
        self.read_compressed_bytes.fetch_add(files_size, Ordering::Relaxed);
    }

    pub fn fetch_increment(&self, id: usize) -> Result<Box<dyn ProcessInfo>> {
        Ok(FuseTableProcessInfo::create(
            id,
            self.s3_elapse_ns.swap(0, Ordering::Relaxed),
            self.read_data_block_rows.swap(0, Ordering::Relaxed),
            self.read_data_block_bytes.swap(0, Ordering::Relaxed),
            self.read_compressed_bytes.swap(0, Ordering::Relaxed),
            self.deserialize_elapse_ns.swap(0, Ordering::Relaxed),
        ))
    }
}


