use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use petgraph::stable_graph::NodeIndex;
use crate::pipelines::new::ProcessorProfiling;
use common_exception::Result;

pub struct ProcessorTracker {
    sync_elapse_ms: AtomicU64,
    async_elapse_ms: AtomicU64,
    after_process_rows: AtomicUsize,
    after_process_bytes: AtomicUsize,
    before_process_rows: AtomicUsize,
    before_process_bytes: AtomicUsize,
}

impl ProcessorTracker {
    pub fn create() -> ProcessorTracker {
        ProcessorTracker {
            sync_elapse_ms: AtomicU64::new(0),
            async_elapse_ms: AtomicU64::new(0),
            after_process_rows: AtomicUsize::new(0),
            after_process_bytes: AtomicUsize::new(0),
            before_process_rows: AtomicUsize::new(0),
            before_process_bytes: AtomicUsize::new(0),
        }
    }

    pub fn restart_sync(&mut self) {
        //
    }

    pub fn sync_process(&self, before_rows: usize, after_rows: usize, before_bytes: usize, after_bytes: usize) {
        if after_rows > 0 {
            self.after_process_rows.fetch_add(after_rows, Ordering::Relaxed);
        }

        if after_bytes > 0 {
            self.after_process_bytes.fetch_add(after_bytes, Ordering::Relaxed);
        }

        if before_rows > 0 {
            self.before_process_rows.fetch_add(before_rows, Ordering::Relaxed);
        }

        if before_bytes > 0 {
            self.before_process_bytes.fetch_add(before_bytes, Ordering::Relaxed);
        }
        // TODO: sync_elapse_ms
    }

    pub fn restart_async(&mut self) {
        //
    }

    pub fn async_process(&self, before_rows: usize, after_rows: usize, before_bytes: usize, after_bytes: usize) {
        self.after_process_rows.fetch_add(after_rows, Ordering::Relaxed);
        self.after_process_bytes.fetch_add(after_bytes, Ordering::Relaxed);
        self.before_process_rows.fetch_add(before_rows, Ordering::Relaxed);
        self.before_process_bytes.fetch_add(before_bytes, Ordering::Relaxed);
        // TODO: sync_elapse_ms
    }

    pub fn increment(&self, id: usize) -> Result<ProcessorProfiling> {
        let sync_elapse_ms = self.sync_elapse_ms.swap(0, Ordering::Relaxed);
        let async_elapse_ms = self.async_elapse_ms.swap(0, Ordering::Relaxed);
        let after_process_rows = self.after_process_rows.swap(0, Ordering::Relaxed);
        let after_process_bytes = self.after_process_bytes.swap(0, Ordering::Relaxed);
        let before_process_rows = self.before_process_rows.swap(0, Ordering::Relaxed);
        let before_process_bytes = self.before_process_bytes.swap(0, Ordering::Relaxed);

        Ok(ProcessorProfiling::create(
            id,
            sync_elapse_ms,
            async_elapse_ms,
            after_process_rows,
            after_process_bytes,
            before_process_rows,
            before_process_bytes,
        ))
    }
}
