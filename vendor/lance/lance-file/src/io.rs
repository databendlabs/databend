// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use futures::{future::BoxFuture, FutureExt};
use lance_encoding::EncodingsIo;
use lance_io::scheduler::FileScheduler;

use super::reader::DEFAULT_READ_CHUNK_SIZE;

#[derive(Debug)]
pub struct LanceEncodingsIo {
    scheduler: FileScheduler,
    /// Size of chunks when reading large pages
    read_chunk_size: u64,
}

impl LanceEncodingsIo {
    pub fn new(scheduler: FileScheduler) -> Self {
        Self {
            scheduler,
            read_chunk_size: DEFAULT_READ_CHUNK_SIZE,
        }
    }

    pub fn with_read_chunk_size(mut self, read_chunk_size: u64) -> Self {
        self.read_chunk_size = read_chunk_size;
        self
    }
}

impl EncodingsIo for LanceEncodingsIo {
    fn submit_request(
        &self,
        ranges: Vec<std::ops::Range<u64>>,
        priority: u64,
    ) -> BoxFuture<'static, lance_core::Result<Vec<bytes::Bytes>>> {
        let mut split_ranges = Vec::new();
        let mut split_indices = Vec::new(); // Track which original range each split came from

        // Split large ranges into smaller chunks
        //
        // TODO: consider read_chunk_size before submitting requests.
        for (idx, range) in ranges.iter().enumerate() {
            let range_size = range.end - range.start;

            if range_size > self.read_chunk_size {
                let num_chunks = range_size.div_ceil(self.read_chunk_size);
                let chunk_size = range_size / num_chunks;

                for i in 0..num_chunks {
                    let start = range.start + i * chunk_size;
                    let end = if i == num_chunks - 1 {
                        range.end // Last chunk gets any remaining bytes
                    } else {
                        start + chunk_size
                    };
                    split_ranges.push(start..end);
                    split_indices.push(idx);
                }
            } else {
                split_ranges.push(range.clone());
                split_indices.push(idx);
            }
        }

        let fut = self.scheduler.submit_request(split_ranges, priority);

        async move {
            let split_results = fut.await?;

            // Fast path: if no splitting occurred, return results directly
            if split_results.len() == ranges.len() {
                return Ok(split_results);
            }

            // Slow path: reassemble split results
            let mut results = vec![Vec::new(); ranges.len()];

            for (split_result, &orig_idx) in split_results.iter().zip(split_indices.iter()) {
                results[orig_idx].push(split_result.clone());
            }

            Ok(results
                .into_iter()
                .map(|chunks| {
                    if chunks.len() == 1 {
                        chunks.into_iter().next().unwrap()
                    } else {
                        // Concatenate multiple chunks
                        let total_size: usize = chunks.iter().map(|c| c.len()).sum();
                        let mut combined = Vec::with_capacity(total_size);
                        for chunk in chunks {
                            combined.extend_from_slice(&chunk);
                        }
                        bytes::Bytes::from(combined)
                    }
                })
                .collect())
        }
        .boxed()
    }
}
