// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Instant;

use databend_common_base::base::convert_byte_size;
use databend_common_base::base::convert_number_size;
use databend_common_expression::AggregateHashTable;

pub struct AggregationStatistics {
    stage: &'static str,
    start: Instant,
    first_block_start: Option<Instant>,
    processed_bytes: usize,
    processed_rows: usize,
}

impl AggregationStatistics {
    pub fn new(stage: &'static str) -> Self {
        Self {
            stage,
            start: Instant::now(),
            first_block_start: None,
            processed_bytes: 0,
            processed_rows: 0,
        }
    }

    pub fn record_block(&mut self, rows: usize, bytes: usize) {
        self.processed_rows += rows;
        self.processed_bytes += bytes;
        if self.first_block_start.is_none() {
            self.first_block_start = Some(Instant::now());
        }
    }

    pub fn log_finish_statistics(&mut self, hashtable: &AggregateHashTable) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let real_elapsed = self
            .first_block_start
            .as_ref()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(elapsed);

        log::info!(
            "[TRANSFORM-AGGREGATOR][{}] Aggregation completed: {} → {} rows in {:.2}s (real: {:.2}s), throughput: {} rows/sec, {}/sec, total: {}, hash index resizes: {}",
            self.stage,
            self.processed_rows,
            hashtable.payload.len(),
            elapsed,
            real_elapsed,
            convert_number_size(self.processed_rows as f64 / elapsed),
            convert_byte_size(self.processed_bytes as f64 / elapsed),
            convert_byte_size(self.processed_bytes as f64),
            hashtable.hash_index_resize_count(),
        );

        self.processed_rows = 0;
        self.processed_bytes = 0;
        self.first_block_start = None;
        self.start = Instant::now();
    }
}
