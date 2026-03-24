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

    pub fn reset(&mut self) {
        self.processed_rows = 0;
        self.processed_bytes = 0;
        self.first_block_start = None;
        self.start = Instant::now();
    }

    pub fn log_finish_statistics(&mut self, hashtable: &AggregateHashTable) {
        self.log_finish(
            hashtable.payload.len(),
            hashtable.hash_index_resize_count(),
            None,
        );
    }

    pub fn log_task_finish_statistics(
        &mut self,
        task_id: u64,
        processor_id: usize,
        spill_depth: usize,
        output_rows: usize,
        hash_index_resizes: usize,
        spilled: bool,
    ) {
        self.log_finish(
            output_rows,
            hash_index_resizes,
            Some((task_id, processor_id, spill_depth, spilled)),
        );
    }

    fn log_finish(
        &mut self,
        output_rows: usize,
        hash_index_resizes: usize,
        task: Option<(u64, usize, usize, bool)>,
    ) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let real_elapsed = self
            .first_block_start
            .as_ref()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(elapsed);

        match task {
            Some((task_id, processor_id, spill_depth, spilled)) => {
                log::info!(
                    "[{}] Task completed: task_id={}, processor={}, spill_depth={}, spilled={}, {} → {} rows in {:.2}s (real: {:.2}s), throughput: {} rows/sec, {}/sec, total: {}, hash index resizes: {}",
                    self.stage,
                    task_id,
                    processor_id,
                    spill_depth,
                    spilled,
                    self.processed_rows,
                    output_rows,
                    elapsed,
                    real_elapsed,
                    convert_number_size(self.processed_rows as f64 / elapsed),
                    convert_byte_size(self.processed_bytes as f64 / elapsed),
                    convert_byte_size(self.processed_bytes as f64),
                    hash_index_resizes,
                );
            }
            None => {
                log::info!(
                    "[{}] Aggregation completed: {} → {} rows in {:.2}s (real: {:.2}s), throughput: {} rows/sec, {}/sec, total: {}, hash index resizes: {}",
                    self.stage,
                    self.processed_rows,
                    output_rows,
                    elapsed,
                    real_elapsed,
                    convert_number_size(self.processed_rows as f64 / elapsed),
                    convert_byte_size(self.processed_bytes as f64 / elapsed),
                    convert_byte_size(self.processed_bytes as f64),
                    hash_index_resizes,
                );
            }
        }

        self.reset();
    }
}
