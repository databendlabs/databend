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

use std::any::Any;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::plan::split_row_id;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;

use super::fetch::ParquetRowsFetcher;
use super::fetch::RowsFetchMetadata;
use super::meta::RowFetchBatch;
use super::meta::RowFetchBlockTask;
use super::meta::RowFetchTask;

#[derive(Clone, Copy)]
pub struct BlockThreshold {
    max_rows: usize,
    max_bytes: usize,
    cur_rows: usize,
    cur_bytes: usize,
}

impl BlockThreshold {
    pub(super) fn new(max_rows: usize, max_bytes: usize) -> Self {
        BlockThreshold {
            max_rows,
            max_bytes,
            cur_rows: 0,
            cur_bytes: 0,
        }
    }

    pub fn acc(&mut self, rows: usize, bytes: usize) -> bool {
        self.cur_rows += rows;
        self.cur_bytes += bytes;
        self.check_threshold()
    }

    fn check_threshold(&self) -> bool {
        self.cur_rows >= self.max_rows || self.cur_bytes >= self.max_bytes
    }

    fn reset(&mut self) {
        self.cur_rows = 0;
        self.cur_bytes = 0;
    }
}

/// Batches the input rows by their size with the fetched columns, and splits each
/// batch into a task per block its row ids are in. Async for the block metadata.
pub struct TransformRowFetchPlan {
    fetcher: ParquetRowsFetcher,

    initialize: bool,
    finished: bool,
    row_id_col_offset: usize,
    fetch_row_ids: Vec<u64>,
    blocks: Vec<DataBlock>,
    next_batch_id: u64,

    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    messages: VecDeque<DataBlock>,

    block_threshold: BlockThreshold,
    metadata: HashMap<u64, Arc<RowsFetchMetadata>>,
}

#[async_trait::async_trait]
impl Processor for TransformRowFetchPlan {
    fn name(&self) -> String {
        String::from("TransformRowFetchPlan")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output_port.is_finished() {
            self.input_port.finish();
            return Ok(Event::Finished);
        }

        if !self.output_port.can_push() {
            self.input_port.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(message) = self.messages.pop_front() {
            self.output_port.push_data(Ok(message));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Async);
        }

        if self.input_port.has_data() {
            self.input_data = Some(self.input_port.pull_data().unwrap()?);
            return Ok(Event::Async);
        }

        if self.input_port.is_finished() {
            if !self.finished {
                self.finished = true;
                return Ok(Event::Async);
            }

            self.output_port.finish();
            return Ok(Event::Finished);
        }

        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }

    async fn async_process(&mut self) -> Result<()> {
        if !self.initialize {
            self.initialize = true;
            self.fetcher.initialize().await?;
        }

        if let Some(data) = self.input_data.take() {
            let num_rows = data.num_rows();
            let fetched_columns_bytes = data.memory_size() / data.num_rows();
            let entry = &data.columns()[self.row_id_col_offset];
            let mut consumed_len = num_rows;
            match entry.data_type() {
                DataType::Number(NumberDataType::UInt64) => {
                    let row_id_column = self.get_row_id_column(&data);
                    // Process the row id column in block batch
                    // Ensure that the same block would be processed in the same batch and threads
                    for (idx, row_id) in row_id_column.iter().enumerate() {
                        let (prefix, _) = split_row_id(*row_id);

                        if !self.metadata.contains_key(&prefix) {
                            let metadata = self.fetcher.fetch_metadata(prefix).await?;
                            self.metadata.insert(prefix, metadata);
                        }

                        let fetch_columns_bytes = self.metadata[&prefix].row_bytes;
                        let bytes = fetched_columns_bytes + fetch_columns_bytes;
                        let reach_threshold = self.block_threshold.acc(1, bytes);
                        self.fetch_row_ids.push(*row_id);
                        if reach_threshold {
                            consumed_len = idx + 1;
                            break;
                        }
                    }

                    self.blocks.push(data.slice(0..consumed_len));
                }
                DataType::Nullable(inner)
                    if matches!(inner.as_ref(), DataType::Number(NumberDataType::UInt64)) =>
                {
                    let column = entry.to_column();
                    let value = column.into_nullable().unwrap();
                    let row_id_column = value.column.into_number().unwrap().into_u_int64().unwrap();

                    for (idx, (row_id, is_valid)) in
                        row_id_column.iter().zip(value.validity.iter()).enumerate()
                    {
                        if !is_valid {
                            if self.block_threshold.acc(1, fetched_columns_bytes) {
                                consumed_len = idx + 1;
                                break;
                            }
                            continue;
                        }

                        let (prefix, _) = split_row_id(*row_id);

                        if !self.metadata.contains_key(&prefix) {
                            let metadata = self.fetcher.fetch_metadata(prefix).await?;
                            self.metadata.insert(prefix, metadata);
                        }

                        let fetch_columns_bytes = self.metadata[&prefix].row_bytes;
                        let bytes = fetched_columns_bytes + fetch_columns_bytes;
                        let reach_threshold = self.block_threshold.acc(1, bytes);
                        self.fetch_row_ids.push(*row_id);
                        if reach_threshold {
                            consumed_len = idx + 1;
                            break;
                        }
                    }

                    self.blocks.push(data.slice(0..consumed_len));
                }
                _ => unreachable!("Row id column should be UInt64 or Nullable(UInt64)"),
            };

            let remain_rows = data.slice(consumed_len..num_rows);

            if !remain_rows.is_empty() {
                self.input_data = Some(remain_rows);
            }
        }

        if !self.finished && !self.block_threshold.check_threshold() {
            return Ok(());
        }

        self.block_threshold.reset();
        self.plan_batch();

        assert!(self.blocks.is_empty());
        assert!(self.metadata.is_empty());

        Ok(())
    }
}

impl TransformRowFetchPlan {
    fn get_row_id_column(&self, data: &DataBlock) -> Buffer<u64> {
        let entry = &data.columns()[self.row_id_col_offset];
        let column = entry.to_column();
        if matches!(entry.data_type(), DataType::Number(NumberDataType::UInt64)) {
            column.into_number().unwrap().into_u_int64().unwrap()
        } else {
            // From merge into matched data, the row id column is nullable but has no null value.
            let value = *column.into_nullable().unwrap();
            debug_assert!(value.validity.null_count() == 0);
            value.column.into_number().unwrap().into_u_int64().unwrap()
        }
    }

    pub(super) fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        row_id_col_offset: usize,
        fetcher: ParquetRowsFetcher,
        block_threshold: BlockThreshold,
    ) -> Box<dyn Processor> {
        Box::new(TransformRowFetchPlan {
            fetcher,
            block_threshold,
            row_id_col_offset,

            input_port: input,
            output_port: output,
            blocks: vec![],
            input_data: None,
            messages: VecDeque::new(),
            fetch_row_ids: vec![],
            metadata: HashMap::new(),
            next_batch_id: 0,
            finished: false,
            initialize: false,
        })
    }

    /// Queues the batch accumulated so far, and a task per block of its row ids.
    fn plan_batch(&mut self) {
        let inputs = std::mem::take(&mut self.blocks);
        let row_ids = std::mem::take(&mut self.fetch_row_ids);
        let metadata = std::mem::take(&mut self.metadata);

        if inputs.iter().all(|block| block.num_rows() == 0) {
            return;
        }

        let parts = metadata
            .keys()
            .enumerate()
            .map(|(part, block_id)| (*block_id, part as u32))
            .collect::<HashMap<_, _>>();

        // The rows to take from each block, and where each row of the batch is.
        let mut take_indices = HashMap::with_capacity(metadata.len());
        let mut indices = Vec::with_capacity(row_ids.len());
        for row_id in row_ids {
            let (block_id, row) = split_row_id(row_id);
            match take_indices.entry(block_id) {
                Entry::Occupied(mut rows) => {
                    let rows: &mut Vec<_> = rows.get_mut();
                    indices.push((parts[&block_id], rows.len() as u32));
                    rows.push(row as u32);
                }
                Entry::Vacant(rows) => {
                    indices.push((parts[&block_id], 0));
                    rows.insert(vec![row as u32]);
                }
            }
        }

        let batch = Arc::new(RowFetchBatch {
            id: self.next_batch_id,
            inputs,
            indices,
            parts: take_indices.len(),
            start: Instant::now(),
        });
        self.next_batch_id += 1;

        if take_indices.is_empty() {
            self.messages
                .push_back(DataBlock::empty_with_meta(Box::new(RowFetchTask {
                    batch,
                    block: None,
                })));
            return;
        }
        for (block_id, take_indices) in take_indices {
            self.messages
                .push_back(DataBlock::empty_with_meta(Box::new(RowFetchTask {
                    batch: batch.clone(),
                    block: Some(RowFetchBlockTask {
                        part: parts[&block_id],
                        metadata: metadata[&block_id].clone(),
                        take_indices,
                    }),
                })));
        }
    }
}
