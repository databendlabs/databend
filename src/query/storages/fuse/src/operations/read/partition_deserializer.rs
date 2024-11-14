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
use std::collections::VecDeque;
use std::sync::Arc;

use async_channel::Sender;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::gen_mutation_stream_meta;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::InternalColumnMeta;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use xorf::BinaryFuse16;

use super::runtime_filter_prunner::update_bitmap_with_bloom_filter;
use crate::fuse_part::FuseBlockPartInfo;
use crate::operations::read::PartitionScanMeta;
use crate::operations::read::PartitionScanState;
use crate::operations::read::ReaderState;
use crate::operations::read::SourceBlockReader;
use crate::operations::read::SourceReader;

pub struct PartitionDeserializer {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data_blocks: VecDeque<DataBlock>,
    partition_scan_meta: Option<PartitionScanMeta>,
    partition_scan_state: Arc<PartitionScanState>,
    base_block_ids: Option<Scalar>,
    next_partition_id: usize,
    task_sender: Sender<Arc<DataBlock>>,
    scan_progress: Arc<Progress>,
}

impl PartitionDeserializer {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        plan: &DataSourcePlan,
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        partition_scan_state: Arc<PartitionScanState>,
        task_sender: Sender<Arc<DataBlock>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(PartitionDeserializer {
            input,
            output,
            output_data_blocks: VecDeque::new(),
            partition_scan_meta: None,
            partition_scan_state,
            next_partition_id: 0,
            task_sender,
            base_block_ids: plan.base_block_ids.clone(),
            scan_progress,
        })))
    }
}

impl PartitionDeserializer {
    fn deserialize(
        &self,
        partition_id: usize,
        meta: &mut PartitionScanMeta,
        source_block_reader: &SourceBlockReader,
    ) -> Result<DataBlock> {
        let partition = &meta.partitions[partition_id];
        let part = FuseBlockPartInfo::from_part(partition)?;
        let io_result = meta.io_results.pop_front().unwrap();
        let columns_chunks = io_result.columns_chunks()?;
        match source_block_reader {
            SourceBlockReader::Parquet {
                block_reader,
                bloom_filter,
            } => {
                let data_block = block_reader.deserialize_parquet_chunks(
                    part.nums_rows,
                    &part.columns_meta,
                    columns_chunks,
                    &part.compression,
                    &part.location,
                )?;

                if let Some(bloom_filter) = bloom_filter {
                    let bitmap = self.runtime_filter(&data_block, bloom_filter)?;
                    if bitmap.unset_bits() == bitmap.len() {
                        Ok(DataBlock::empty())
                    } else {
                        if bitmap.unset_bits() != 0 {
                            meta.bitmaps[partition_id] = if let Some(partition_bitmap) =
                                meta.bitmaps[partition_id].as_ref()
                            {
                                Some((partition_bitmap) & (&bitmap))
                            } else {
                                Some(bitmap)
                            };
                        }
                        Ok(data_block)
                    }
                } else {
                    Ok(data_block)
                }
            }
        }
    }

    fn reorder_columns(columns: Vec<BlockEntry>, column_positions: &[usize]) -> Vec<BlockEntry> {
        let mut columns_with_position = Vec::with_capacity(columns.len());
        for (column, position) in columns.into_iter().zip(column_positions.iter()) {
            columns_with_position.push((column, position));
        }
        columns_with_position.sort_by(|a, b| a.1.cmp(b.1));

        columns_with_position
            .into_iter()
            .map(|(column, _)| column.clone())
            .collect()
    }

    pub(crate) fn construct_data_blocks(
        &mut self,
        mut meta: PartitionScanMeta,
    ) -> Result<Vec<DataBlock>> {
        let mut data_blocks = Vec::with_capacity(meta.partitions.len());
        let partitions = std::mem::take(&mut meta.partitions);
        let partition_columns = std::mem::take(&mut meta.columns);
        let bitmap = std::mem::take(&mut meta.bitmaps);
        for (partition, ((mut columns, num_rows), bitmap)) in partitions.into_iter().zip(
            partition_columns
                .into_iter()
                .zip(meta.num_rows.iter())
                .zip(bitmap.into_iter()),
        ) {
            let part = FuseBlockPartInfo::from_part(&partition)?;
            let data_block = match &meta.source_reader {
                SourceReader::Parquet {
                    block_reader,
                    filter_readers,
                    column_positions,
                    ..
                } => {
                    let mut meta: Option<BlockMetaInfoPtr> = if block_reader.update_stream_columns()
                    {
                        // Fill `BlockMetaInfoPtr` if update stream columns
                        let stream_meta = gen_mutation_stream_meta(None, &part.location)?;
                        Some(Box::new(stream_meta))
                    } else {
                        None
                    };

                    if block_reader.query_internal_columns() {
                        // Fill `BlockMetaInfoPtr` if query internal columns
                        let block_meta = part.block_meta_index().unwrap();
                        let internal_column_meta = InternalColumnMeta {
                            segment_idx: block_meta.segment_idx,
                            block_id: block_meta.block_id,
                            block_location: block_meta.block_location.clone(),
                            segment_location: block_meta.segment_location.clone(),
                            snapshot_location: block_meta.snapshot_location.clone(),
                            offsets: None,
                            base_block_ids: self.base_block_ids.clone(),
                            inner: meta,
                            matched_rows: block_meta.matched_rows.clone(),
                        };
                        meta = Some(Box::new(internal_column_meta));
                    }

                    if !filter_readers.is_empty() {
                        columns = Self::reorder_columns(columns, column_positions);
                    }
                    let mut data_block = DataBlock::new_with_meta(columns, *num_rows, meta);
                    if let Some(bitmap) = bitmap {
                        data_block = data_block.filter_with_bitmap(&bitmap)?;
                    }

                    self.record_scan_statistics(data_block.num_rows(), data_block.memory_size());

                    data_block
                }
            };

            data_blocks.push(data_block);
        }
        Ok(data_blocks)
    }

    fn runtime_filter(
        &self,
        data_block: &DataBlock,
        bloom_filter: &Arc<BinaryFuse16>,
    ) -> Result<Bitmap> {
        let mut bitmap = MutableBitmap::from_len_zeroed(data_block.num_rows());
        let probe_block_entry = data_block.get_by_offset(0);
        let probe_column = probe_block_entry
            .value
            .convert_to_full_column(&probe_block_entry.data_type, data_block.num_rows());
        update_bitmap_with_bloom_filter(probe_column, bloom_filter, &mut bitmap)?;
        Ok(bitmap.into())
    }

    fn prune_partitions(
        &mut self,
        mut meta: PartitionScanMeta,
        partition_id: usize,
    ) -> PartitionScanMeta {
        meta.partitions.remove(partition_id);
        meta.columns.remove(partition_id);
        meta.num_rows.remove(partition_id);
        meta.bitmaps.remove(partition_id);
        meta
    }

    fn record_scan_statistics(&self, num_rows: usize, bytes: usize) {
        // Record scan statistics.
        let progress_values = ProgressValues {
            rows: num_rows,
            bytes,
        };
        self.scan_progress.incr(&progress_values);
        Profile::record_usize_profile(ProfileStatisticsName::ScanBytes, bytes);
    }
}

#[async_trait::async_trait]
impl Processor for PartitionDeserializer {
    fn name(&self) -> String {
        String::from("PartitionDeserializer")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data_blocks.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.partition_scan_meta.is_some() {
            if !self.input.has_data() {
                self.input.set_need_data();
            }
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(meta) = data_block.take_meta() {
                if let Some(source_meta) = PartitionScanMeta::downcast_from(meta) {
                    self.partition_scan_meta = Some(source_meta);
                    return Ok(Event::Sync);
                }
            }
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let Some(mut meta) = self.partition_scan_meta.take() else {
            return Ok(());
        };

        let partition_id = self.next_partition_id;
        let source_block_reader = meta.source_reader.source_block_reader(&meta.reader_state);
        let data_block = self.deserialize(partition_id, &mut meta, &source_block_reader)?;

        if data_block.is_empty() {
            meta = self.prune_partitions(meta, partition_id);
            self.partition_scan_state.inc_num_deserialized_partitions(1);
        } else {
            meta.num_rows[partition_id] = data_block.num_rows();
            meta.columns[partition_id].extend(data_block.into_columns());
            self.next_partition_id += 1;
        }

        if self.next_partition_id >= meta.partitions.len() {
            self.next_partition_id = 0;
            let is_partitions_finished = matches!(
                meta.reader_state.next_reader_state(&meta.source_reader),
                ReaderState::Finish
            );
            if !is_partitions_finished && !meta.partitions.is_empty() {
                let data_block = Arc::new(DataBlock::empty_with_meta(Box::new(meta)));
                self.task_sender
                    .send_blocking(data_block)
                    .map_err(|_| ErrorCode::Internal("channel is closed"))?;
            } else {
                let num_partitions = meta.partitions.len();
                let data_blocks = self.construct_data_blocks(meta)?;
                self.output_data_blocks.extend(data_blocks);
                if self.partition_scan_state.finished(num_partitions) {
                    self.task_sender.close();
                }
            }
        } else {
            self.partition_scan_meta = Some(meta);
        }

        Ok(())
    }
}
