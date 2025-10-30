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
use std::ops::BitAnd;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::runtime_filter_info::RuntimeFilterEntry;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStats;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FieldIndex;
use databend_common_expression::Scalar;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use xorf::BinaryFuse16;

use super::parquet_data_source::ParquetDataSource;
use super::util::add_data_block_meta;
use super::util::need_reserve_block_info;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::VirtualColumnReader;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning::ExprBloomFilter;

pub struct DeserializeDataTransform {
    ctx: Arc<dyn TableContext>,
    scan_id: usize,
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    src_schema: DataSchema,
    output_schema: DataSchema,
    parts: Vec<PartInfoPtr>,
    chunks: Vec<ParquetDataSource>,

    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,

    base_block_ids: Option<Scalar>,
    cached_runtime_filter: Option<Vec<BloomRuntimeFilterRef>>,
    need_reserve_block_info: bool,
}

#[derive(Clone)]
struct BloomRuntimeFilterRef {
    column_index: FieldIndex,
    filter_id: usize,
    filter: BinaryFuse16,
    stats: Arc<RuntimeFilterStats>,
}

unsafe impl Send for DeserializeDataTransform {}

impl DeserializeDataTransform {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();

        let mut src_schema: DataSchema = (block_reader.schema().as_ref()).into();
        if let Some(virtual_reader) = virtual_reader.as_ref() {
            let mut fields = src_schema.fields().clone();
            for virtual_column_field in &virtual_reader.virtual_column_info.virtual_column_fields {
                let field = DataField::new(
                    &virtual_column_field.name,
                    DataType::from(&*virtual_column_field.data_type),
                );
                fields.push(field);
            }
            src_schema = DataSchema::new(fields);
        }

        let mut output_schema = plan.schema().as_ref().clone();
        output_schema.remove_internal_fields();
        let output_schema: DataSchema = (&output_schema).into();
        let (need_reserve_block_info, _) = need_reserve_block_info(ctx.clone(), plan.table_index);
        Ok(ProcessorPtr::create(Box::new(DeserializeDataTransform {
            ctx: ctx.clone(),
            scan_id: plan.scan_id,
            scan_progress,
            block_reader,
            input,
            output,
            output_data: None,
            src_schema,
            output_schema,
            parts: vec![],
            chunks: vec![],
            index_reader,
            virtual_reader,
            base_block_ids: plan.base_block_ids.clone(),
            cached_runtime_filter: None,
            need_reserve_block_info,
        })))
    }

    fn runtime_filter(&mut self, data_block: DataBlock) -> Result<Option<Bitmap>> {
        // Check if already cached runtime filters
        if self.cached_runtime_filter.is_none() {
            let bloom_filters = self
                .ctx
                .get_runtime_filters(self.scan_id)
                .into_iter()
                .filter_map(|entry| {
                    let filter_id = entry.id;
                    let RuntimeFilterEntry { bloom, stats, .. } = entry;
                    let bloom = bloom?;
                    let column_index = self.src_schema.index_of(bloom.column_name.as_str()).ok()?;
                    Some(BloomRuntimeFilterRef {
                        column_index,
                        filter_id,
                        filter: bloom.filter.clone(),
                        stats,
                    })
                })
                .collect::<Vec<_>>();
            if bloom_filters.is_empty() {
                return Ok(None);
            }
            let mut filter_ids = bloom_filters
                .iter()
                .map(|f| f.filter_id)
                .collect::<Vec<_>>();
            filter_ids.sort_unstable();
            log::info!(
                "RUNTIME-FILTER: scan_id={} bloom_filters={} filter_ids={:?}",
                self.scan_id,
                bloom_filters.len(),
                filter_ids
            );
            self.cached_runtime_filter = Some(bloom_filters);
        }

        let mut bitmaps = vec![];
        for runtime_filter in self.cached_runtime_filter.as_ref().unwrap().iter() {
            let mut bitmap = MutableBitmap::from_len_zeroed(data_block.num_rows());
            let probe_block_entry = data_block.get_by_offset(runtime_filter.column_index);
            let probe_column = probe_block_entry.to_column();

            // Apply bloom filter
            let start = Instant::now();
            ExprBloomFilter::new(runtime_filter.filter.clone()).apply(probe_column, &mut bitmap)?;
            let elapsed = start.elapsed();
            let unset_bits = bitmap.null_count();
            runtime_filter
                .stats
                .record_bloom(elapsed.as_nanos() as u64, unset_bits as u64);
            bitmaps.push(bitmap);
        }
        if !bitmaps.is_empty() {
            let rf_bitmap = bitmaps
                .into_iter()
                .reduce(|acc, rf_filter| acc.bitand(&rf_filter.into()))
                .unwrap();

            Ok(rf_bitmap.into())
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
impl Processor for DeserializeDataTransform {
    fn name(&self) -> String {
        String::from("DeserializeDataTransform")
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

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if !self.chunks.is_empty() {
            if !self.input.has_data() {
                self.input.set_need_data();
            }

            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(source_meta) = data_block.take_meta() {
                if let Some(source_meta) = DataSourceWithMeta::downcast_from(source_meta) {
                    self.parts = source_meta.meta;
                    self.chunks = source_meta.data;
                    return Ok(Event::Sync);
                }
            }

            unreachable!();
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let part = self.parts.pop();
        let chunks = self.chunks.pop();
        if let Some((part, read_res)) = part.zip(chunks) {
            match read_res {
                ParquetDataSource::AggIndex((actual_part, data)) => {
                    let agg_index_reader = self.index_reader.as_ref().as_ref().unwrap();
                    let block = agg_index_reader.deserialize_parquet_data(actual_part, data)?;

                    let progress_values = ProgressValues {
                        rows: block.num_rows(),
                        bytes: block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);
                    Profile::record_usize_profile(
                        ProfileStatisticsName::ScanBytes,
                        block.memory_size(),
                    );

                    self.output_data = Some(block);
                }
                ParquetDataSource::Normal((data, virtual_data)) => {
                    let start = Instant::now();
                    let columns_chunks = data.columns_chunks()?;
                    let part = FuseBlockPartInfo::from_part(&part)?;

                    let mut data_block = self.block_reader.deserialize_parquet_chunks(
                        part.nums_rows,
                        &part.columns_meta,
                        columns_chunks,
                        &part.compression,
                        &part.location,
                    )?;

                    let origin_num_rows = data_block.num_rows();

                    let mut filter = None;
                    let bloom_start = Instant::now();

                    let rows_before = data_block.num_rows();
                    if let Some(bitmap) = self.runtime_filter(data_block.clone())? {
                        data_block = data_block.filter_with_bitmap(&bitmap)?;
                        filter = Some(bitmap);
                        let rows_after = data_block.num_rows();
                        let bloom_duration = bloom_start.elapsed();
                        Profile::record_usize_profile(
                            ProfileStatisticsName::RuntimeFilterBloomTime,
                            bloom_duration.as_nanos() as usize,
                        );
                        if rows_before > rows_after {
                            Profile::record_usize_profile(
                                ProfileStatisticsName::RuntimeFilterBloomRowsFiltered,
                                rows_before - rows_after,
                            );
                        }
                    }

                    // Add optional virtual columns
                    if let Some(virtual_reader) = self.virtual_reader.as_ref() {
                        data_block = virtual_reader
                            .deserialize_virtual_columns(data_block.clone(), virtual_data)?;
                    }

                    // Perf.
                    {
                        metrics_inc_remote_io_deserialize_milliseconds(
                            start.elapsed().as_millis() as u64
                        );
                    }

                    let progress_values = ProgressValues {
                        rows: data_block.num_rows(),
                        bytes: data_block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);
                    Profile::record_usize_profile(
                        ProfileStatisticsName::ScanBytes,
                        data_block.memory_size(),
                    );

                    let mut data_block =
                        data_block.resort(&self.src_schema, &self.output_schema)?;

                    // Fill `BlockMetaIndex` as `DataBlock.meta` if query internal columns,
                    // `TransformAddInternalColumns` will generate internal columns using `BlockMetaIndex` in next pipeline.
                    let offsets = if self.block_reader.query_internal_columns() {
                        filter.as_ref().map(|bitmap| {
                            (0..origin_num_rows)
                                .filter(|i| unsafe { bitmap.get_bit_unchecked(*i) })
                                .collect()
                        })
                    } else {
                        None
                    };

                    data_block = add_data_block_meta(
                        data_block,
                        part,
                        offsets,
                        self.base_block_ids.clone(),
                        self.block_reader.update_stream_columns(),
                        self.block_reader.query_internal_columns(),
                        self.need_reserve_block_info,
                    )?;

                    self.output_data = Some(data_block);
                }
            }
        }

        Ok(())
    }
}
