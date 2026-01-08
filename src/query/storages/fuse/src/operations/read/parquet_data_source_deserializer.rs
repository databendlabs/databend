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
use std::collections::HashSet;
use std::ops::BitAnd;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::runtime_filter_info::RuntimeBloomFilter;
use databend_common_catalog::runtime_filter_info::RuntimeFilterEntry;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStats;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::utils::filter_helper::FilterHelpers;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_metrics::storage::*;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::arrow_reader::RowSelector;
use roaring::RoaringTreemap;

use super::parquet_data_source::ParquetDataSource;
use super::util::add_data_block_meta;
use super::util::need_reserve_block_info;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::DataItem;
use crate::io::VirtualColumnReader;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning::ExprBloomFilter;

pub struct DeserializeDataTransform {
    ctx: Arc<dyn TableContext>,
    scan_id: usize,
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,
    func_ctx: FunctionContext,

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

    prewhere: Option<ParquetPrewhereState>,
}

#[derive(Clone)]
struct BloomRuntimeFilterRef {
    column_index: FieldIndex,
    filter_id: usize,
    filter: RuntimeBloomFilter,
    stats: Arc<RuntimeFilterStats>,
}

unsafe impl Send for DeserializeDataTransform {}

struct ParquetPrewhereState {
    prewhere_reader: Arc<BlockReader>,
    prewhere_filter: Expr,
    prewhere_leaf_column_ids: HashSet<ColumnId>,

    remain_reader: Option<Arc<BlockReader>>,
    remain_leaf_column_ids: HashSet<ColumnId>,
}

struct PrewhereEvalResult {
    selection: Option<RowSelection>,
    offsets: Option<RoaringTreemap>,
    filtered_all: bool,
    prewhere_block: DataBlock,
}

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
        let func_ctx = ctx.get_function_context()?;

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

        let prewhere =
            Self::try_build_parquet_prewhere_state(ctx.clone(), block_reader.clone(), plan)?;

        let (need_reserve_block_info, _) = need_reserve_block_info(ctx.clone(), plan.table_index);
        Ok(ProcessorPtr::create(Box::new(DeserializeDataTransform {
            ctx: ctx.clone(),
            scan_id: plan.scan_id,
            scan_progress,
            block_reader,
            func_ctx,
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
            prewhere,
        })))
    }

    fn try_build_parquet_prewhere_state(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
    ) -> Result<Option<ParquetPrewhereState>> {
        let Some(push_down) = plan.push_downs.as_ref() else {
            return Ok(None);
        };
        let Some(prewhere_info) = push_down.prewhere.as_ref() else {
            return Ok(None);
        };

        let prewhere_reader = BlockReader::create(
            ctx.clone(),
            block_reader.operator.clone(),
            block_reader.original_schema.clone(),
            prewhere_info.prewhere_columns.clone(),
            false,
            block_reader.update_stream_columns(),
            block_reader.put_cache,
        )?;

        let schema: DataSchema = (prewhere_reader.schema().as_ref()).into();
        let filter = prewhere_info
            .filter
            .as_expr(&BUILTIN_FUNCTIONS)
            .project_column_ref(|name| Ok(schema.column_with_name(name).unwrap().0))?;

        let prewhere_leaf_column_ids = prewhere_reader
            .project_column_nodes
            .iter()
            .flat_map(|node| node.leaf_column_ids.iter().copied())
            .collect::<HashSet<_>>();

        let remain_reader = if prewhere_info.remain_columns.is_empty() {
            None
        } else {
            Some(BlockReader::create(
                ctx,
                block_reader.operator.clone(),
                block_reader.original_schema.clone(),
                prewhere_info.remain_columns.clone(),
                false,
                block_reader.update_stream_columns(),
                block_reader.put_cache,
            )?)
        };

        let remain_leaf_column_ids = remain_reader
            .as_ref()
            .map(|reader| {
                reader
                    .project_column_nodes
                    .iter()
                    .flat_map(|node| node.leaf_column_ids.iter().copied())
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        Ok(Some(ParquetPrewhereState {
            prewhere_reader,
            prewhere_filter: filter,
            prewhere_leaf_column_ids,
            remain_reader,
            remain_leaf_column_ids,
        }))
    }

    fn filter_column_chunks<'a>(
        column_chunks: &std::collections::HashMap<ColumnId, DataItem<'a>>,
        leaf_column_ids: &HashSet<ColumnId>,
    ) -> std::collections::HashMap<ColumnId, DataItem<'a>> {
        let mut filtered = std::collections::HashMap::with_capacity(leaf_column_ids.len());
        for (column_id, data_item) in column_chunks.iter() {
            if !leaf_column_ids.contains(column_id) {
                continue;
            }
            let data_item = match data_item {
                DataItem::RawData(buf) => DataItem::RawData(buf.clone()),
                DataItem::ColumnArray(arr) => DataItem::ColumnArray(arr),
            };
            filtered.insert(*column_id, data_item);
        }
        filtered
    }

    fn bitmap_to_row_selection(bitmap: &MutableBitmap) -> RowSelection {
        let mut selectors = Vec::new();
        let mut i = 0;
        while i < bitmap.len() {
            let current = bitmap.get(i);
            let mut run = 1;
            while i + run < bitmap.len() && bitmap.get(i + run) == current {
                run += 1;
            }

            selectors.push(if current {
                RowSelector::select(run)
            } else {
                RowSelector::skip(run)
            });
            i += run;
        }
        RowSelection::from(selectors)
    }

    fn eval_prewhere_selection(
        &self,
        prewhere: &ParquetPrewhereState,
        part: &FuseBlockPartInfo,
        column_chunks: &std::collections::HashMap<ColumnId, DataItem<'_>>,
    ) -> Result<PrewhereEvalResult> {
        let prewhere_chunks =
            Self::filter_column_chunks(column_chunks, &prewhere.prewhere_leaf_column_ids);
        let prewhere_block = prewhere.prewhere_reader.deserialize_parquet_chunks(
            part.nums_rows,
            &part.columns_meta,
            prewhere_chunks,
            &part.compression,
            &part.location,
            None,
        )?;

        let evaluator = Evaluator::new(&prewhere_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let predicate = evaluator
            .run(&prewhere.prewhere_filter)?
            .try_downcast::<BooleanType>()
            .unwrap();
        let bitmap = FilterHelpers::filter_to_bitmap(predicate, prewhere_block.num_rows());

        if bitmap.null_count() == bitmap.len() {
            let offsets = if self.block_reader.query_internal_columns() {
                Some(RoaringTreemap::new())
            } else {
                None
            };
            return Ok(PrewhereEvalResult {
                selection: None,
                offsets,
                filtered_all: true,
                prewhere_block: DataBlock::empty_with_rows(0),
            });
        }

        if bitmap.null_count() == 0 {
            return Ok(PrewhereEvalResult {
                selection: None,
                offsets: None,
                filtered_all: false,
                prewhere_block,
            });
        }

        let selection = Some(Self::bitmap_to_row_selection(&bitmap));
        let offsets = if self.block_reader.query_internal_columns() {
            Some(
                RoaringTreemap::from_sorted_iter(
                    (0..bitmap.len())
                        .filter(|i| bitmap.get(*i))
                        .map(|i| i as u64),
                )
                .unwrap(),
            )
        } else {
            None
        };

        let filter_bitmap: Bitmap = bitmap.into();
        let prewhere_block = prewhere_block.filter_with_bitmap(&filter_bitmap)?;

        Ok(PrewhereEvalResult {
            selection,
            offsets,
            filtered_all: false,
            prewhere_block,
        })
    }

    fn build_block_from_prewhere_and_remain(
        &self,
        prewhere: &ParquetPrewhereState,
        prewhere_block: DataBlock,
        remain_block: Option<DataBlock>,
    ) -> Result<DataBlock> {
        let num_rows = prewhere_block.num_rows();
        let remain_block = remain_block.unwrap_or_else(|| DataBlock::empty_with_rows(num_rows));

        if remain_block.num_rows() != num_rows {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Fuse parquet prewhere produced mismatched rows: prewhere {}, remain {}",
                num_rows,
                remain_block.num_rows()
            )));
        }

        let src_schema: DataSchema = (self.block_reader.schema().as_ref()).into();
        let prewhere_schema: DataSchema = (prewhere.prewhere_reader.schema().as_ref()).into();
        let prewhere_indices = prewhere_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| (field.name().to_string(), idx))
            .collect::<std::collections::HashMap<_, _>>();

        let remain_indices = prewhere
            .remain_reader
            .as_ref()
            .map(|reader| {
                let remain_schema: DataSchema = (reader.schema().as_ref()).into();
                remain_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| (field.name().to_string(), idx))
                    .collect::<std::collections::HashMap<_, _>>()
            })
            .unwrap_or_default();

        let mut entries = Vec::with_capacity(src_schema.num_fields());
        for (idx, field) in src_schema.fields().iter().enumerate() {
            let name = field.name().as_str();

            let entry = if let Some(i) = prewhere_indices.get(name) {
                prewhere_block.get_by_offset(*i).clone()
            } else if let Some(i) = remain_indices.get(name) {
                remain_block.get_by_offset(*i).clone()
            } else {
                BlockEntry::new_const_column(
                    field.data_type().clone(),
                    self.block_reader.default_vals[idx].clone(),
                    num_rows,
                )
            };

            entries.push(entry);
        }

        Ok(DataBlock::new(entries, num_rows))
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
            self.cached_runtime_filter = Some(bloom_filters);
        }

        let mut bitmaps = vec![];
        for runtime_filter in self.cached_runtime_filter.as_ref().unwrap().iter() {
            let mut bitmap = MutableBitmap::from_len_zeroed(data_block.num_rows());
            let probe_block_entry = data_block.get_by_offset(runtime_filter.column_index);
            let probe_column = probe_block_entry.to_column();

            // Apply bloom filter
            let start = Instant::now();
            ExprBloomFilter::new(&runtime_filter.filter).apply(probe_column, &mut bitmap)?;
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

                    let mut prewhere_offsets = None;
                    let mut row_selection = None;
                    let mut data_block = if let Some(prewhere) = &self.prewhere {
                        let eval = self.eval_prewhere_selection(prewhere, part, &columns_chunks)?;
                        prewhere_offsets = eval.offsets;
                        row_selection = eval.selection.clone();

                        if eval.filtered_all {
                            let mut empty_block =
                                DataBlock::empty_with_schema(Arc::new(self.output_schema.clone()));
                            empty_block = add_data_block_meta(
                                empty_block,
                                part,
                                prewhere_offsets,
                                self.base_block_ids.clone(),
                                self.block_reader.update_stream_columns(),
                                self.block_reader.query_internal_columns(),
                                self.need_reserve_block_info,
                            )?;
                            self.output_data = Some(empty_block);
                            return Ok(());
                        }

                        let remain_block =
                            if let Some(remain_reader) = prewhere.remain_reader.as_ref() {
                                let remain_chunks = Self::filter_column_chunks(
                                    &columns_chunks,
                                    &prewhere.remain_leaf_column_ids,
                                );
                                Some(remain_reader.deserialize_parquet_chunks(
                                    part.nums_rows,
                                    &part.columns_meta,
                                    remain_chunks,
                                    &part.compression,
                                    &part.location,
                                    row_selection.clone(),
                                )?)
                            } else {
                                None
                            };

                        self.build_block_from_prewhere_and_remain(
                            prewhere,
                            eval.prewhere_block,
                            remain_block,
                        )?
                    } else {
                        self.block_reader.deserialize_parquet_chunks(
                            part.nums_rows,
                            &part.columns_meta,
                            columns_chunks,
                            &part.compression,
                            &part.location,
                            None,
                        )?
                    };

                    let mut filter = None;
                    let bloom_start = Instant::now();

                    if self.prewhere.is_some() {
                        // If `virtual_data` exists, it must be filtered consistently with prewhere selection.
                        if let Some(virtual_reader) = self.virtual_reader.as_ref() {
                            data_block = virtual_reader.deserialize_virtual_columns(
                                data_block.clone(),
                                virtual_data,
                                row_selection.as_ref(),
                            )?;
                        }

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
                    } else {
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
                            data_block = virtual_reader.deserialize_virtual_columns(
                                data_block.clone(),
                                virtual_data,
                                None,
                            )?;
                        }
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
                        match (prewhere_offsets, filter.as_ref()) {
                            (None, None) => None,
                            (None, Some(bitmap)) => Some(
                                RoaringTreemap::from_sorted_iter(
                                    (0..part.nums_rows)
                                        .filter(|i| unsafe { bitmap.get_bit_unchecked(*i) })
                                        .map(|i| i as u64),
                                )
                                .unwrap(),
                            ),
                            (Some(offsets), None) => Some(offsets),
                            (Some(offsets), Some(bitmap)) => Some(
                                RoaringTreemap::from_sorted_iter(
                                    offsets
                                        .iter()
                                        .enumerate()
                                        .filter(|(idx, _)| unsafe {
                                            bitmap.get_bit_unchecked(*idx)
                                        })
                                        .map(|(_, row_idx)| row_idx),
                                )
                                .unwrap(),
                            ),
                        }
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
