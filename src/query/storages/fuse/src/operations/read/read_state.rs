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

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::PrewhereInfo;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::runtime_filter_info::RuntimeBloomFilter;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStats;
use databend_common_catalog::table_context::TableContext;
use databend_common_column::bitmap::utils::SlicesIterator;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::EvaluateOptions;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::SelectExpr;
use databend_common_expression::SelectExprBuilder;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::MutableBitmap;
use databend_common_functions::BUILTIN_FUNCTIONS;
use roaring::RoaringTreemap;

use super::condition_cache;
use super::condition_cache::CachedCondition;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::io::DataItem;
use crate::io::ParquetBatches;
use crate::io::RowSelection;
use crate::pruning::ExprBloomFilter;

/// Fewest rows per block of [`ReadState::start_batches`]. A Parquet data page holds
/// about 1 MiB, so smaller batches would share their pages anyway, while each batch costs
/// a reader call and one more block downstream. With a smaller `max_block_size` (tests go
/// down to 1), batches keep this size and downstream operators split them as before.
const MIN_BATCH_ROWS: usize = 8192;

#[derive(Clone)]
pub struct BloomRuntimeFilterRef {
    pub probe_expr: Expr<FieldIndex>,
    pub filter: RuntimeBloomFilter,
    pub stats: Arc<RuntimeFilterStats>,
    pub sample_rows: u64,
}

/// Build a selector executor for a prewhere predicate, or `None` to keep the evaluator.
///
/// The selector evaluates each AND/OR child only on the rows earlier children left
/// undecided, and works on row indexes instead of a boolean column. A single comparison has
/// nothing to short-circuit and keeps the evaluator.
fn prewhere_selector(
    filter: &Expr,
    func_ctx: &FunctionContext,
    max_block_size: usize,
) -> Option<FilterExecutor> {
    if !func_ctx.enable_selector_executor {
        return None;
    }
    let (select_expr, _) = SelectExprBuilder::new(&BUILTIN_FUNCTIONS)
        .build(filter)
        .into();
    if matches!(select_expr, SelectExpr::Compare(_)) {
        return None;
    }
    Some(FilterExecutor::new(
        filter.clone(),
        func_ctx.clone(),
        max_block_size,
        None,
        &BUILTIN_FUNCTIONS,
        false,
    ))
}

/// Scan workload of one block, measured before prewhere and runtime filters
/// drop rows. This is what `scan_progress` and the `ScanBytes` profile report,
/// so filtering inside the scan must not shrink it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockScanStats {
    /// Number of rows in the block, regardless of how many survive the filters.
    pub rows: usize,
    /// Uncompressed size of all projected columns for the whole block. When
    /// row selection pushdown skips rows of the remaining columns, their
    /// contribution is estimated from the rows that were decoded.
    pub bytes: usize,
}

impl BlockScanStats {
    /// Account for columns that were materialized after `deserialize_and_filter`
    /// (e.g. virtual columns). `decoded_bytes` is the size of those columns as
    /// decoded for `decoded_rows` rows; when row selection pushdown left fewer
    /// rows than the block holds, the size is estimated for the whole block.
    pub fn add_decoded_bytes(&mut self, decoded_bytes: usize, decoded_rows: usize) {
        if decoded_bytes == 0 {
            return;
        }
        self.bytes += estimate_block_bytes(decoded_bytes, decoded_rows, self.rows, decoded_bytes);
    }
}

/// Result of [`ReadState::deserialize_and_filter`]: the block after prewhere
/// and runtime filters, the selection still to be applied by the caller, and
/// the scan statistics measured before any row was filtered out.
pub struct ReadStateOutput {
    pub block: DataBlock,
    /// Callers that need a Parquet [`RowSelection`] build it from the bitmap: it is
    /// costly for fragmented bitmaps and most blocks never use it.
    pub bitmap_selection: Option<Bitmap>,
    pub scan_stats: BlockScanStats,
}

/// One output batch of [`BlockBatches`].
pub(crate) struct ReadStateBatch {
    pub(crate) block: DataBlock,
    /// Original block row indices of the rows in `block`, populated when the caller
    /// requested row offsets for internal columns.
    pub(crate) offsets: Option<RoaringTreemap>,
}

pub struct ReadState {
    pub pre_reader: Arc<BlockReader>,
    pub remain_reader: Arc<BlockReader>,
    pub filters: Option<Expr>,
    prewhere_selector: Option<FilterExecutor>,
    /// Whether `filters` is set and deterministic. Evaluating a non-deterministic filter (e.g.
    /// seeded `rand()`) in batches yields different results than evaluating it on the whole
    /// block, so such filters skip the batched scan.
    filters_deterministic: bool,
    pub runtime_filters: Vec<BloomRuntimeFilterRef>,
    pub pre_column_ids: HashSet<ColumnId>,
    pub remain_column_ids: HashSet<ColumnId>,
    pub func_ctx: FunctionContext,
    pub output_schema: DataSchema,
    pub prewhere_selectivity_threshold: u64,
    pub use_single_prewhere_reader: bool,
    /// Static stage and column-mapping plan computed once from the readers and `output_schema`.
    plan: ReadPlan,
    /// Rows per block of [`Self::start_batches`]: the `max_block_size` setting, at
    /// least [`MIN_BATCH_ROWS`].
    batch_rows: usize,
    /// The key of the prewhere filter in the condition cache, if it is cached.
    condition_key: Option<u128>,
    /// The leading conjunct of the prewhere filter in the condition cache, if it has several.
    leading: Option<LeadingFilter>,
}

/// The first conjunct of a prewhere filter, its key in the condition cache, and the other
/// conjuncts. To record its rows, it is evaluated on the block, and the others on the rows it
/// selects, as the filter would.
struct LeadingFilter {
    key: u128,
    conjunct: Expr,
    conjunct_selector: Option<FilterExecutor>,
    rest: Expr,
    rest_selector: Option<FilterExecutor>,
}

impl ReadState {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        scan_id: usize,
        prewhere_info: Option<&PrewhereInfo>,
        block_reader: Arc<BlockReader>,
    ) -> Result<Self> {
        let prewhere_selectivity_threshold =
            ctx.get_settings().get_prewhere_selectivity_threshold()?;
        let use_single_prewhere_reader =
            prewhere_info.is_some() && prewhere_selectivity_threshold == 0;
        let original_schema = block_reader.original_schema.as_ref();

        let runtime_filter_entries: Vec<_> = ctx
            .get_runtime_filters(scan_id)
            .into_iter()
            .filter(|entry| {
                entry.bloom.is_some()
                    && entry
                        .probe_expr
                        .column_refs()
                        .keys()
                        .all(|name| runtime_filter_column_is_projectable(original_schema, name))
            })
            .collect();
        let mut runtime_filter_column_names = Vec::new();
        for entry in &runtime_filter_entries {
            for name in entry.probe_expr.column_refs().into_keys() {
                if !runtime_filter_column_names.contains(&name) {
                    runtime_filter_column_names.push(name);
                }
            }
        }
        let runtime_filter_column_names: Vec<_> = runtime_filter_column_names.iter().collect();

        let mut preread_projection =
            Projection::from_column_names(original_schema, &runtime_filter_column_names)?;
        if let Some(prewhere_info) = prewhere_info {
            Projection::merge(&mut preread_projection, &prewhere_info.prewhere_columns);
        }

        let remain_projection = if use_single_prewhere_reader {
            Projection::Columns(vec![])
        } else {
            block_reader.projection.difference(&preread_projection)
        };
        let prewhere_reader = if use_single_prewhere_reader {
            block_reader.clone()
        } else {
            block_reader.change_projection(preread_projection)?
        };
        let remain_reader = block_reader.change_projection(remain_projection)?;
        let pre_column_ids = prewhere_reader.schema().to_leaf_column_id_set();
        let remain_column_ids = remain_reader.schema().to_leaf_column_id_set();

        let prewhere_schema: DataSchema = (prewhere_reader.schema().as_ref()).into();

        let runtime_filters: Vec<BloomRuntimeFilterRef> = runtime_filter_entries
            .into_iter()
            .filter_map(|entry| {
                let bloom = entry.bloom?;
                let probe_expr = entry
                    .probe_expr
                    .project_column_ref(|name| prewhere_schema.index_of(name))
                    .ok()?;
                Some(BloomRuntimeFilterRef {
                    probe_expr,
                    filter: bloom.filter,
                    stats: entry.stats,
                    sample_rows: RuntimeFilterStats::bloom_sample_rows(entry.build_rows),
                })
            })
            .collect();

        let prewhere_filter = if let Some(prewhere_info) = prewhere_info {
            let filter = prewhere_info
                .filter
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| Ok(prewhere_schema.column_with_name(name).unwrap().0))?;
            Some(filter)
        } else {
            None
        };

        let func_ctx = ctx.get_function_context()?;
        let max_block_size = (ctx.get_settings().get_max_block_size()? as usize).max(1);
        let prewhere_selector = prewhere_filter
            .as_ref()
            .and_then(|filter| prewhere_selector(filter, &func_ctx, max_block_size));
        let filters_deterministic = prewhere_filter
            .as_ref()
            .is_some_and(|filter| filter.is_deterministic(&BUILTIN_FUNCTIONS));
        let condition_keys = match prewhere_info {
            Some(prewhere_info) if ctx.get_settings().get_enable_condition_cache()? => {
                condition_cache::condition_keys(prewhere_info, original_schema, &func_ctx)
            }
            _ => None,
        };
        let condition_key = condition_keys.as_ref().map(|keys| keys.whole);
        let leading = condition_keys
            .and_then(|keys| keys.leading)
            .map(|leading| -> Result<LeadingFilter> {
                let project = |expr: Expr<String>| {
                    expr.project_column_ref(|name| {
                        Ok(prewhere_schema.column_with_name(name).unwrap().0)
                    })
                };
                let conjunct = project(leading.conjunct)?;
                let rest = project(leading.rest)?;
                Ok(LeadingFilter {
                    key: leading.key,
                    conjunct_selector: self::prewhere_selector(
                        &conjunct,
                        &func_ctx,
                        max_block_size,
                    ),
                    conjunct,
                    rest_selector: self::prewhere_selector(&rest, &func_ctx, max_block_size),
                    rest,
                })
            })
            .transpose()?;

        let output_schema = block_reader.data_schema();
        let remain_schema = DataSchema::new(remain_reader.data_fields());
        let plan = ReadPlan::new(
            &prewhere_schema,
            &remain_schema,
            &output_schema,
            use_single_prewhere_reader,
        )?;

        Ok(Self {
            pre_reader: prewhere_reader,
            remain_reader,
            filters: prewhere_filter,
            prewhere_selector,
            filters_deterministic,
            runtime_filters,
            pre_column_ids,
            remain_column_ids,
            func_ctx,
            output_schema,
            prewhere_selectivity_threshold,
            use_single_prewhere_reader,
            plan,
            batch_rows: max_block_size.max(MIN_BATCH_ROWS),
            condition_key,
            leading,
        })
    }

    /// The rows `filters` selects, and those its leading conjunct selects: the conjunct is
    /// evaluated on the block, and the others on the rows it selects.
    fn filter_with_leading(
        &mut self,
        block: &DataBlock,
        num_rows: usize,
    ) -> Result<(MutableBitmap, Bitmap)> {
        let leading = self.leading.as_mut().unwrap();
        let conjunct: Bitmap = eval_filter(
            &leading.conjunct,
            leading.conjunct_selector.as_mut(),
            &self.func_ctx,
            block,
            num_rows,
        )?
        .into();
        let selected = num_rows - conjunct.null_count();
        if selected == 0 {
            return Ok((MutableBitmap::from_len_zeroed(num_rows), conjunct));
        }
        let rows = if selected == num_rows {
            block.clone()
        } else {
            block.clone().filter_with_bitmap(&conjunct)?
        };
        let rest = eval_filter(
            &leading.rest,
            leading.rest_selector.as_mut(),
            &self.func_ctx,
            &rows,
            selected,
        )?;
        Ok((expand_runtime_filter_bitmap(&conjunct, &rest), conjunct))
    }

    pub fn filter(&mut self, block: &DataBlock, num_rows: usize) -> Result<Option<MutableBitmap>> {
        self.filters
            .as_ref()
            .map(|filter| {
                eval_filter(
                    filter,
                    self.prewhere_selector.as_mut(),
                    &self.func_ctx,
                    block,
                    num_rows,
                )
            })
            .transpose()
    }

    pub fn runtime_filter(
        &self,
        block: &DataBlock,
        num_rows: usize,
    ) -> Result<Option<MutableBitmap>> {
        eval_runtime_filters(&self.runtime_filters, &self.func_ctx, block, num_rows)
    }

    pub fn deserialize_and_filter(
        &mut self,
        columns_chunks: HashMap<ColumnId, DataItem>,
        part: &FuseBlockPartInfo,
        no_output_columns: bool,
        condition: Option<&CachedCondition>,
        io_skipped: bool,
    ) -> Result<ReadStateOutput> {
        // The condition cache entry of the block, looked up before it was read: the rows where
        // the filter can select rows, or exactly the rows it selects. Without an entry of the
        // filter, its result is recorded, and without an entry of its leading conjunct either,
        // that of the conjunct.
        let condition = condition.filter(|_| self.condition_key.is_some());
        let record = self
            .condition_key
            .filter(|_| !matches!(condition, Some(CachedCondition::Whole(_))));
        let record_leading = condition.is_none() && self.leading.is_some();
        let exact = condition.is_some_and(CachedCondition::is_exact);
        let cached_rows = condition
            .and_then(|condition| condition_cache::rows_of(condition.condition(), part.nums_rows));
        if io_skipped {
            // Nothing is output: the block is read only for the rows it selects.
            debug_assert!(exact && no_output_columns);
            let rows = match (cached_rows, selected_rows(part)) {
                (Some(cached), Some(rows)) => Some(&cached & &rows),
                (cached, rows) => cached.or(rows),
            };
            let num_rows = rows
                .as_ref()
                .map_or(part.nums_rows, |rows| rows.len() - rows.null_count());
            return Ok(ReadStateOutput {
                block: DataBlock::empty_with_rows(num_rows),
                bitmap_selection: rows,
                scan_stats: BlockScanStats {
                    rows: part.nums_rows,
                    bytes: 0,
                },
            });
        }
        let pre_columns_chunks = Self::filter_column_chunks(&columns_chunks, &self.pre_column_ids)?;
        if no_output_columns
            && condition.is_none()
            && self.filters_deterministic
            && self.runtime_filters.is_empty()
            && self.remain_column_ids.is_empty()
            && part.nums_rows > MIN_BATCH_ROWS
            && self.pre_reader.can_parquet_batches(&pre_columns_chunks)
        {
            let mut batches =
                self.pre_reader
                    .parquet_batches(part, &pre_columns_chunks, None, MIN_BATCH_ROWS)?;
            let mut bytes = 0;
            let mut bits = MutableBitmap::with_capacity(part.nums_rows);
            let mut leading_bits = MutableBitmap::with_capacity(part.nums_rows);
            while let Some(batch) = batches.next_block(part) {
                let batch = batch?;
                bytes += block_logical_bytes(&batch);
                let bitmap = if record_leading {
                    let (bitmap, leading) = self.filter_with_leading(&batch, batch.num_rows())?;
                    leading_bits.extend_from_bitmap(&leading);
                    bitmap
                } else {
                    self.filter(&batch, batch.num_rows())?.unwrap()
                };
                bits.extend_from_bitmap(&bitmap.into());
            }
            // Apply the pruner's selection in original block coordinates.
            let mut bitmap: Bitmap = bits.into();
            if let Some(key) = record {
                condition_cache::insert(key, part, &bitmap);
            }
            if record_leading {
                let key = self.leading.as_ref().unwrap().key;
                condition_cache::insert(key, part, &leading_bits.into());
            }
            if let Some(rows) = selected_rows(part) {
                bitmap = &bitmap & &rows;
            }
            return Ok(ReadStateOutput {
                block: DataBlock::empty_with_rows(bitmap.len() - bitmap.null_count()),
                bitmap_selection: Some(bitmap),
                scan_stats: BlockScanStats {
                    rows: part.nums_rows,
                    bytes,
                },
            });
        }

        // Without cached granules, `num_rows` is the rows of the block; otherwise the prewhere
        // columns are decoded for the rows of the granules only.
        let (mut preread_block, num_rows, bytes) = match &cached_rows {
            None => {
                let block = self
                    .pre_reader
                    .deserialize_part(part, pre_columns_chunks, None)?;
                let bytes = block.memory_size();
                (block, part.nums_rows, bytes)
            }
            Some(rows) => {
                let raw_bytes = raw_bytes_of_chunks(&pre_columns_chunks);
                let block = self.pre_reader.deserialize_part(
                    part,
                    pre_columns_chunks,
                    Some(&RowSelection::from(rows)),
                )?;
                let bytes = estimate_block_bytes(
                    block.memory_size(),
                    block.num_rows(),
                    part.nums_rows,
                    raw_bytes,
                );
                (block, rows.len() - rows.null_count(), bytes)
            }
        };
        let mut scan_stats = BlockScanStats {
            rows: part.nums_rows,
            bytes,
        };

        // The rows of an exact entry all pass the filter.
        let filter_bitmap = if exact {
            None
        } else if record_leading {
            let (bitmap, leading) = self.filter_with_leading(&preread_block, num_rows)?;
            condition_cache::insert(self.leading.as_ref().unwrap().key, part, &leading);
            Some(bitmap)
        } else {
            self.filter(&preread_block, num_rows)?
        };
        if let (Some(key), Some(bitmap)) = (record, &filter_bitmap) {
            // In the rows of the block.
            let bitmap: Bitmap = bitmap.clone().into();
            let bitmap = match &cached_rows {
                Some(rows) => expand_selection(rows, &bitmap),
                None => bitmap,
            };
            condition_cache::insert(key, part, &bitmap);
        }
        // Expensive probe expressions are evaluated only for rows surviving
        // the static prewhere predicate. Expand their bitmap back to the
        // original row positions before combining it with prewhere.
        let runtime_filter_bitmap = if self.runtime_filters.is_empty() {
            // Preserve the profile entry emitted before expression probes were
            // supported, even when there is no Bloom filter to evaluate.
            self.runtime_filter(&preread_block, num_rows)?
        } else if let Some(filter_bitmap) = &filter_bitmap {
            let filter_bitmap: Bitmap = filter_bitmap.clone().into();
            let filtered_block = preread_block.clone().filter_with_bitmap(&filter_bitmap)?;
            self.runtime_filter(&filtered_block, filtered_block.num_rows())?
                .map(|runtime_bitmap| expand_runtime_filter_bitmap(&filter_bitmap, &runtime_bitmap))
        } else {
            self.runtime_filter(&preread_block, num_rows)?
        };

        let bitmap_selection: Option<Bitmap> = match (filter_bitmap, runtime_filter_bitmap) {
            (Some(filter_bitmap), Some(runtime_filter_bitmap)) => {
                let rhs: Bitmap = runtime_filter_bitmap.into();
                Some((filter_bitmap & &rhs).into())
            }
            (Some(filter_bitmap), None) => Some(filter_bitmap.into()),
            (None, Some(runtime_filter_bitmap)) => Some(runtime_filter_bitmap.into()),
            (None, None) => None,
        };
        // Back to the rows of the block.
        let bitmap_selection = match &cached_rows {
            Some(rows) => Some(expand_selection(
                rows,
                &bitmap_selection.unwrap_or_else(|| MutableBitmap::from_len_set(num_rows).into()),
            )),
            None => bitmap_selection,
        };
        let bitmap_selection = match (bitmap_selection, selected_rows(part)) {
            (Some(selection), Some(rows)) => Some(&selection & &rows),
            (selection, rows) => selection.or(rows),
        };

        if let Some(ref bitmap) = bitmap_selection {
            preread_block = match &cached_rows {
                Some(rows) => {
                    preread_block.filter_with_bitmap(&compress_selection(rows, bitmap))?
                }
                None => preread_block.filter_with_bitmap(bitmap)?,
            };
        }

        if self.use_single_prewhere_reader {
            return Ok(ReadStateOutput {
                block: preread_block,
                bitmap_selection,
                scan_stats,
            });
        }

        let remain_columns_chunks =
            Self::filter_column_chunks(&columns_chunks, &self.remain_column_ids)?;
        // Only built when pushed down to the remaining reader.
        let row_selection = bitmap_selection
            .as_ref()
            .filter(|bitmap| {
                should_push_down_row_selection(bitmap, self.prewhere_selectivity_threshold)
            })
            .map(RowSelection::from);
        let push_down_row_selection = row_selection.is_some();
        let remain_raw_bytes = if push_down_row_selection {
            raw_bytes_of_chunks(&remain_columns_chunks)
        } else {
            0
        };

        let mut remain_block = self.remain_reader.deserialize_part(
            part,
            remain_columns_chunks,
            row_selection.as_ref(),
        )?;
        if push_down_row_selection {
            scan_stats.bytes += estimate_block_bytes(
                remain_block.memory_size(),
                remain_block.num_rows(),
                part.nums_rows,
                remain_raw_bytes,
            );
        } else {
            scan_stats.bytes += remain_block.memory_size();
            if let Some(bitmap) = bitmap_selection.as_ref() {
                remain_block = remain_block.filter_with_bitmap(bitmap)?;
            }
        }

        let data_block = match &self.plan {
            ReadPlan::Empty => DataBlock::empty_with_rows(remain_block.num_rows()),
            ReadPlan::RemainOnly { output_indices } => {
                project_block(remain_block, output_indices.as_deref())
            }
            ReadPlan::PreOnly { output_indices } => {
                project_block(preread_block, output_indices.as_deref())
            }
            ReadPlan::TwoStage {
                pre_output_indices,
                merge_sources,
            } => {
                let kept_pre = (!pre_output_indices.is_empty())
                    .then(|| project_block(preread_block, Some(pre_output_indices)));
                merge_blocks(kept_pre.as_ref(), &remain_block, merge_sources)
            }
        };

        Ok(ReadStateOutput {
            block: data_block,
            bitmap_selection,
            scan_stats,
        })
    }

    /// Starts decoding an oversized block in batches of at most `max_block_size` rows (at
    /// least [`MIN_BATCH_ROWS`]), each holding its own Parquet page buffers. The batches are
    /// decoded one at a time by [`BlockBatches::next_batches`], as the caller pulls them.
    ///
    /// Supports unfiltered scans, deterministic prewhere predicates, Bloom runtime filters,
    /// and index row selections (`BlockMetaIndex::selected_rows`). Returns `None` when the
    /// block does not need splitting or must be decoded whole (non-deterministic filter,
    /// count-only scan without runtime filters, condition cache, deserialized column cache, or
    /// encoded columns).
    pub(crate) fn start_batches(
        &mut self,
        columns_chunks: &HashMap<ColumnId, DataItem>,
        part: &FuseBlockPartInfo,
        no_output_columns: bool,
        need_row_offsets: bool,
    ) -> Result<Option<BlockBatches>> {
        if !self.can_deserialize_batches(part, no_output_columns) {
            return Ok(None);
        }

        let index_selection = selected_rows(part);

        let (stage, bytes) = match &self.plan {
            ReadPlan::Empty => return Ok(None),
            ReadPlan::RemainOnly { output_indices } => {
                let remain_chunks =
                    Self::filter_column_chunks(columns_chunks, &self.remain_column_ids)?;
                if !self.remain_reader.can_parquet_batches(&remain_chunks) {
                    return Ok(None);
                }
                let assembler = RemainAssembler::RemainOnly(output_indices.clone());
                self.remain_stage(
                    part,
                    &remain_chunks,
                    index_selection,
                    assembler,
                    need_row_offsets,
                )?
            }
            ReadPlan::PreOnly { .. } => {
                let pre_chunks = Self::filter_column_chunks(columns_chunks, &self.pre_column_ids)?;
                if !self.pre_reader.can_parquet_batches(&pre_chunks) {
                    return Ok(None);
                }
                let batches =
                    self.pre_reader
                        .parquet_batches(part, &pre_chunks, None, self.batch_rows)?;
                let stage = BatchStage::Pre {
                    batches,
                    index_selection,
                    row_offset: 0,
                };
                (stage, 0)
            }
            ReadPlan::TwoStage {
                pre_output_indices,
                merge_sources,
            } => {
                // Check both readers before Pass 1 so we never mutate runtime filter stats and
                // then fall back to `deserialize_and_filter`. The check holds for the block even
                // if the column array cache is enabled meanwhile: batches never fill it.
                let pre_chunks = Self::filter_column_chunks(columns_chunks, &self.pre_column_ids)?;
                let remain_chunks =
                    Self::filter_column_chunks(columns_chunks, &self.remain_column_ids)?;
                if !self.pre_reader.can_parquet_batches(&pre_chunks)
                    || !self.remain_reader.can_parquet_batches(&remain_chunks)
                {
                    return Ok(None);
                }

                // Pass 1 runs over the whole block: reading the remaining columns with a pushed
                // down `RowSelection` needs the selection of every row up front. Only the
                // selection and the surviving rows of the output prewhere columns are kept.
                let mut whole_bitmap = MutableBitmap::with_capacity(part.nums_rows);
                let mut any_filtered = false;
                let mut kept_pre = (!pre_output_indices.is_empty()).then(BlockQueue::default);
                let mut filter_eval = FilterEvaluator {
                    filters: self.filters.as_ref(),
                    selector: self.prewhere_selector.as_mut(),
                    runtime_filters: &self.runtime_filters,
                    func_ctx: &self.func_ctx,
                };
                let pre_bytes = filter_eval.for_each_pre_batch(
                    &self.pre_reader,
                    part,
                    &pre_chunks,
                    index_selection.as_ref(),
                    self.batch_rows,
                    |batch, batch_sel, _| {
                        let batch_len = batch.num_rows();
                        match &batch_sel {
                            Some(b) => {
                                any_filtered |= b.null_count() > 0;
                                whole_bitmap.extend_from_bitmap(b);
                            }
                            None => whole_bitmap.extend_constant(batch_len, true),
                        }
                        if let Some(queue) = kept_pre.as_mut()
                            && let Some(kept) = project_and_filter(
                                batch,
                                Some(pre_output_indices),
                                batch_sel.as_ref(),
                            )?
                        {
                            queue.push(kept);
                        }
                        Ok(())
                    },
                )?;

                let selection: Option<Bitmap> = any_filtered.then(|| whole_bitmap.into());
                let assembler = RemainAssembler::Merge {
                    kept_pre,
                    sources: merge_sources.clone(),
                };
                let (stage, remain_bytes) = self.remain_stage(
                    part,
                    &remain_chunks,
                    selection,
                    assembler,
                    need_row_offsets,
                )?;
                (stage, pre_bytes + remain_bytes)
            }
        };

        Ok(Some(BlockBatches {
            stage,
            nums_rows: part.nums_rows,
            need_row_offsets,
            bytes,
        }))
    }

    fn can_deserialize_batches(&self, part: &FuseBlockPartInfo, no_output_columns: bool) -> bool {
        // The condition cache is read and filled by `deserialize_and_filter` only.
        self.condition_key.is_none()
            && part.nums_rows > self.batch_rows
            && self
                .filters
                .as_ref()
                .is_none_or(|_| self.filters_deterministic)
            && !(no_output_columns && self.runtime_filters.is_empty())
            && !(self.pre_column_ids.is_empty()
                && (self.filters.is_some() || !self.runtime_filters.is_empty()))
    }

    /// The stage reading `remain_reader` in batches, pushing down `RowSelection` when
    /// selective or else filtering each batch in memory, with the scan bytes already known.
    fn remain_stage(
        &self,
        part: &FuseBlockPartInfo,
        remain_chunks: &HashMap<ColumnId, DataItem>,
        selection: Option<Bitmap>,
        assembler: RemainAssembler,
        need_row_offsets: bool,
    ) -> Result<(BatchStage, usize)> {
        let raw_bytes = raw_bytes_of_chunks(remain_chunks);
        if selection
            .as_ref()
            .is_some_and(|b| b.null_count() == b.len())
        {
            return Ok((BatchStage::Done, raw_bytes));
        }

        let row_selection = selection
            .as_ref()
            .filter(|b| should_push_down_row_selection(b, self.prewhere_selectivity_threshold))
            .map(RowSelection::from);
        let batches = self.remain_reader.parquet_batches(
            part,
            remain_chunks,
            row_selection.as_ref(),
            self.batch_rows,
        )?;
        let offset_cursor = need_row_offsets.then(|| RowOffsetCursor::new(selection.clone()));
        let stage = RemainStage {
            batches,
            selection,
            pushed_down: row_selection.is_some(),
            assembler,
            offset_cursor,
            row_offset: 0,
            decoded_bytes: 0,
            decoded_rows: 0,
            raw_bytes,
        };
        Ok((BatchStage::Remain(stage), 0))
    }

    fn filter_column_chunks<'a>(
        columns_chunks: &'a HashMap<ColumnId, DataItem<'a>>,
        column_ids: &HashSet<ColumnId>,
    ) -> Result<HashMap<ColumnId, DataItem<'a>>> {
        let mut filtered_columns_chunks = HashMap::new();
        for (column_id, data_item) in columns_chunks {
            if column_ids.contains(column_id) {
                filtered_columns_chunks.insert(*column_id, data_item.clone());
            }
        }
        Ok(filtered_columns_chunks)
    }
}

/// The rows of `block` that `filter` selects, with the selector if any.
fn eval_filter(
    filter: &Expr,
    selector: Option<&mut FilterExecutor>,
    func_ctx: &FunctionContext,
    block: &DataBlock,
    num_rows: usize,
) -> Result<MutableBitmap> {
    if let Some(selector) = selector {
        // The block is selected in slices of `max_block_size` rows, as the `Filter`
        // operator does. The selected indexes are not sorted when the predicate
        // contains OR.
        let max_block_size = selector.max_block_size();
        let mut bitmap = MutableBitmap::from_len_zeroed(num_rows);
        for offset in (0..num_rows).step_by(max_block_size) {
            let end = (offset + max_block_size).min(num_rows);
            let selected = selector.select(&block.slice(offset..end))?;
            for index in &selector.true_selection()[..selected] {
                bitmap.set(offset + *index as usize, true);
            }
        }
        return Ok(bitmap);
    }
    let evaluator = Evaluator::new(block, func_ctx, &BUILTIN_FUNCTIONS);
    let filter_result = evaluator
        .run(filter)?
        .try_downcast::<BooleanType>()
        .unwrap();
    Ok(FilterHelpers::filter_to_bitmap(filter_result, num_rows))
}

/// Sets the rows of `outer` to the bits of `inner`, which has a bit per row `outer` sets.
fn expand_selection(outer: &Bitmap, inner: &Bitmap) -> Bitmap {
    debug_assert_eq!(outer.len() - outer.null_count(), inner.len());
    let mut inner = inner.iter();
    outer
        .iter()
        .map(|set| set && inner.next().unwrap())
        .collect()
}

/// The bits of `selection` at the rows `outer` sets: the inverse of [`expand_selection`].
fn compress_selection(outer: &Bitmap, selection: &Bitmap) -> Bitmap {
    outer
        .iter()
        .zip(selection.iter())
        .filter_map(|(set, selected)| set.then_some(selected))
        .collect()
}

fn runtime_filter_column_is_projectable(
    schema: &databend_common_expression::TableSchema,
    name: &str,
) -> bool {
    Projection::from_column_names(schema, &[name]).is_ok()
}

/// The rows of the block that index pruning selected for output (see
/// `BlockMetaIndex::selected_rows`). `None` when every row is output.
fn selected_rows(part: &FuseBlockPartInfo) -> Option<Bitmap> {
    part.block_meta_index()?.selected_rows(part.nums_rows)
}

fn expand_runtime_filter_bitmap(
    prewhere_bitmap: &Bitmap,
    runtime_filter_bitmap: &MutableBitmap,
) -> MutableBitmap {
    let mut runtime_filter_bits = runtime_filter_bitmap.iter();
    let mut expanded = MutableBitmap::with_capacity(prewhere_bitmap.len());
    for survives_prewhere in prewhere_bitmap.iter() {
        expanded.push(
            survives_prewhere
                && runtime_filter_bits
                    .next()
                    .expect("runtime filter bitmap must match surviving rows"),
        );
    }
    debug_assert!(runtime_filter_bits.next().is_none());
    expanded
}

/// Size of the column chunks as they were read, before decoding.
fn raw_bytes_of_chunks(columns_chunks: &HashMap<ColumnId, DataItem>) -> usize {
    columns_chunks
        .values()
        .map(|item| match item {
            DataItem::RawData(buffer) => buffer.len(),
            DataItem::ColumnArray(array) => array.1,
        })
        .sum()
}

/// Estimate the uncompressed size of some columns for the whole block when
/// only `decoded_rows` of them were materialized, by scaling `decoded_bytes`
/// up to `total_rows`.
///
/// Falls back to `raw_bytes` (the size of the column chunks that were read)
/// when nothing was decoded, so blocks whose rows are all filtered out still
/// account for the data that was read for them.
fn estimate_block_bytes(
    decoded_bytes: usize,
    decoded_rows: usize,
    total_rows: usize,
    raw_bytes: usize,
) -> usize {
    if decoded_rows == 0 || total_rows == 0 {
        return raw_bytes;
    }
    if decoded_rows >= total_rows {
        return decoded_bytes;
    }
    ((decoded_bytes as u128 * total_rows as u128) / decoded_rows as u128) as usize
}

fn block_logical_bytes(block: &DataBlock) -> usize {
    // Adjacent batches can reference the same page; count logical decoded bytes.
    block
        .columns()
        .iter()
        .map(|entry| entry.memory_size_with_options(true))
        .sum()
}

/// The batches of one block from [`ReadState::start_batches`], decoded as they are pulled:
/// each pull decompresses and decodes the pages of one more batch, and the pages of the
/// batches already pulled are released as soon as downstream drops them. The compressed
/// column chunks stay alive until the last batch. With [`ReadPlan::TwoStage`], the prewhere
/// pass has already run over the whole block, so its selection and the surviving rows of
/// the output prewhere columns are held until the remaining columns catch up with them.
pub(crate) struct BlockBatches {
    stage: BatchStage,
    nums_rows: usize,
    need_row_offsets: bool,
    /// Scan bytes of the stages that are done.
    bytes: usize,
}

enum BatchStage {
    /// [`ReadPlan::PreOnly`]: filters each batch of the prewhere columns as it is decoded.
    Pre {
        batches: ParquetBatches,
        index_selection: Option<Bitmap>,
        row_offset: usize,
    },
    /// [`ReadPlan::RemainOnly`] and the second pass of [`ReadPlan::TwoStage`].
    Remain(RemainStage),
    Done,
}

struct RemainStage {
    batches: ParquetBatches,
    /// The rows of the block surviving the filters, `None` when all do.
    selection: Option<Bitmap>,
    /// Whether `selection` was pushed down to the reader, which then decodes only the
    /// surviving rows.
    pushed_down: bool,
    assembler: RemainAssembler,
    offset_cursor: Option<RowOffsetCursor>,
    /// First row of the next batch in the block, when `selection` is not pushed down.
    row_offset: usize,
    decoded_bytes: usize,
    decoded_rows: usize,
    raw_bytes: usize,
}

impl RemainStage {
    fn scan_bytes(&self, nums_rows: usize) -> usize {
        if self.pushed_down {
            estimate_block_bytes(
                self.decoded_bytes,
                self.decoded_rows,
                nums_rows,
                self.raw_bytes,
            )
        } else {
            self.decoded_bytes
        }
    }
}

impl BlockBatches {
    /// Decodes the next batch of the block `part` and returns its output blocks, none when
    /// the filters drop all its rows; `None` once the block is done.
    pub(crate) fn next_batches(
        &mut self,
        state: &mut ReadState,
        part: &FuseBlockPartInfo,
    ) -> Result<Option<Vec<ReadStateBatch>>> {
        let need_row_offsets = self.need_row_offsets;
        let mut out = Vec::new();
        match &mut self.stage {
            BatchStage::Done => return Ok(None),
            BatchStage::Pre {
                batches,
                index_selection,
                row_offset,
            } => {
                let Some(batch) = batches.next_block(part) else {
                    self.stage = BatchStage::Done;
                    return Ok(None);
                };
                let batch = batch?;
                let batch_len = batch.num_rows();
                self.bytes += block_logical_bytes(&batch);
                let batch_start = *row_offset;
                *row_offset += batch_len;

                let ReadPlan::PreOnly { output_indices } = &state.plan else {
                    unreachable!("prewhere-only batches come from ReadPlan::PreOnly");
                };
                let mut filter_eval = FilterEvaluator {
                    filters: state.filters.as_ref(),
                    selector: state.prewhere_selector.as_mut(),
                    runtime_filters: &state.runtime_filters,
                    func_ctx: &state.func_ctx,
                };
                let idx_slice = index_selection
                    .as_ref()
                    .map(|rows| rows.clone().sliced(batch_start, batch_len));
                let batch_sel = filter_eval.eval_selection(&batch, batch_len, idx_slice)?;
                if let Some(block) =
                    project_and_filter(batch, output_indices.as_deref(), batch_sel.as_ref())?
                {
                    let offsets = need_row_offsets
                        .then(|| bitmap_offsets_at(batch_sel.as_ref(), batch_start, batch_len));
                    out.push(ReadStateBatch { block, offsets });
                }
            }
            BatchStage::Remain(stage) => {
                let Some(batch) = stage.batches.next_block(part) else {
                    stage.assembler.assert_drained();
                    self.bytes += stage.scan_bytes(self.nums_rows);
                    self.stage = BatchStage::Done;
                    return Ok(None);
                };
                let batch = batch?;
                let batch_len = batch.num_rows();
                stage.decoded_bytes += block_logical_bytes(&batch);
                stage.decoded_rows += batch_len;

                let surviving = if stage.pushed_down {
                    (batch_len > 0).then_some(batch)
                } else {
                    let slice = stage
                        .selection
                        .as_ref()
                        .map(|b| b.clone().sliced(stage.row_offset, batch_len));
                    stage.row_offset += batch_len;
                    filter_surviving(batch, slice.as_ref())?
                };

                if let Some(remain_batch) = surviving {
                    let offset_cursor = &mut stage.offset_cursor;
                    stage.assembler.assemble(remain_batch, |block| {
                        let offsets = offset_cursor.as_mut().map(|c| c.take(block.num_rows()));
                        out.push(ReadStateBatch { block, offsets });
                    });
                }
            }
        }
        Ok(Some(out))
    }

    /// The scan workload of the block so far, see [`BlockScanStats`]: all its rows, and the
    /// bytes of the batches decoded so far. It never decreases, so callers can report the
    /// increase after each batch; once the block is done it matches whole-block decoding,
    /// which scales the bytes decoded with a pushed-down selection up to the block.
    pub(crate) fn scan_stats(&self) -> BlockScanStats {
        let pending = match &self.stage {
            BatchStage::Remain(stage) => stage.decoded_bytes,
            BatchStage::Pre { .. } | BatchStage::Done => 0,
        };
        BlockScanStats {
            rows: self.nums_rows,
            bytes: self.bytes + pending,
        }
    }
}

#[derive(Clone, Copy)]
enum ColumnSource {
    Pre(usize),
    Remain(usize),
}

/// Static execution plan and column layout for a [`ReadState`], computed once from the
/// pre/remain/output schemas so per-block decoding never rebuilds schemas or re-derives
/// stage flags.
enum ReadPlan {
    Empty,
    RemainOnly {
        output_indices: Option<Vec<usize>>,
    },
    PreOnly {
        output_indices: Option<Vec<usize>>,
    },
    TwoStage {
        pre_output_indices: Vec<usize>,
        merge_sources: Vec<ColumnSource>,
    },
}

impl ReadPlan {
    fn new(
        pre_schema: &DataSchema,
        remain_schema: &DataSchema,
        output_schema: &DataSchema,
        use_single_prewhere_reader: bool,
    ) -> Result<Self> {
        let has_pre = !pre_schema.fields().is_empty();
        let has_remain = !use_single_prewhere_reader && !remain_schema.fields().is_empty();
        match (has_pre, has_remain) {
            (false, false) => Ok(Self::Empty),
            (false, true) => Ok(Self::RemainOnly {
                output_indices: identity_or_indices(remain_schema, output_schema)?,
            }),
            (true, false) => Ok(Self::PreOnly {
                output_indices: identity_or_indices(pre_schema, output_schema)?,
            }),
            (true, true) => {
                let mut pre_output_indices = Vec::new();
                let mut pre_pos_by_name = HashMap::new();
                for (i, field) in pre_schema.fields().iter().enumerate() {
                    if output_schema.index_of(field.name()).is_ok() {
                        pre_pos_by_name.insert(field.name().as_str(), pre_output_indices.len());
                        pre_output_indices.push(i);
                    }
                }
                let merge_sources = output_schema
                    .fields()
                    .iter()
                    .map(|field| {
                        if let Some(&pos) = pre_pos_by_name.get(field.name().as_str()) {
                            Ok(ColumnSource::Pre(pos))
                        } else {
                            Ok(ColumnSource::Remain(remain_schema.index_of(field.name())?))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Self::TwoStage {
                    pre_output_indices,
                    merge_sources,
                })
            }
        }
    }
}

fn identity_or_indices(src: &DataSchema, dst: &DataSchema) -> Result<Option<Vec<usize>>> {
    let indices: Vec<usize> = dst
        .fields()
        .iter()
        .map(|f| src.index_of(f.name()))
        .collect::<Result<_>>()?;
    let is_identity =
        src.fields().len() == indices.len() && indices.iter().enumerate().all(|(i, &j)| i == j);
    Ok((!is_identity).then_some(indices))
}

fn project_block(block: DataBlock, indices: Option<&[usize]>) -> DataBlock {
    match indices {
        None => block,
        Some(indices) => {
            let entries = indices
                .iter()
                .map(|&i| block.get_by_offset(i).clone())
                .collect();
            DataBlock::new(entries, block.num_rows())
        }
    }
}

fn merge_blocks(
    pre: Option<&DataBlock>,
    remain: &DataBlock,
    sources: &[ColumnSource],
) -> DataBlock {
    let entries = sources
        .iter()
        .map(|&src| match src {
            ColumnSource::Pre(i) => pre
                .expect("kept pre block must exist for Pre column source")
                .get_by_offset(i)
                .clone(),
            ColumnSource::Remain(i) => remain.get_by_offset(i).clone(),
        })
        .collect();
    DataBlock::new(entries, remain.num_rows())
}

enum RemainAssembler {
    RemainOnly(Option<Vec<usize>>),
    Merge {
        kept_pre: Option<BlockQueue>,
        sources: Vec<ColumnSource>,
    },
}

impl RemainAssembler {
    /// Emits the output blocks of one remain batch, in row order.
    ///
    /// With pushed-down `RowSelection`, remain batches hold `batch_rows` selected rows and
    /// do not line up with the kept prewhere blocks, which hold the rows surviving each
    /// prewhere batch. The remain batch is then sliced at the kept block boundaries instead
    /// of concatenating kept blocks, so neither side is copied: a batch yields at most one
    /// more block than the kept blocks it spans.
    fn assemble(&mut self, remain: DataBlock, mut emit: impl FnMut(DataBlock)) {
        match self {
            Self::RemainOnly(indices) => emit(project_block(remain, indices.as_deref())),
            Self::Merge {
                kept_pre: None,
                sources,
            } => emit(merge_blocks(None, &remain, sources)),
            Self::Merge {
                kept_pre: Some(queue),
                sources,
            } => {
                let rows = remain.num_rows();
                let mut start = 0;
                while start < rows {
                    let pre = queue.pop_front(rows - start);
                    let len = pre.num_rows();
                    let remain_rows = if len == rows {
                        remain.clone()
                    } else {
                        remain.slice(start..start + len)
                    };
                    emit(merge_blocks(Some(&pre), &remain_rows, sources));
                    start += len;
                }
            }
        }
    }

    fn assert_drained(&self) {
        if let Self::Merge {
            kept_pre: Some(q), ..
        } = self
        {
            assert!(
                q.is_empty(),
                "kept prewhere rows must be fully consumed by remain batches"
            );
        }
    }
}

/// Split-borrow view over [`ReadState`]'s filter state so prewhere/runtime filters can be
/// evaluated while borrowing `pre_reader`, `remain_reader`, and `plan` immutably.
struct FilterEvaluator<'a> {
    filters: Option<&'a Expr>,
    selector: Option<&'a mut FilterExecutor>,
    runtime_filters: &'a [BloomRuntimeFilterRef],
    func_ctx: &'a FunctionContext,
}

impl FilterEvaluator<'_> {
    fn eval_prewhere(
        &mut self,
        block: &DataBlock,
        num_rows: usize,
    ) -> Result<Option<MutableBitmap>> {
        self.filters
            .map(|filter| {
                eval_filter(
                    filter,
                    self.selector.as_deref_mut(),
                    self.func_ctx,
                    block,
                    num_rows,
                )
            })
            .transpose()
    }

    /// Evaluates static prewhere, Bloom runtime filters, and optional index row selection
    /// on `block` (`num_rows` rows). Expensive runtime-filter probe expressions run only on
    /// rows surviving prewhere, then expand back to `block`'s row positions.
    fn eval_selection(
        &mut self,
        block: &DataBlock,
        num_rows: usize,
        index_selection: Option<Bitmap>,
    ) -> Result<Option<Bitmap>> {
        let filter_bitmap = self.eval_prewhere(block, num_rows)?;
        let runtime_filter_bitmap = if self.runtime_filters.is_empty() {
            // Preserve the profile entry even when there is no Bloom filter to evaluate.
            eval_runtime_filters(self.runtime_filters, self.func_ctx, block, num_rows)?
        } else if let Some(filter_bitmap) = &filter_bitmap {
            let filter_bitmap: Bitmap = filter_bitmap.clone().into();
            let filtered_block = if filter_bitmap.null_count() == 0 {
                block.clone()
            } else {
                block.clone().filter_with_bitmap(&filter_bitmap)?
            };
            eval_runtime_filters(
                self.runtime_filters,
                self.func_ctx,
                &filtered_block,
                filtered_block.num_rows(),
            )?
            .map(|runtime_bitmap| expand_runtime_filter_bitmap(&filter_bitmap, &runtime_bitmap))
        } else {
            eval_runtime_filters(self.runtime_filters, self.func_ctx, block, num_rows)?
        };

        let selection: Option<Bitmap> = match (filter_bitmap, runtime_filter_bitmap) {
            (Some(f), Some(rf)) => {
                let rhs: Bitmap = rf.into();
                Some((f & &rhs).into())
            }
            (Some(f), None) => Some(f.into()),
            (None, Some(rf)) => Some(rf.into()),
            (None, None) => None,
        };
        Ok(match (selection, index_selection) {
            (Some(s), Some(rows)) => Some(&s & &rows),
            (s, rows) => s.or(rows),
        })
    }

    fn for_each_pre_batch(
        &mut self,
        pre_reader: &Arc<BlockReader>,
        part: &FuseBlockPartInfo,
        pre_chunks: &HashMap<ColumnId, DataItem>,
        index_selection: Option<&Bitmap>,
        batch_rows: usize,
        mut on_batch: impl FnMut(DataBlock, Option<Bitmap>, usize) -> Result<()>,
    ) -> Result<usize> {
        let mut pre_batches = pre_reader.parquet_batches(part, pre_chunks, None, batch_rows)?;
        let mut scan_bytes = 0usize;
        let mut row_offset = 0usize;
        while let Some(batch) = pre_batches.next_block(part) {
            let batch = batch?;
            let batch_len = batch.num_rows();
            scan_bytes += block_logical_bytes(&batch);
            let idx_slice = index_selection.map(|rows| rows.clone().sliced(row_offset, batch_len));
            let batch_sel = self.eval_selection(&batch, batch_len, idx_slice)?;
            on_batch(batch, batch_sel, row_offset)?;
            row_offset += batch_len;
        }
        Ok(scan_bytes)
    }
}

fn eval_runtime_filters(
    runtime_filters: &[BloomRuntimeFilterRef],
    func_ctx: &FunctionContext,
    block: &DataBlock,
    num_rows: usize,
) -> Result<Option<MutableBitmap>> {
    let bloom_start = Instant::now();

    let mut bitmaps = vec![];
    let evaluator = Evaluator::new(block, func_ctx, &BUILTIN_FUNCTIONS);
    for runtime_filter in runtime_filters {
        // The shared state is per target entry, not per RF id. Disabling
        // only skips this Bloom optimization; other filters still apply.
        if runtime_filter.stats.bloom_disabled() {
            continue;
        }
        let filter_start = Instant::now();
        // The bloom filter hashes encoded columns without decoding them.
        let probe_expr = &runtime_filter.probe_expr;
        let probe_column = evaluator
            .partial_run(probe_expr, None, &mut EvaluateOptions::default(), true)
            .map_err(|err| Evaluator::map_err(err, probe_expr))?
            .convert_to_full_column(probe_expr.data_type(), num_rows);
        let bitmap = ExprBloomFilter::new(&runtime_filter.filter).apply(probe_column)?;
        runtime_filter.stats.record_bloom(
            filter_start.elapsed().as_nanos() as u64,
            bitmap.null_count() as u64,
            bitmap.len() as u64,
            runtime_filter.sample_rows,
        );
        bitmaps.push(bitmap);
    }

    let result_bitmap = bitmaps.into_iter().reduce(|acc, b| {
        let rhs: Bitmap = b.into();
        acc & &rhs
    });

    let bloom_duration = bloom_start.elapsed();
    Profile::record_usize_profile(
        ProfileStatisticsName::RuntimeFilterBloomTime,
        bloom_duration.as_nanos() as usize,
    );

    Ok(result_bitmap)
}

fn project_and_filter(
    block: DataBlock,
    indices: Option<&[usize]>,
    selection: Option<&Bitmap>,
) -> Result<Option<DataBlock>> {
    if block.num_rows() == 0 || selection.is_some_and(|b| b.null_count() == b.len()) {
        return Ok(None);
    }
    let projected = project_block(block, indices);
    filter_surviving(projected, selection)
}

fn filter_surviving(block: DataBlock, selection: Option<&Bitmap>) -> Result<Option<DataBlock>> {
    if block.num_rows() == 0 {
        return Ok(None);
    }
    match selection {
        None => Ok(Some(block)),
        Some(b) if b.null_count() == b.len() => Ok(None),
        Some(b) if b.null_count() == 0 => Ok(Some(block)),
        Some(b) => Ok(Some(block.filter_with_bitmap(b)?)),
    }
}

fn bitmap_offsets_at(
    selection: Option<&Bitmap>,
    row_offset: usize,
    batch_len: usize,
) -> RoaringTreemap {
    let mut offsets = RoaringTreemap::new();
    match selection {
        Some(bitmap) => {
            for (start, len) in SlicesIterator::new(bitmap) {
                let abs_start = (row_offset + start) as u64;
                offsets.insert_range(abs_start..abs_start + len as u64);
            }
        }
        None => {
            offsets.insert_range(row_offset as u64..(row_offset + batch_len) as u64);
        }
    }
    offsets
}

/// Yields original block row offsets in chunks of `count` surviving rows.
enum RowOffsetCursor {
    Contiguous(usize),
    /// The rows `selection` sets: the rest of a run already started, then the runs from
    /// row `scan` on. Each bit is scanned once over the whole block.
    Selected {
        selection: Bitmap,
        pending: Option<(usize, usize)>,
        scan: usize,
    },
}

impl RowOffsetCursor {
    fn new(selection: Option<Bitmap>) -> Self {
        match selection {
            None => Self::Contiguous(0),
            Some(selection) => Self::Selected {
                selection,
                pending: None,
                scan: 0,
            },
        }
    }

    fn take(&mut self, count: usize) -> RoaringTreemap {
        let mut offsets = RoaringTreemap::new();
        match self {
            Self::Contiguous(next) => {
                offsets.insert_range(*next as u64..(*next + count) as u64);
                *next += count;
            }
            Self::Selected {
                selection,
                pending,
                scan,
            } => {
                let mut remaining = count;
                while remaining > 0 {
                    let (start, len) = match pending.take() {
                        Some(run) => run,
                        None => {
                            let start = find_bit(selection, *scan, true);
                            assert!(
                                start < selection.len(),
                                "bitmap selection must cover every decoded row"
                            );
                            let end = find_bit(selection, start, false);
                            *scan = end;
                            (start, end - start)
                        }
                    };
                    let take = remaining.min(len);
                    offsets.insert_range(start as u64..(start + take) as u64);
                    remaining -= take;
                    if take < len {
                        *pending = Some((start + take, len - take));
                    }
                }
            }
        }
        offsets
    }
}

/// The first row from `from` on whose bit in `bitmap` is `value`, or the length of `bitmap`.
fn find_bit(bitmap: &Bitmap, from: usize, value: bool) -> usize {
    let (bytes, offset, len) = bitmap.as_slice();
    let mut row = from;
    while row < len {
        let bit = offset + row;
        let byte = if value {
            bytes[bit / 8]
        } else {
            !bytes[bit / 8]
        };
        let rest = byte >> (bit % 8);
        if rest != 0 {
            return (row + rest.trailing_zeros() as usize).min(len);
        }
        row += 8 - bit % 8;
    }
    len
}

/// FIFO queue of filtered prewhere batches, consumed in row order by the remain batches
/// without copying.
#[derive(Default)]
struct BlockQueue {
    blocks: VecDeque<DataBlock>,
    offset: usize,
}

impl BlockQueue {
    fn push(&mut self, block: DataBlock) {
        self.blocks.push_back(block);
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Removes the next at most `max_rows` rows, all from the front block: the whole
    /// block when it fits, else a zero-copy slice of it.
    fn pop_front(&mut self, max_rows: usize) -> DataBlock {
        let front = self
            .blocks
            .front()
            .expect("prewhere blocks must match remaining rows");
        let end = front.num_rows().min(self.offset + max_rows);
        if self.offset == 0 && end == front.num_rows() {
            return self.blocks.pop_front().unwrap();
        }
        let piece = front.slice(self.offset..end);
        if end == front.num_rows() {
            self.blocks.pop_front();
            self.offset = 0;
        } else {
            self.offset = end;
        }
        piece
    }
}

fn should_push_down_row_selection(bitmap: &Bitmap, threshold: u64) -> bool {
    let total_rows = bitmap.len();
    if threshold == 0 || total_rows == 0 {
        return false;
    }
    let selected_rows = total_rows - bitmap.null_count();
    (selected_rows as u128) * 100 < (total_rows as u128) * (threshold as u128)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::MutableBitmap;
    use databend_common_expression::types::NumberDataType;

    use super::*;

    #[test]
    fn test_should_push_down_row_selection() {
        let mut sparse_bitmap = MutableBitmap::from_len_zeroed(5);
        sparse_bitmap.set(2, true);
        let sparse_bitmap: Bitmap = sparse_bitmap.into();

        assert!(should_push_down_row_selection(&sparse_bitmap, 50));
        assert!(!should_push_down_row_selection(&sparse_bitmap, 20));
        assert!(!should_push_down_row_selection(&sparse_bitmap, 0));

        let dense_bitmap: Bitmap = MutableBitmap::from_len_set(5).into();
        assert!(!should_push_down_row_selection(&dense_bitmap, 100));
    }

    #[test]
    fn test_estimate_block_bytes() {
        // Scales decoded bytes up to the whole block.
        assert_eq!(estimate_block_bytes(40, 2, 10, 7), 200);
        // Fully decoded block is reported as is.
        assert_eq!(estimate_block_bytes(40, 10, 10, 7), 40);
        assert_eq!(estimate_block_bytes(40, 12, 10, 7), 40);
        // Nothing decoded: fall back to the raw chunk sizes that were read.
        assert_eq!(estimate_block_bytes(0, 0, 10, 7), 7);
    }

    #[test]
    fn test_add_decoded_bytes() {
        let mut stats = BlockScanStats {
            rows: 10,
            bytes: 100,
        };
        // Fully decoded columns are added as is.
        stats.add_decoded_bytes(50, 10);
        assert_eq!(stats.bytes, 150);
        // Columns decoded for a subset of rows are scaled to the block.
        stats.add_decoded_bytes(10, 2);
        assert_eq!(stats.bytes, 200);
        // Nothing decoded leaves the stats untouched.
        stats.add_decoded_bytes(0, 0);
        assert_eq!(stats.bytes, 200);
    }

    #[test]
    fn test_raw_bytes_of_chunks() {
        let mut chunks = HashMap::new();
        chunks.insert(0, DataItem::RawData(opendal::Buffer::from(vec![0u8; 16])));
        chunks.insert(1, DataItem::RawData(opendal::Buffer::from(vec![0u8; 8])));
        assert_eq!(raw_bytes_of_chunks(&chunks), 24);
        assert_eq!(raw_bytes_of_chunks(&HashMap::new()), 0);
    }

    #[test]
    fn test_threshold_zero_disables_row_selection_pushdown() {
        let mut sparse_bitmap = MutableBitmap::from_len_zeroed(5);
        sparse_bitmap.set(2, true);
        let sparse_bitmap: Bitmap = sparse_bitmap.into();

        assert!(!should_push_down_row_selection(&sparse_bitmap, 0));
    }

    #[test]
    fn test_expand_runtime_filter_bitmap_after_prewhere() {
        let prewhere: Bitmap = [false, true, true, false, true].into_iter().collect();
        let runtime_filter: MutableBitmap = [true, false, true].into_iter().collect();

        let expanded = expand_runtime_filter_bitmap(&prewhere, &runtime_filter);
        assert_eq!(expanded.iter().collect::<Vec<_>>(), vec![
            false, true, false, false, true
        ]);
    }

    #[test]
    fn test_expand_and_compress_selection() {
        let outer: Bitmap = [false, true, true, false, true].into_iter().collect();
        let inner: Bitmap = [true, false, true].into_iter().collect();
        let expanded = expand_selection(&outer, &inner);
        assert_eq!(expanded.iter().collect::<Vec<_>>(), vec![
            false, true, false, false, true
        ]);
        assert_eq!(compress_selection(&outer, &expanded), inner);
    }

    #[test]
    fn test_nested_runtime_filter_column_is_projectable() {
        let schema = TableSchema::new(vec![TableField::new("payload", TableDataType::Tuple {
            fields_name: vec!["value".to_string()],
            fields_type: vec![TableDataType::Number(NumberDataType::Int64)],
        })]);
        assert!(schema.index_of("payload:value").is_err());
        assert!(runtime_filter_column_is_projectable(
            &schema,
            "payload:value"
        ));

        let projection = Projection::from_column_names(&schema, &["payload:value"]).unwrap();
        let projected_schema = projection.project_schema(&schema);
        let projected_schema: DataSchema = (&projected_schema).into();
        let probe_expr = Expr::ColumnRef(ColumnRef {
            span: None,
            id: "payload:value".to_string(),
            data_type: DataType::Number(NumberDataType::Int64),
            display_name: "payload:value".to_string(),
        });
        let projected_expr = probe_expr
            .project_column_ref(|name| projected_schema.index_of(name))
            .unwrap();
        assert_eq!(
            projected_expr.column_refs().into_keys().collect::<Vec<_>>(),
            vec![0]
        );
    }

    #[test]
    fn test_row_offset_cursor() {
        let mut contiguous = RowOffsetCursor::new(None);
        assert_eq!(contiguous.take(3).iter().collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(contiguous.take(2).iter().collect::<Vec<_>>(), vec![3, 4]);

        // Set bits at: 0, 1, 2, 5, 6, 9
        let bitmap: Bitmap = [
            true, true, true, false, false, true, true, false, false, true,
        ]
        .into_iter()
        .collect();
        let mut cursor = RowOffsetCursor::new(Some(bitmap));
        assert_eq!(cursor.take(2).iter().collect::<Vec<_>>(), vec![0, 1]);
        assert_eq!(cursor.take(3).iter().collect::<Vec<_>>(), vec![2, 5, 6]);
        assert_eq!(cursor.take(1).iter().collect::<Vec<_>>(), vec![9]);

        // A sliced bitmap with runs crossing bytes: rows 1..21 and 30..37 of the slice.
        let bits: Bitmap = (0..43)
            .map(|i| (4..24).contains(&i) || (33..40).contains(&i))
            .collect();
        let mut cursor = RowOffsetCursor::new(Some(bits.sliced(3, 40)));
        assert_eq!(
            cursor.take(5).iter().collect::<Vec<_>>(),
            (1..6).collect::<Vec<_>>()
        );
        assert_eq!(
            cursor.take(17).iter().collect::<Vec<_>>(),
            (6..21).chain(30..32).collect::<Vec<_>>()
        );
        assert_eq!(
            cursor.take(5).iter().collect::<Vec<_>>(),
            (32..37).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_block_queue() {
        let mut queue = BlockQueue::default();
        let first = int64_block(vec![0, 1, 2, 3]);
        let first_ptr = int64_values_ptr(&first);
        queue.push(first);
        queue.push(int64_block(vec![4, 5]));
        queue.push(int64_block(vec![6, 7, 8]));

        // A whole front block is returned as is.
        let popped = queue.pop_front(4);
        assert_eq!(int64_values(&popped), vec![0, 1, 2, 3]);
        assert_eq!(int64_values_ptr(&popped), first_ptr);
        // Rows never come from two blocks.
        assert_eq!(int64_values(&queue.pop_front(3)), vec![4, 5]);
        assert_eq!(int64_values(&queue.pop_front(1)), vec![6]);
        assert_eq!(int64_values(&queue.pop_front(5)), vec![7, 8]);
        assert!(queue.is_empty());
    }

    // Remain batches that do not line up with the kept prewhere blocks are sliced at the
    // kept block boundaries.
    #[test]
    fn test_remain_assembler_slices_at_kept_blocks() {
        let mut queue = BlockQueue::default();
        queue.push(int64_block(vec![0, 1]));
        queue.push(int64_block(vec![2, 3, 4]));
        queue.push(int64_block(vec![5]));
        let mut assembler = RemainAssembler::Merge {
            kept_pre: Some(queue),
            sources: vec![ColumnSource::Remain(0), ColumnSource::Pre(0)],
        };

        let mut out = vec![];
        let remain = int64_block(vec![10, 11, 12, 13]);
        let remain_ptr = int64_values_ptr(&remain);
        assembler.assemble(remain, |block| out.push(block));
        assembler.assemble(int64_block(vec![14, 15]), |block| out.push(block));
        assembler.assert_drained();

        let columns = |block: &DataBlock| {
            (0..block.num_columns())
                .map(|i| int64_column_values(block, i))
                .collect::<Vec<_>>()
        };
        assert_eq!(out.iter().map(columns).collect::<Vec<_>>(), vec![
            vec![vec![10, 11], vec![0, 1]],
            vec![vec![12, 13], vec![2, 3]],
            vec![vec![14], vec![4]],
            vec![vec![15], vec![5]],
        ]);
        // The remain batch is sliced, not copied.
        assert_eq!(int64_values_ptr(&out[0]), remain_ptr);
    }

    fn int64_block(values: Vec<i64>) -> DataBlock {
        use databend_common_expression::FromData;
        use databend_common_expression::types::Int64Type;
        DataBlock::new_from_columns(vec![Int64Type::from_data(values)])
    }

    fn int64_column_values(block: &DataBlock, index: usize) -> Vec<i64> {
        use databend_common_expression::types::AccessType;
        use databend_common_expression::types::Int64Type;
        Int64Type::try_downcast_column(&block.get_by_offset(index).to_column())
            .unwrap()
            .iter()
            .copied()
            .collect()
    }

    fn int64_values(block: &DataBlock) -> Vec<i64> {
        int64_column_values(block, 0)
    }

    fn int64_values_ptr(block: &DataBlock) -> *const i64 {
        use databend_common_expression::types::AccessType;
        use databend_common_expression::types::Int64Type;
        Int64Type::try_downcast_column(&block.get_by_offset(0).to_column())
            .unwrap()
            .as_ptr()
    }
}
