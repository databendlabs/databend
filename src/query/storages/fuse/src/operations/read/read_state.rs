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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::PrewhereInfo;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::runtime_filter_info::RuntimeBloomFilter;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStats;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::EvaluateOptions;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionCall;
use databend_common_expression::FunctionContext;
use databend_common_expression::SelectExpr;
use databend_common_expression::SelectExprBuilder;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MutableBitmap;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::fuse_part::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::io::DataItem;
use crate::io::RowSelection;
use crate::pruning::ExprBloomFilter;

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

/// The conjuncts of a filter, split at the non-nullable ANDs.
fn split_conjuncts(expr: &Expr<String>, conjuncts: &mut Vec<Expr<String>>) {
    match expr {
        Expr::FunctionCall(FunctionCall { function, args, .. })
            if matches!(function.signature.name.as_str(), "and" | "and_filters")
                && expr.data_type() == &DataType::Boolean =>
        {
            for arg in args {
                split_conjuncts(arg, conjuncts);
            }
        }
        _ => conjuncts.push(expr.clone()),
    }
}

/// A non-nullable boolean filter of the conjuncts.
fn conjunction(mut conjuncts: Vec<Expr<String>>) -> Result<Expr<String>> {
    if conjuncts.len() == 1 && conjuncts[0].data_type() == &DataType::Boolean {
        return Ok(conjuncts.pop().unwrap());
    }
    let name = if conjuncts.len() == 1 {
        "is_true"
    } else {
        "and_filters"
    };
    check_function(None, name, &[], &conjuncts, &BUILTIN_FUNCTIONS)
}

/// The first stage of a prewhere: the leading conjuncts are evaluated on their columns first,
/// and the other prewhere columns are then decoded for the rows that pass only. The early
/// columns that the later stages or the output read are kept for those rows, not decoded again.
struct EarlyStage {
    reader: Arc<BlockReader>,
    column_ids: HashSet<ColumnId>,
    /// Offsets in the early reader's block, in its column order.
    retained_indices: BTreeSet<FieldIndex>,
    filter: Expr,
    selector: Option<FilterExecutor>,
}

struct EarlyStagePlan {
    /// The columns of the early stage.
    projection: Projection,
    /// Offsets in `projection` of the early columns that are kept after the early stage.
    retained_indices: BTreeSet<FieldIndex>,
    filter: Expr<String>,
    /// The conjuncts left to the later stage, if any.
    late_filter: Option<Expr<String>>,
}

/// The early stage of a prewhere, if it has one: the longest prefix of the conjuncts that
/// leaves some prewhere columns to the later stage. `None` if the first conjunct already
/// reads all of them.
fn plan_early_stage(
    prewhere_info: &PrewhereInfo,
    schema: &databend_common_expression::TableSchema,
    runtime_filter_columns: &[&String],
    preread_projection: &Projection,
) -> Result<Option<EarlyStagePlan>> {
    let (
        Projection::Columns(pre_columns),
        Projection::Columns(output_columns),
        Projection::Columns(remain_columns),
    ) = (
        preread_projection,
        &prewhere_info.output_columns,
        &prewhere_info.remain_columns,
    )
    else {
        return Ok(None);
    };
    if prewhere_info
        .virtual_column_ids
        .as_ref()
        .is_some_and(|ids| !ids.is_empty())
        || !matches!(prewhere_info.prewhere_columns, Projection::Columns(_))
    {
        return Ok(None);
    }
    let filter = prewhere_info.filter.as_expr(&BUILTIN_FUNCTIONS);
    if !filter.is_deterministic(&BUILTIN_FUNCTIONS) {
        return Ok(None);
    }

    // The early conjuncts are a prefix, so that the conjuncts are evaluated in their order: a
    // conjunct may fail on rows an earlier one rejects (e.g. `b <> 0 AND 3 / b = 1`). Internal
    // columns, e.g. `_row_id`, are not in the schema and end the prefix.
    let mut conjuncts = Vec::new();
    split_conjuncts(&filter, &mut conjuncts);
    let column_index = |name: &String| {
        let (index, _) = schema.column_with_name(name)?;
        pre_columns.contains(&index).then_some(index)
    };
    let mut early_columns = BTreeSet::new();
    let mut prefix = 0;
    for conjunct in &conjuncts {
        let refs = conjunct.column_refs();
        let Some(new_columns) = refs.keys().try_fold(0, |count, name| {
            let index = column_index(name)?;
            Some(count + usize::from(!early_columns.contains(&index)))
        }) else {
            break;
        };
        // Stop before a conjunct that would leave no prewhere column to the later stage.
        if refs.is_empty() || early_columns.len() + new_columns == pre_columns.len() {
            break;
        }
        early_columns.extend(refs.keys().filter_map(column_index));
        prefix += 1;
    }
    if prefix == 0 {
        return Ok(None);
    }

    let late_conjuncts = conjuncts.split_off(prefix);
    let late_filter = (!late_conjuncts.is_empty())
        .then(|| conjunction(late_conjuncts))
        .transpose()?;
    let late_columns = late_filter
        .as_ref()
        .map(|filter| filter.column_refs())
        .unwrap_or_default();
    // `remain_columns` can contain physical sources of virtual output columns.
    let retained_indices = early_columns
        .iter()
        .enumerate()
        .filter(|(_, index)| {
            let name = schema.field(**index).name();
            output_columns.contains(index)
                || remain_columns.contains(index)
                || late_columns.contains_key(name)
                || runtime_filter_columns.contains(&name)
        })
        .map(|(offset, _)| offset)
        .collect();
    Ok(Some(EarlyStagePlan {
        projection: Projection::Columns(early_columns.into_iter().collect()),
        retained_indices,
        filter: conjunction(conjuncts)?,
        late_filter,
    }))
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

pub struct ReadState {
    /// See [`EarlyStage`]; `pre_reader` and `filters` are then the later prewhere stage.
    early: Option<EarlyStage>,
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
    merged_schema: DataSchema,
    pub prewhere_selectivity_threshold: u64,
    pub use_single_prewhere_reader: bool,
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
        let staged = match prewhere_info {
            Some(prewhere_info)
                if !use_single_prewhere_reader
                    && matches!(block_reader.projection, Projection::Columns(_)) =>
            {
                plan_early_stage(
                    prewhere_info,
                    original_schema,
                    &runtime_filter_column_names,
                    &preread_projection,
                )?
            }
            _ => None,
        };
        if let Some(plan) = staged.as_ref() {
            preread_projection = preread_projection.difference(&plan.projection);
        }
        let prewhere_reader = if use_single_prewhere_reader {
            block_reader.clone()
        } else {
            block_reader.change_projection(preread_projection)?
        };
        let remain_reader = block_reader.change_projection(remain_projection)?;
        let pre_column_ids = prewhere_reader.schema().to_leaf_column_id_set();
        let remain_column_ids = remain_reader.schema().to_leaf_column_id_set();

        let func_ctx = ctx.get_function_context()?;
        let max_block_size = (ctx.get_settings().get_max_block_size()? as usize).max(1);
        let mut output_schema = block_reader.data_schema();
        let mut prewhere_fields = prewhere_reader.data_fields();
        let (early, late_filter) = if let Some(plan) = staged {
            let reader = block_reader.change_projection(plan.projection)?;
            let schema = reader.data_schema();
            let filter = plan
                .filter
                .project_column_ref(|name| schema.index_of(name))?;
            prewhere_fields.extend(
                plan.retained_indices
                    .iter()
                    .map(|&offset| schema.fields()[offset].clone()),
            );
            output_schema = output_schema.project_by_fields(
                output_schema
                    .fields()
                    .iter()
                    .filter(|field| {
                        schema
                            .column_with_name(field.name())
                            .is_none_or(|(offset, _)| plan.retained_indices.contains(&offset))
                    })
                    .cloned()
                    .collect(),
            );
            let early = EarlyStage {
                column_ids: reader.schema().to_leaf_column_id_set(),
                retained_indices: plan.retained_indices,
                selector: prewhere_selector(&filter, &func_ctx, max_block_size),
                reader,
                filter,
            };
            (Some(early), Some(plan.late_filter))
        } else {
            (None, None)
        };
        // Order matches the block: newly decoded late columns, followed by retained early columns.
        let prewhere_schema = DataSchema::new(prewhere_fields);

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

        let prewhere_filter = match late_filter {
            Some(late_filter) => late_filter,
            None => prewhere_info.map(|info| info.filter.as_expr(&BUILTIN_FUNCTIONS)),
        };
        let prewhere_filter = prewhere_filter
            .map(|filter| filter.project_column_ref(|name| prewhere_schema.index_of(name)))
            .transpose()?;
        let mut merged_fields = prewhere_schema.fields().clone();
        merged_fields.extend(remain_reader.data_fields());
        let merged_schema = DataSchema::new(merged_fields);
        let prewhere_selector = prewhere_filter
            .as_ref()
            .and_then(|filter| prewhere_selector(filter, &func_ctx, max_block_size));
        let filters_deterministic = prewhere_filter
            .as_ref()
            .is_some_and(|filter| filter.is_deterministic(&BUILTIN_FUNCTIONS));

        Ok(Self {
            early,
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
            merged_schema,
            prewhere_selectivity_threshold,
            use_single_prewhere_reader,
        })
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
        let bloom_start = Instant::now();

        let mut bitmaps = vec![];
        let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        for runtime_filter in &self.runtime_filters {
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

    pub fn deserialize_and_filter(
        &mut self,
        columns_chunks: HashMap<ColumnId, DataItem>,
        part: &FuseBlockPartInfo,
        no_output_columns: bool,
    ) -> Result<ReadStateOutput> {
        let pre_columns_chunks = Self::filter_column_chunks(&columns_chunks, &self.pre_column_ids)?;
        if no_output_columns
            && self.early.is_none()
            && self.filters_deterministic
            && self.runtime_filters.is_empty()
            && self.remain_column_ids.is_empty()
            && part.nums_rows > FILTER_BATCH_ROWS
            && let Some(batches) = self.pre_reader.clone().parquet_filter_batches(
                part,
                &pre_columns_chunks,
                FILTER_BATCH_ROWS,
            )?
        {
            let mut bytes = 0;
            let mut bits = MutableBitmap::with_capacity(part.nums_rows);
            for batch in batches {
                let batch = batch?;
                // Adjacent batches can reference the same page; count logical decoded bytes.
                bytes += batch
                    .columns()
                    .iter()
                    .map(|entry| entry.memory_size_with_options(true))
                    .sum::<usize>();
                let bitmap = self.filter(&batch, batch.num_rows())?.unwrap();
                bits.extend_from_bitmap(&bitmap.into());
            }
            // Apply the pruner's selection in original block coordinates.
            let mut bitmap: Bitmap = bits.into();
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

        let mut scan_stats = BlockScanStats {
            rows: part.nums_rows,
            bytes: 0,
        };
        // The early stage selects the rows that the later prewhere stage decodes and filters:
        // `num_rows` is then the number of these rows.
        let (early_selection, retained_early) = match self.early.as_mut() {
            Some(early) => {
                let EarlyStageOutput {
                    selection,
                    retained,
                    bytes,
                } = filter_early_stage(early, &self.func_ctx, part, &columns_chunks)?;
                scan_stats.bytes += bytes;
                (Some(selection), retained)
            }
            None => (None, None),
        };
        let num_rows = early_selection
            .as_ref()
            .map_or(part.nums_rows, |selection| {
                selection.len() - selection.null_count()
            });
        if let Some(selection) = &early_selection
            && num_rows == 0
        {
            // Nothing else is decoded: account for the chunks that were read, as when a pushed
            // down selection decodes no row.
            scan_stats.bytes += raw_bytes_of_chunks(&pre_columns_chunks)
                + columns_chunks
                    .iter()
                    .filter(|(id, _)| self.remain_column_ids.contains(id))
                    .map(|(_, item)| raw_bytes_of_item(item))
                    .sum::<usize>();
            return Ok(ReadStateOutput {
                block: DataBlock::empty_with_schema(&self.output_schema),
                bitmap_selection: Some(selection.clone()),
                scan_stats,
            });
        }
        let mut preread_block = deserialize_selected(
            &self.pre_reader,
            part,
            pre_columns_chunks,
            early_selection.as_ref(),
            self.prewhere_selectivity_threshold,
            &mut scan_stats.bytes,
        )?;

        if let Some(retained) = retained_early {
            // Both sides are already in early-selection row order.
            preread_block.merge_block(retained);
        }
        let filter_bitmap = self.filter(&preread_block, num_rows)?.map(Bitmap::from);
        let runtime_filter_bitmap = match &filter_bitmap {
            // Expensive probe expressions are evaluated only for rows surviving the static
            // prewhere predicate. Expand their bitmap back to the rows of `preread_block`.
            Some(filter_bitmap) if !self.runtime_filters.is_empty() => {
                let filtered_block = preread_block.clone().filter_with_bitmap(filter_bitmap)?;
                self.runtime_filter(&filtered_block, filtered_block.num_rows())?
                    .map(|bitmap| expand_selection(filter_bitmap, &bitmap.into()))
            }
            // Also called without Bloom filters, to preserve the profile entry emitted before
            // expression probes were supported.
            _ => self
                .runtime_filter(&preread_block, num_rows)?
                .map(Bitmap::from),
        };

        let mut bitmap_selection = and_selection(filter_bitmap, runtime_filter_bitmap);
        // The early stage applied the pruner's selection.
        if early_selection.is_none() {
            bitmap_selection = and_selection(bitmap_selection, selected_rows(part));
        }

        if let Some(ref bitmap) = bitmap_selection {
            preread_block = preread_block.filter_with_bitmap(bitmap)?;
        }
        // Back to the rows of the block.
        let bitmap_selection = match (early_selection, bitmap_selection) {
            (Some(early), Some(later)) => Some(expand_selection(&early, &later)),
            (Some(early), None) => Some(early),
            (None, selection) => selection,
        };

        if self.use_single_prewhere_reader {
            return Ok(ReadStateOutput {
                block: preread_block,
                bitmap_selection,
                scan_stats,
            });
        }

        let remain_columns_chunks =
            Self::filter_column_chunks(&columns_chunks, &self.remain_column_ids)?;
        let remain_block = deserialize_selected(
            &self.remain_reader,
            part,
            remain_columns_chunks,
            bitmap_selection.as_ref(),
            self.prewhere_selectivity_threshold,
            &mut scan_stats.bytes,
        )?;

        preread_block.merge_block(remain_block);

        let data_block = preread_block.resort(&self.merged_schema, &self.output_schema)?;

        Ok(ReadStateOutput {
            block: data_block,
            bitmap_selection,
            scan_stats,
        })
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

const FILTER_BATCH_ROWS: usize = 8192;

struct EarlyStageOutput {
    selection: Bitmap,
    retained: Option<DataBlock>,
    bytes: usize,
}

/// The rows of the block that the early stage selects, with the pruner's selection, the
/// retained early columns of these rows, and the bytes it decoded. The early columns are
/// decoded in batches when possible, see [`BlockReader::parquet_filter_batches`].
fn filter_early_stage(
    early: &mut EarlyStage,
    func_ctx: &FunctionContext,
    part: &FuseBlockPartInfo,
    columns_chunks: &HashMap<ColumnId, DataItem>,
) -> Result<EarlyStageOutput> {
    let chunks = ReadState::filter_column_chunks(columns_chunks, &early.column_ids)?;
    let mut bytes = 0;
    let mut bits = MutableBitmap::with_capacity(part.nums_rows);
    let mut retained = Vec::new();
    let pruned = selected_rows(part);
    let mut filter_batch = |block: DataBlock| -> Result<()> {
        let bitmap: Bitmap = eval_filter(
            &early.filter,
            early.selector.as_mut(),
            func_ctx,
            &block,
            block.num_rows(),
        )?
        .into();
        let bitmap = match &pruned {
            Some(rows) => &bitmap & &rows.clone().sliced(bits.len(), bitmap.len()),
            None => bitmap,
        };
        if !early.retained_indices.is_empty() && bitmap.null_count() < bitmap.len() {
            let block = block.project(&early.retained_indices);
            retained.push(block.filter_with_bitmap(&bitmap)?);
        }
        bits.extend_from_bitmap(&bitmap);
        Ok(())
    };
    if part.nums_rows > FILTER_BATCH_ROWS
        && let Some(batches) =
            early
                .reader
                .parquet_filter_batches(part, &chunks, FILTER_BATCH_ROWS)?
    {
        for batch in batches {
            let batch = batch?;
            // Adjacent batches can reference the same page; count logical decoded bytes.
            bytes += batch
                .columns()
                .iter()
                .map(|entry| entry.memory_size_with_options(true))
                .sum::<usize>();
            filter_batch(batch)?;
        }
    } else {
        let block = early.reader.deserialize_part(part, chunks, None)?;
        bytes += block.memory_size();
        filter_batch(block)?;
    }
    let retained = if retained.len() > 1 {
        Some(DataBlock::concat(&retained)?)
    } else {
        retained.pop()
    };
    Ok(EarlyStageOutput {
        selection: bits.into(),
        retained,
        bytes,
    })
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

/// Decodes `chunks` for the rows `selection` sets and adds the bytes of the whole block to
/// `bytes`. A selective enough selection is pushed down to the decoder; otherwise the block is
/// decoded and then filtered.
fn deserialize_selected(
    reader: &BlockReader,
    part: &FuseBlockPartInfo,
    chunks: HashMap<ColumnId, DataItem>,
    selection: Option<&Bitmap>,
    threshold: u64,
    bytes: &mut usize,
) -> Result<DataBlock> {
    if let Some(selection) =
        selection.filter(|selection| should_push_down_row_selection(selection, threshold))
    {
        let raw_bytes = raw_bytes_of_chunks(&chunks);
        let block = reader.deserialize_part(part, chunks, Some(&RowSelection::from(selection)))?;
        *bytes += estimate_block_bytes(
            block.memory_size(),
            block.num_rows(),
            part.nums_rows,
            raw_bytes,
        );
        return Ok(block);
    }
    let block = reader.deserialize_part(part, chunks, None)?;
    *bytes += block.memory_size();
    match selection {
        Some(selection) => block.filter_with_bitmap(selection),
        None => Ok(block),
    }
}

/// The rows both selections set, where `None` sets every row.
fn and_selection(lhs: Option<Bitmap>, rhs: Option<Bitmap>) -> Option<Bitmap> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(&lhs & &rhs),
        (selection, None) | (None, selection) => selection,
    }
}

/// Size of the column chunks as they were read, before decoding.
fn raw_bytes_of_chunks(columns_chunks: &HashMap<ColumnId, DataItem>) -> usize {
    columns_chunks.values().map(raw_bytes_of_item).sum()
}

fn raw_bytes_of_item(item: &DataItem) -> usize {
    match item {
        DataItem::RawData(buffer) => buffer.len(),
        DataItem::ColumnArray(array) => array.1,
    }
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
    fn test_expand_selection() {
        let outer: Bitmap = [false, true, true, false, true].into_iter().collect();
        let inner: Bitmap = [true, false, true].into_iter().collect();

        let expanded = expand_selection(&outer, &inner);
        assert_eq!(expanded.iter().collect::<Vec<_>>(), vec![
            false, true, false, false, true
        ]);
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

    fn column(name: &str, data_type: DataType) -> Expr<String> {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id: name.to_string(),
            data_type,
            display_name: name.to_string(),
        })
    }

    fn call(name: &str, args: &[Expr<String>]) -> Expr<String> {
        check_function(None, name, &[], args, &BUILTIN_FUNCTIONS).unwrap()
    }

    fn int(value: i32) -> Expr<String> {
        Expr::Constant(databend_common_expression::Constant {
            span: None,
            scalar: databend_common_expression::Scalar::Number(value.into()),
            data_type: DataType::Number(NumberDataType::Int32),
        })
    }

    /// Early columns, retained early columns, and the columns of the late filter.
    fn plan(
        filter: Expr<String>,
        output: &[usize],
        runtime_filter_columns: &[&String],
    ) -> Option<(Vec<String>, Vec<String>, Vec<String>)> {
        let schema = TableSchema::new(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("s", TableDataType::Number(NumberDataType::Int32)),
        ]);
        let names: Vec<String> = filter.column_refs().into_keys().collect();
        let info = PrewhereInfo {
            output_columns: Projection::Columns(output.to_vec()),
            prewhere_columns: Projection::from_column_names(&schema, &names).unwrap(),
            remain_columns: Projection::Columns(vec![]),
            filter: filter.as_remote_expr(),
            virtual_column_ids: None,
        };
        let mut preread = Projection::from_column_names(&schema, runtime_filter_columns).unwrap();
        preread.merge(&info.prewhere_columns);
        let plan = plan_early_stage(&info, &schema, runtime_filter_columns, &preread).unwrap()?;
        let schema = plan.projection.project_schema(&schema);
        let names = schema.fields().iter().map(|f| f.name().clone()).collect();
        let retained = plan
            .retained_indices
            .iter()
            .map(|&index| schema.field(index).name().clone())
            .collect();
        let mut late: Vec<String> = plan
            .late_filter
            .map(|filter| filter.column_refs().into_keys().collect())
            .unwrap_or_default();
        late.sort();
        Some((names, retained, late))
    }

    #[test]
    fn test_plan_early_stage() {
        let int32 = DataType::Number(NumberDataType::Int32);
        let a = column("a", int32.clone());
        let b = column("b", int32.clone());
        let s = column("s", int32.clone());
        let a_eq = call("eq", &[a.clone(), int(1)]);
        let b_gt = call("gt", &[b.clone(), int(2)]);
        let s_lt = call("lt", &[s.clone(), int(3)]);
        let filter = |conjuncts: &[Expr<String>]| call("and_filters", conjuncts);

        let ab = vec!["a".to_string(), "b".to_string()];
        let only_s = vec!["s".to_string()];
        let clauses = filter(&[a_eq.clone(), b_gt.clone(), s_lt.clone()]);
        assert_eq!(
            plan(clauses.clone(), &[2], &[]),
            Some((ab.clone(), vec![], only_s.clone()))
        );
        let nested = filter(&[filter(&[a_eq.clone(), b_gt.clone()]), s_lt.clone()]);
        assert_eq!(
            plan(nested, &[2], &[]),
            Some((ab.clone(), vec![], only_s.clone()))
        );

        // Output and runtime filter dependencies keep early columns, without moving the split.
        let retained_b = Some((ab.clone(), vec!["b".to_string()], only_s));
        assert_eq!(plan(clauses.clone(), &[1, 2], &[]), retained_b);
        assert_eq!(plan(clauses, &[2], &[&"b".to_string()]), retained_b);

        // Also keep columns shared with a late conjunct.
        let a_plus_s = call("gt", &[call("plus", &[a.clone(), s.clone()]), int(0)]);
        assert_eq!(
            plan(filter(&[a_eq.clone(), b_gt.clone(), a_plus_s]), &[2], &[]),
            Some((ab.clone(), vec!["a".to_string()], vec![
                "a".to_string(),
                "s".to_string()
            ]))
        );
        // A new runtime filter column is enough to justify a late read.
        assert_eq!(
            plan(filter(&[a_eq.clone(), b_gt]), &[], &[&"s".to_string()]),
            Some((ab, vec![], vec![]))
        );
        // The first conjunct already reads every prewhere column. Do not move the second up.
        let a_plus_s = call("gt", &[call("plus", &[s.clone(), a.clone()]), int(0)]);
        assert_eq!(plan(filter(&[a_plus_s, a_eq]), &[2], &[]), None);
        assert_eq!(plan(s_lt.clone(), &[2], &[]), None);
        // A non-deterministic filter is evaluated as a whole.
        let random = call("gt", &[call("rand", &[]), call("to_float64", &[int(0)])]);
        assert_eq!(plan(filter(&[random, s_lt.clone()]), &[2], &[]), None);
    }
}
