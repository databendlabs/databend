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
fn split_conjuncts(expr: &Expr<String>) -> Vec<Expr<String>> {
    match expr {
        Expr::FunctionCall(FunctionCall { function, args, .. })
            if matches!(function.signature.name.as_str(), "and" | "and_filters")
                && expr.data_type() == &DataType::Boolean =>
        {
            args.iter().flat_map(split_conjuncts).collect()
        }
        _ => vec![expr.clone()],
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

/// The first stage of a prewhere that has columns only its filter reads: the conjuncts that
/// reference only such columns are evaluated on them first, and the other prewhere columns
/// are then decoded for the rows that pass only.
struct EarlyStage {
    reader: Arc<BlockReader>,
    column_ids: HashSet<ColumnId>,
    filter: Expr,
    selector: Option<FilterExecutor>,
}

struct EarlyStagePlan {
    /// The columns of the early stage.
    projection: Projection,
    early_conjuncts: Vec<Expr<String>>,
    late_conjuncts: Vec<Expr<String>>,
}

/// The prewhere columns and conjuncts of the early stage, if the prewhere has any: the leading
/// conjuncts whose columns are neither output, nor read by a runtime filter or by a later
/// conjunct. `None` unless the later prewhere stage still reads columns.
fn plan_early_stage(
    prewhere_info: &PrewhereInfo,
    schema: &databend_common_expression::TableSchema,
    runtime_filter_columns: &[&String],
    preread_projection: &Projection,
) -> Result<Option<EarlyStagePlan>> {
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
    let output: HashSet<String> = prewhere_info
        .output_columns
        .project_schema(schema)
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let conjuncts: Vec<(Expr<String>, HashSet<String>)> = split_conjuncts(&filter)
        .into_iter()
        .map(|conjunct| {
            let columns = conjunct.column_refs().into_keys().collect();
            (conjunct, columns)
        })
        .collect();
    let qualifies = |columns: &HashSet<String>| {
        !columns.is_empty()
            && columns.iter().all(|name| {
                // Internal columns, e.g. `_row_id`, are not in the schema.
                schema.index_of(name).is_ok()
                    && !output.contains(name)
                    && !runtime_filter_columns.contains(&name)
            })
    };
    // The early conjuncts are a prefix, so that the conjuncts are evaluated in their order: a
    // conjunct may fail on rows an earlier one rejects (e.g. `b <> 0 AND 3 / b = 1`). A column
    // a later conjunct reads ends the prefix.
    let mut prefix = conjuncts
        .iter()
        .take_while(|(_, columns)| qualifies(columns))
        .count();
    loop {
        let later: HashSet<&String> = conjuncts[prefix..]
            .iter()
            .flat_map(|(_, columns)| columns)
            .collect();
        let end = conjuncts[..prefix]
            .iter()
            .position(|(_, columns)| columns.iter().any(|name| later.contains(name)))
            .unwrap_or(prefix);
        if end == prefix {
            break;
        }
        prefix = end;
    }
    let early_columns: Vec<&String> = conjuncts[..prefix]
        .iter()
        .flat_map(|(_, columns)| columns)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if early_columns.is_empty() {
        return Ok(None);
    }
    let early_projection = Projection::from_column_names(schema, &early_columns)?;
    let late_projection = preread_projection.difference(&early_projection);
    if late_projection.is_empty() {
        return Ok(None);
    }
    let mut early_conjuncts = Vec::with_capacity(prefix);
    let mut late_conjuncts = Vec::with_capacity(conjuncts.len() - prefix);
    for (i, (conjunct, _)) in conjuncts.into_iter().enumerate() {
        if i < prefix {
            early_conjuncts.push(conjunct);
        } else {
            late_conjuncts.push(conjunct);
        }
    }
    Ok(Some(EarlyStagePlan {
        projection: early_projection,
        early_conjuncts,
        late_conjuncts,
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
            Some(prewhere_info) if !use_single_prewhere_reader => plan_early_stage(
                prewhere_info,
                original_schema,
                &runtime_filter_column_names,
                &preread_projection,
            )?,
            _ => None,
        };
        let mut late_conjuncts = None;
        let mut early_plan = None;
        if let Some(plan) = staged {
            preread_projection = preread_projection.difference(&plan.projection);
            late_conjuncts = Some(plan.late_conjuncts);
            early_plan = Some((plan.projection, plan.early_conjuncts));
        }
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

        let prewhere_filter = match (prewhere_info, late_conjuncts) {
            (_, Some(conjuncts)) if conjuncts.is_empty() => None,
            (_, Some(conjuncts)) => Some(conjunction(conjuncts)?),
            (Some(prewhere_info), None) => Some(prewhere_info.filter.as_expr(&BUILTIN_FUNCTIONS)),
            (None, None) => None,
        };
        let prewhere_filter = prewhere_filter
            .map(|filter| {
                filter.project_column_ref(|name| {
                    Ok(prewhere_schema.column_with_name(name).unwrap().0)
                })
            })
            .transpose()?;

        let func_ctx = ctx.get_function_context()?;
        let max_block_size = (ctx.get_settings().get_max_block_size()? as usize).max(1);
        let mut output_schema = block_reader.data_schema();
        let early = early_plan
            .map(|(projection, conjuncts)| -> Result<EarlyStage> {
                let reader = block_reader.change_projection(projection)?;
                let schema = reader.data_schema();
                let filter = conjunction(conjuncts)?
                    .project_column_ref(|name| Ok(schema.column_with_name(name).unwrap().0))?;
                // The early columns are not output.
                output_schema = DataSchema::new(
                    output_schema
                        .fields()
                        .iter()
                        .filter(|field| schema.column_with_name(field.name()).is_none())
                        .cloned()
                        .collect(),
                );
                Ok(EarlyStage {
                    column_ids: reader.schema().to_leaf_column_id_set(),
                    selector: prewhere_selector(&filter, &func_ctx, max_block_size),
                    reader,
                    filter,
                })
            })
            .transpose()?;
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
        let early_selection = match self.early.as_mut() {
            Some(early) => {
                let (selection, bytes) =
                    filter_early_stage(early, &self.func_ctx, part, &columns_chunks)?;
                scan_stats.bytes += bytes;
                Some(selection)
            }
            None => None,
        };
        let (mut preread_block, num_rows) = match &early_selection {
            None => {
                let block = self
                    .pre_reader
                    .deserialize_part(part, pre_columns_chunks, None)?;
                scan_stats.bytes += block.memory_size();
                (block, part.nums_rows)
            }
            Some(selection) => {
                let rows = selection.len() - selection.null_count();
                if rows == 0 {
                    // Nothing else is decoded: account for the chunks that were read, as when
                    // a pushed down selection decodes no row.
                    scan_stats.bytes += raw_bytes_of_chunks(&pre_columns_chunks)
                        + raw_bytes_of_chunks(&Self::filter_column_chunks(
                            &columns_chunks,
                            &self.remain_column_ids,
                        )?);
                    return Ok(ReadStateOutput {
                        block: DataBlock::empty_with_schema(&self.output_schema),
                        bitmap_selection: Some(selection.clone()),
                        scan_stats,
                    });
                }
                let block = if should_push_down_row_selection(
                    selection,
                    self.prewhere_selectivity_threshold,
                ) {
                    let raw_bytes = raw_bytes_of_chunks(&pre_columns_chunks);
                    let block = self.pre_reader.deserialize_part(
                        part,
                        pre_columns_chunks,
                        Some(&RowSelection::from(selection)),
                    )?;
                    scan_stats.bytes += estimate_block_bytes(
                        block.memory_size(),
                        block.num_rows(),
                        part.nums_rows,
                        raw_bytes,
                    );
                    block
                } else {
                    let block = self
                        .pre_reader
                        .deserialize_part(part, pre_columns_chunks, None)?;
                    scan_stats.bytes += block.memory_size();
                    block.filter_with_bitmap(selection)?
                };
                (block, rows)
            }
        };

        let filter_bitmap = self.filter(&preread_block, num_rows)?;
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
        // The early stage applied the pruner's selection.
        let bitmap_selection = match (bitmap_selection, early_selection.is_none()) {
            (Some(selection), true) => match selected_rows(part) {
                Some(rows) => Some(&selection & &rows),
                None => Some(selection),
            },
            (None, true) => selected_rows(part),
            (selection, false) => selection,
        };

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

        let mut merged_fields = self.pre_reader.data_fields();
        merged_fields.extend(self.remain_reader.data_fields());
        let merged_schema = DataSchema::new(merged_fields);

        preread_block.merge_block(remain_block);

        let data_block = preread_block.resort(&merged_schema, &self.output_schema)?;

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

/// The rows of the block that the early stage selects, with the pruner's selection, and the
/// bytes it decoded. The early columns are not output: they are decoded in batches when
/// possible, see [`BlockReader::parquet_filter_batches`].
fn filter_early_stage(
    early: &mut EarlyStage,
    func_ctx: &FunctionContext,
    part: &FuseBlockPartInfo,
    columns_chunks: &HashMap<ColumnId, DataItem>,
) -> Result<(Bitmap, usize)> {
    let chunks = ReadState::filter_column_chunks(columns_chunks, &early.column_ids)?;
    let mut bytes = 0;
    let batches = if part.nums_rows > FILTER_BATCH_ROWS {
        early
            .reader
            .parquet_filter_batches(part, &chunks, FILTER_BATCH_ROWS)?
    } else {
        None
    };
    let bitmap: Bitmap = match batches {
        Some(batches) => {
            let mut bits = MutableBitmap::with_capacity(part.nums_rows);
            for batch in batches {
                let batch = batch?;
                // Adjacent batches can reference the same page; count logical decoded bytes.
                bytes += batch
                    .columns()
                    .iter()
                    .map(|entry| entry.memory_size_with_options(true))
                    .sum::<usize>();
                let bitmap = eval_filter(
                    &early.filter,
                    early.selector.as_mut(),
                    func_ctx,
                    &batch,
                    batch.num_rows(),
                )?;
                bits.extend_from_bitmap(&bitmap.into());
            }
            bits.into()
        }
        None => {
            let block = early.reader.deserialize_part(part, chunks.clone(), None)?;
            bytes += block.memory_size();
            eval_filter(
                &early.filter,
                early.selector.as_mut(),
                func_ctx,
                &block,
                part.nums_rows,
            )?
            .into()
        }
    };
    Ok(match selected_rows(part) {
        Some(rows) => (&bitmap & &rows, bytes),
        None => (bitmap, bytes),
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

    /// `(early projection, early conjuncts, late conjuncts)` of a prewhere with `filter` over
    /// columns `a`, `b` and `s` that outputs `output`, as column names and conjunct counts.
    fn plan(
        filter: Expr<String>,
        output: &[usize],
        runtime_filter_columns: &[&String],
    ) -> Option<(Vec<String>, usize, usize)> {
        let schema = TableSchema::new(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("s", TableDataType::Number(NumberDataType::Int32)),
        ]);
        let mut names: Vec<String> = filter.column_refs().into_keys().collect();
        names.sort();
        let prewhere_columns = Projection::from_column_names(&schema, &names).unwrap();
        let info = PrewhereInfo {
            output_columns: Projection::Columns(output.to_vec()),
            prewhere_columns: prewhere_columns.clone(),
            remain_columns: Projection::Columns(vec![]),
            filter: filter.as_remote_expr(),
            virtual_column_ids: None,
        };
        plan_early_stage(&info, &schema, runtime_filter_columns, &prewhere_columns)
            .unwrap()
            .map(|plan| {
                let names = plan
                    .projection
                    .project_schema(&schema)
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect();
                (names, plan.early_conjuncts.len(), plan.late_conjuncts.len())
            })
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

        // `a` and `b` are only filtered on.
        let expected = Some((vec!["a".to_string(), "b".to_string()], 2, 1));
        assert_eq!(
            plan(
                filter(&[a_eq.clone(), b_gt.clone(), s_lt.clone()]),
                &[2],
                &[]
            ),
            expected
        );
        // Nested ANDs are split.
        let nested = filter(&[filter(&[a_eq.clone(), b_gt.clone()]), s_lt.clone()]);
        assert_eq!(plan(nested, &[2], &[]), expected);
        // An output column is not early.
        assert_eq!(
            plan(
                filter(&[a_eq.clone(), b_gt.clone(), s_lt.clone()]),
                &[1, 2],
                &[]
            ),
            Some((vec!["a".to_string()], 1, 2))
        );
        // A column a later conjunct reads ends the early conjuncts.
        let a_plus_s = call("gt", &[call("plus", &[a.clone(), s.clone()]), int(0)]);
        assert_eq!(
            plan(
                filter(&[b_gt.clone(), a_eq.clone(), a_plus_s.clone()]),
                &[2],
                &[]
            ),
            Some((vec!["b".to_string()], 1, 2))
        );
        assert_eq!(
            plan(filter(&[a_eq.clone(), b_gt.clone(), a_plus_s]), &[2], &[]),
            None
        );
        // The conjuncts keep their order: one on an output column comes first.
        assert_eq!(plan(filter(&[s_lt.clone(), a_eq.clone()]), &[2], &[]), None);
        // Nor a column a runtime filter reads.
        let b_name = "b".to_string();
        assert_eq!(
            plan(
                filter(&[a_eq.clone(), b_gt.clone(), s_lt.clone()]),
                &[2],
                &[&b_name]
            ),
            Some((vec!["a".to_string()], 1, 2))
        );
        // Without a later stage to read, there is nothing to stage.
        assert_eq!(plan(filter(&[a_eq.clone(), b_gt.clone()]), &[], &[]), None);
        // One conjunct over an output column only.
        assert_eq!(plan(s_lt.clone(), &[2], &[]), None);
        // A non-deterministic filter is evaluated as a whole.
        let random = call("gt", &[call("rand", &[]), call("to_float64", &[int(0)])]);
        assert_eq!(plan(filter(&[random, s_lt.clone()]), &[2], &[]), None);
    }

    #[test]
    fn test_expand_selection() {
        let outer: Bitmap = [false, true, true, false, true].into_iter().collect();
        let inner: Bitmap = [true, false, true].into_iter().collect();
        assert_eq!(
            expand_selection(&outer, &inner).iter().collect::<Vec<_>>(),
            vec![false, true, false, false, true]
        );
    }
}
