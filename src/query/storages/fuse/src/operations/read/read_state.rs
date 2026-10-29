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
use databend_common_expression::FunctionContext;
use databend_common_expression::SelectExprBuilder;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
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
    pub pre_reader: Arc<BlockReader>,
    pub remain_reader: Arc<BlockReader>,
    pub filters: Option<Expr>,
    filter_executor: Option<FilterExecutor>,
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
        let filter_executor = prewhere_filter.as_ref().and_then(|filter| {
            if !func_ctx.enable_selector_executor {
                return None;
            }

            // Keep bitmap evaluation for predicates that are already cheap to vectorize. Direct
            // string-scalar equality has a specialized selector path that avoids that overhead.
            let (select_expr, _) = SelectExprBuilder::new(&BUILTIN_FUNCTIONS)
                .build(filter)
                .into();
            select_expr.contains_string_column_eq_constant().then(|| {
                FilterExecutor::new(
                    filter.clone(),
                    func_ctx.clone(),
                    max_block_size,
                    None,
                    &BUILTIN_FUNCTIONS,
                    false,
                )
            })
        });

        Ok(Self {
            pre_reader: prewhere_reader,
            remain_reader,
            filters: prewhere_filter,
            filter_executor,
            runtime_filters,
            pre_column_ids,
            remain_column_ids,
            func_ctx,
            output_schema: block_reader.data_schema(),
            prewhere_selectivity_threshold,
            use_single_prewhere_reader,
        })
    }

    pub fn filter(&mut self, block: &DataBlock, num_rows: usize) -> Result<Option<MutableBitmap>> {
        if let Some(ref f) = self.filters {
            if let Some(filter_executor) = self.filter_executor.as_mut() {
                return Ok(Some(filter_with_selector(
                    filter_executor,
                    block,
                    num_rows,
                )?));
            }

            let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);
            let filter_result = evaluator.run(f)?.try_downcast::<BooleanType>().unwrap();
            Ok(Some(FilterHelpers::filter_to_bitmap(
                filter_result,
                num_rows,
            )))
        } else {
            Ok(None)
        }
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
    ) -> Result<ReadStateOutput> {
        let pre_columns_chunks = Self::filter_column_chunks(&columns_chunks, &self.pre_column_ids)?;
        let mut preread_block = self
            .pre_reader
            .deserialize_part(part, pre_columns_chunks, None)?;
        let mut scan_stats = BlockScanStats {
            rows: part.nums_rows,
            bytes: preread_block.memory_size(),
        };

        let filter_bitmap = self.filter(&preread_block, part.nums_rows)?;
        // Expensive probe expressions are evaluated only for rows surviving
        // the static prewhere predicate. Expand their bitmap back to the
        // original row positions before combining it with prewhere.
        let runtime_filter_bitmap = if self.runtime_filters.is_empty() {
            // Preserve the profile entry emitted before expression probes were
            // supported, even when there is no Bloom filter to evaluate.
            self.runtime_filter(&preread_block, part.nums_rows)?
        } else if let Some(filter_bitmap) = &filter_bitmap {
            let filter_bitmap: Bitmap = filter_bitmap.clone().into();
            let filtered_block = preread_block.clone().filter_with_bitmap(&filter_bitmap)?;
            self.runtime_filter(&filtered_block, filtered_block.num_rows())?
                .map(|runtime_bitmap| expand_runtime_filter_bitmap(&filter_bitmap, &runtime_bitmap))
        } else {
            self.runtime_filter(&preread_block, part.nums_rows)?
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

        if let Some(ref bitmap) = bitmap_selection {
            preread_block = preread_block.filter_with_bitmap(bitmap)?;
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
        column_ids: &'a HashSet<ColumnId>,
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

fn runtime_filter_column_is_projectable(
    schema: &databend_common_expression::TableSchema,
    name: &str,
) -> bool {
    Projection::from_column_names(schema, &[name]).is_ok()
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

/// Evaluate the prewhere predicate with the selector executor and convert the selected
/// row indexes into a bitmap. A fuse block may hold more rows than the executor's
/// selection buffers, so the block is evaluated in chunks of `max_block_size` rows.
fn filter_with_selector(
    filter_executor: &mut FilterExecutor,
    block: &DataBlock,
    num_rows: usize,
) -> Result<MutableBitmap> {
    debug_assert_eq!(block.num_rows(), num_rows);
    let mut bitmap = MutableBitmap::from_len_zeroed(num_rows);
    let chunk_size = filter_executor.max_block_size().max(1);

    if num_rows <= chunk_size {
        let selected = filter_executor.select(block)?;
        apply_selection(
            &mut bitmap,
            &filter_executor.true_selection()[..selected],
            0,
        );
        return Ok(bitmap);
    }

    for offset in (0..num_rows).step_by(chunk_size) {
        let end = (offset + chunk_size).min(num_rows);
        let block = block.slice(offset..end);
        let selected = filter_executor.select(&block)?;
        apply_selection(
            &mut bitmap,
            &filter_executor.true_selection()[..selected],
            offset,
        );
    }
    Ok(bitmap)
}

/// Selected indexes are not sorted when the predicate contains OR.
fn apply_selection(bitmap: &mut MutableBitmap, selection: &[u32], offset: usize) {
    for index in selection {
        bitmap.set(offset + *index as usize, true);
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use databend_common_expression::BlockEntry;
    use databend_common_expression::Column;
    use databend_common_expression::ColumnRef;
    use databend_common_expression::ConstantFolder;
    use databend_common_expression::FromData;
    use databend_common_expression::RawExpr;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::type_check;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::DictColumn;
    use databend_common_expression::types::EncodedColumn;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::MutableBitmap;
    use databend_common_expression::types::NarrowBuffer;
    use databend_common_expression::types::NullableColumn;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::StringType;

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

    #[test]
    fn test_apply_selection_accepts_unordered_indices_and_offset() {
        let mut bitmap = MutableBitmap::from_len_zeroed(8);
        apply_selection(&mut bitmap, &[4, 1, 3], 2);
        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![
            false, false, false, true, false, true, true, false
        ]);
    }

    fn column(id: usize, data_type: DataType) -> RawExpr {
        RawExpr::ColumnRef {
            span: None,
            id,
            data_type,
            display_name: format!("c{id}"),
        }
    }

    fn string_constant(value: &str) -> RawExpr {
        RawExpr::Constant {
            span: None,
            scalar: Scalar::String(value.to_string()),
            data_type: Some(DataType::String),
        }
    }

    fn int_constant(value: i32) -> RawExpr {
        RawExpr::Constant {
            span: None,
            scalar: Scalar::Number(NumberScalar::Int32(value)),
            data_type: Some(DataType::Number(NumberDataType::Int32)),
        }
    }

    fn call(name: &str, args: Vec<RawExpr>) -> RawExpr {
        RawExpr::FunctionCall {
            span: None,
            name: name.to_string(),
            params: vec![],
            args,
        }
    }

    /// Check and fold the predicate as the planner does, so that the cast of a string
    /// literal compared with a nullable column becomes a constant.
    fn prewhere_expr(raw_expr: &RawExpr, func_ctx: &FunctionContext) -> Expr {
        let expr = type_check::check(raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        ConstantFolder::fold(Cow::Owned(expr), func_ctx, &BUILTIN_FUNCTIONS)
            .0
            .into_owned()
    }

    fn evaluator_bitmap(expr: &Expr, block: &DataBlock, func_ctx: &FunctionContext) -> Vec<bool> {
        let result = Evaluator::new(block, func_ctx, &BUILTIN_FUNCTIONS)
            .run(expr)
            .unwrap()
            .try_downcast::<BooleanType>()
            .unwrap();
        FilterHelpers::filter_to_bitmap(result, block.num_rows())
            .iter()
            .collect()
    }

    /// The selector bitmap must match the evaluator bitmap for blocks smaller and larger
    /// than the selection buffers, including OR predicates whose selected indexes are not
    /// sorted and nullable string columns.
    #[test]
    fn test_chunked_selector_bitmap_matches_evaluator() {
        let string_type = DataType::String;
        let nullable_string_type = DataType::Nullable(Box::new(DataType::String));
        let int_type = DataType::Number(NumberDataType::Int32);
        let values = vec![
            "other", "match", "other", "match", "match", "other", "match",
        ];
        let num_rows = values.len();
        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(values.clone()),
            StringType::from_opt_data(vec![
                Some("match"),
                None,
                Some("match"),
                Some("other"),
                None,
                Some("match"),
                Some("match"),
            ]),
            Int32Type::from_data((0..num_rows as i32).collect()),
        ]);
        let func_ctx = FunctionContext::default();

        for raw_expr in [
            call("eq", vec![
                column(0, string_type.clone()),
                string_constant("match"),
            ]),
            call("or", vec![
                call("eq", vec![
                    column(0, string_type.clone()),
                    string_constant("match"),
                ]),
                call("gt", vec![column(2, int_type.clone()), int_constant(4)]),
            ]),
            call("and", vec![
                call("eq", vec![
                    column(1, nullable_string_type.clone()),
                    string_constant("match"),
                ]),
                call("gt", vec![column(2, int_type.clone()), int_constant(1)]),
            ]),
            call("or", vec![
                call("eq", vec![
                    string_constant("match"),
                    column(1, nullable_string_type.clone()),
                ]),
                call("lt", vec![column(2, int_type.clone()), int_constant(2)]),
            ]),
        ] {
            let expr = prewhere_expr(&raw_expr, &func_ctx);
            let (select_expr, _) = SelectExprBuilder::new(&BUILTIN_FUNCTIONS)
                .build(&expr)
                .into();
            assert!(
                select_expr.contains_string_column_eq_constant(),
                "{}",
                expr.sql_display()
            );
            let expected = evaluator_bitmap(&expr, &block, &func_ctx);

            for max_block_size in [1, 3, num_rows, 16] {
                let mut filter_executor = FilterExecutor::new(
                    expr.clone(),
                    func_ctx.clone(),
                    max_block_size,
                    None,
                    &BUILTIN_FUNCTIONS,
                    false,
                );
                // The executor keeps adaptive state across blocks; run it several times.
                for _ in 0..4 {
                    let actual =
                        filter_with_selector(&mut filter_executor, &block, num_rows).unwrap();
                    assert_eq!(
                        actual.iter().collect::<Vec<_>>(),
                        expected,
                        "{} with max_block_size {max_block_size}",
                        expr.sql_display()
                    );
                }
            }
        }
    }

    /// Fuse scans may keep string columns dictionary encoded. Chunking slices the encoded
    /// column, and the selector must still match the evaluator over the decoded column.
    #[test]
    fn test_chunked_selector_bitmap_over_dictionary_column() {
        let keys = [1u64, 0, 1, 2, 1, 0, 2, 1];
        let num_rows = keys.len();
        let encoded = Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(
            NarrowBuffer::pack(keys.into_iter(), 2),
            StringType::from_data(vec!["other", "match", "unused"]),
        ))));
        let func_ctx = FunctionContext::default();

        for nullable in [false, true] {
            let column_data = if nullable {
                NullableColumn::new_column(
                    encoded.clone(),
                    Bitmap::from_iter([true, true, false, true, true, true, true, false]),
                )
            } else {
                encoded.clone()
            };
            let data_type = column_data.data_type();
            let block = DataBlock::new(vec![column_data.into()], num_rows);
            let decoded = DataBlock::new(
                vec![BlockEntry::try_from(block.get_by_offset(0).decoded_value()).unwrap()],
                num_rows,
            );
            let raw_expr = call("eq", vec![column(0, data_type), string_constant("match")]);
            let expr = prewhere_expr(&raw_expr, &func_ctx);
            let expected = evaluator_bitmap(&expr, &decoded, &func_ctx);

            for max_block_size in [3, num_rows] {
                let mut filter_executor = FilterExecutor::new(
                    expr.clone(),
                    func_ctx.clone(),
                    max_block_size,
                    None,
                    &BUILTIN_FUNCTIONS,
                    false,
                );
                let actual = filter_with_selector(&mut filter_executor, &block, num_rows).unwrap();
                assert_eq!(
                    actual.iter().collect::<Vec<_>>(),
                    expected,
                    "nullable {nullable}, max_block_size {max_block_size}"
                );
            }
        }
    }
}
