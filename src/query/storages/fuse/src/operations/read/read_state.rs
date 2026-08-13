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
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FunctionContext;
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

pub struct ReadState {
    pub pre_reader: Arc<BlockReader>,
    pub remain_reader: Arc<BlockReader>,
    pub filters: Option<Expr>,
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

        Ok(Self {
            pre_reader: prewhere_reader,
            remain_reader,
            filters: prewhere_filter,
            runtime_filters,
            pre_column_ids,
            remain_column_ids,
            func_ctx: ctx.get_function_context()?,
            output_schema: block_reader.data_schema(),
            prewhere_selectivity_threshold,
            use_single_prewhere_reader,
        })
    }

    pub fn filter(&self, block: &DataBlock, num_rows: usize) -> Result<Option<MutableBitmap>> {
        if let Some(ref f) = self.filters {
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
            let probe_column = evaluator
                .run(&runtime_filter.probe_expr)?
                .convert_to_full_column(runtime_filter.probe_expr.data_type(), num_rows);
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
        &self,
        columns_chunks: HashMap<ColumnId, DataItem>,
        part: &FuseBlockPartInfo,
    ) -> Result<(DataBlock, Option<RowSelection>, Option<Bitmap>)> {
        let pre_columns_chunks = Self::filter_column_chunks(&columns_chunks, &self.pre_column_ids)?;
        let mut preread_block = self
            .pre_reader
            .deserialize_part(part, pre_columns_chunks, None)?;

        let filter_bitmap = self.filter(&preread_block, part.nums_rows)?;
        // Expensive probe expressions are evaluated only for rows surviving
        // the static prewhere predicate. Expand their bitmap back to the
        // original row positions before combining it with prewhere.
        let runtime_filter_bitmap = if self.runtime_filters.is_empty() {
            None
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

        let row_selection = bitmap_selection.as_ref().map(RowSelection::from);

        if let Some(ref bitmap) = bitmap_selection {
            preread_block = preread_block.filter_with_bitmap(bitmap)?;
        }

        if self.use_single_prewhere_reader {
            return Ok((preread_block, row_selection, bitmap_selection));
        }

        let remain_columns_chunks =
            Self::filter_column_chunks(&columns_chunks, &self.remain_column_ids)?;
        let push_down_row_selection = row_selection.as_ref().is_some_and(|row_selection| {
            should_push_down_row_selection(row_selection, self.prewhere_selectivity_threshold)
        });

        let mut remain_block = self.remain_reader.deserialize_part(
            part,
            remain_columns_chunks,
            push_down_row_selection
                .then_some(row_selection.as_ref())
                .flatten(),
        )?;
        if !push_down_row_selection {
            if let Some(bitmap) = bitmap_selection.as_ref() {
                remain_block = remain_block.filter_with_bitmap(bitmap)?;
            }
        }

        let mut merged_fields = self.pre_reader.data_fields();
        merged_fields.extend(self.remain_reader.data_fields());
        let merged_schema = DataSchema::new(merged_fields);

        preread_block.merge_block(remain_block);

        let data_block = preread_block.resort(&merged_schema, &self.output_schema)?;

        Ok((data_block, row_selection, bitmap_selection))
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

fn should_push_down_row_selection(row_selection: &RowSelection, threshold: u64) -> bool {
    let total_rows = row_selection.bitmap.len();
    if threshold == 0 || total_rows == 0 {
        return false;
    }

    (row_selection.selected_rows as u128) * 100 < (total_rows as u128) * (threshold as u128)
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
        let sparse_selection = RowSelection::from(&sparse_bitmap);

        assert!(should_push_down_row_selection(&sparse_selection, 50));
        assert!(!should_push_down_row_selection(&sparse_selection, 20));
        assert!(!should_push_down_row_selection(&sparse_selection, 0));

        let dense_bitmap: Bitmap = MutableBitmap::from_len_set(5).into();
        let dense_selection = RowSelection::from(&dense_bitmap);
        assert!(!should_push_down_row_selection(&dense_selection, 100));
    }

    #[test]
    fn test_threshold_zero_disables_row_selection_pushdown() {
        let mut sparse_bitmap = MutableBitmap::from_len_zeroed(5);
        sparse_bitmap.set(2, true);
        let sparse_bitmap: Bitmap = sparse_bitmap.into();
        let sparse_selection = RowSelection::from(&sparse_bitmap);

        assert!(!should_push_down_row_selection(&sparse_selection, 0));
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
}
