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
use databend_common_catalog::runtime_filter_info::RowRuntimeFilter;
use databend_common_catalog::runtime_filter_info::RowRuntimeFilters;
use databend_common_catalog::runtime_filter_info::RuntimeFilterSource;
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

struct ResolvedRowFilter {
    column_index: FieldIndex,
    filter: Arc<dyn RowRuntimeFilter>,
}

pub struct ReadState {
    pub pre_reader: Arc<BlockReader>,
    pub remain_reader: Arc<BlockReader>,
    pub filters: Option<Expr>,
    resolved_row_filters: Vec<ResolvedRowFilter>,
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
        _scan_id: usize,
        prewhere_info: Option<&PrewhereInfo>,
        block_reader: Arc<BlockReader>,
        rf_source: Option<&Arc<RuntimeFilterSource>>,
    ) -> Result<Self> {
        let prewhere_selectivity_threshold =
            ctx.get_settings().get_prewhere_selectivity_threshold()?;
        let use_single_prewhere_reader =
            prewhere_info.is_some() && prewhere_selectivity_threshold == 0;
        let original_schema = block_reader.original_schema.as_ref();

        // Extract bloom filter column names from RuntimeFilterSource for preread projection
        let row_filters: RowRuntimeFilters =
            rf_source.map(|s| s.get_row_filters()).unwrap_or_default();
        let runtime_filter_column_names: Vec<_> = row_filters
            .iter()
            .filter_map(|filter| {
                let column_name = filter.column_name();
                if original_schema.index_of(column_name).is_ok() {
                    Some(column_name)
                } else {
                    None
                }
            })
            .collect();

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

        let resolved_row_filters: Vec<ResolvedRowFilter> = row_filters
            .into_iter()
            .filter_map(|filter| {
                let column_index = prewhere_schema.index_of(filter.column_name()).ok()?;
                Some(ResolvedRowFilter {
                    column_index,
                    filter,
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
            resolved_row_filters,
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
        _num_rows: usize,
    ) -> Result<Option<MutableBitmap>> {
        if self.resolved_row_filters.is_empty() {
            return Ok(None);
        }

        let bloom_start = Instant::now();

        let mut bitmaps: Vec<Bitmap> = vec![];
        for rf in &self.resolved_row_filters {
            let column = block.get_by_offset(rf.column_index).to_column();
            bitmaps.push(rf.filter.apply(column)?);
        }

        let result_bitmap = bitmaps.into_iter().reduce(|acc, b| &acc & &b);

        let bloom_duration = bloom_start.elapsed();
        Profile::record_usize_profile(
            ProfileStatisticsName::RuntimeFilterBloomTime,
            bloom_duration.as_nanos() as usize,
        );

        Ok(result_bitmap.map(|bm| bm.make_mut()))
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
        let runtime_filter_bitmap = self.runtime_filter(&preread_block, part.nums_rows)?;

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

fn should_push_down_row_selection(row_selection: &RowSelection, threshold: u64) -> bool {
    let total_rows = row_selection.bitmap.len();
    if threshold == 0 || total_rows == 0 {
        return false;
    }

    (row_selection.selected_rows as u128) * 100 < (total_rows as u128) * (threshold as u128)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::MutableBitmap;

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
}
