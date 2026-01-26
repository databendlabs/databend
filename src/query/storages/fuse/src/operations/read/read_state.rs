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
    pub column_index: FieldIndex,
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
}

impl ReadState {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        scan_id: usize,
        prewhere_info: Option<&PrewhereInfo>,
        block_reader: &BlockReader,
    ) -> Result<Self> {
        let original_schema = block_reader.original_schema.as_ref();

        let runtime_filter_entries = ctx.get_runtime_filters(scan_id);
        let runtime_filter_column_names: Vec<_> = runtime_filter_entries
            .iter()
            .filter_map(|entry| {
                let column_name = entry.bloom.as_ref().map(|bloom| &bloom.column_name)?;
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

        let remain_projection = block_reader.projection.difference(&preread_projection);
        let prewhere_reader = block_reader.change_projection(preread_projection)?;
        let remain_reader = block_reader.change_projection(remain_projection)?;
        let pre_column_ids = prewhere_reader.schema().to_leaf_column_id_set();
        let remain_column_ids = remain_reader.schema().to_leaf_column_id_set();

        let prewhere_schema: DataSchema = (prewhere_reader.schema().as_ref()).into();

        let runtime_filters: Vec<BloomRuntimeFilterRef> = runtime_filter_entries
            .into_iter()
            .filter_map(|entry| {
                let bloom = entry.bloom?;
                let column_index = prewhere_schema.index_of(bloom.column_name.as_str()).ok()?;
                Some(BloomRuntimeFilterRef {
                    column_index,
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
        let bloom_start = Instant::now();

        let mut bitmaps = vec![];
        for runtime_filter in &self.runtime_filters {
            let probe_column = block.get_by_offset(runtime_filter.column_index).to_column();
            let bitmap = ExprBloomFilter::new(&runtime_filter.filter).apply(probe_column)?;
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

        if let Some(ref bitmap) = bitmap_selection {
            preread_block = preread_block.filter_with_bitmap(bitmap)?;
        }

        let remain_columns_chunks =
            Self::filter_column_chunks(&columns_chunks, &self.remain_column_ids)?;
        let row_selection = bitmap_selection.as_ref().map(RowSelection::from);

        let remain_block = self.remain_reader.deserialize_part(
            part,
            remain_columns_chunks,
            row_selection.as_ref(),
        )?;

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
