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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::TableMetaFuncTemplate;
use crate::table_functions::function_template::TableMetaFunc;

pub struct FuseBlockStatistics;
pub type FuseBlockStatisticsFunc = TableMetaFuncTemplate<FuseBlockStatistics>;

#[async_trait::async_trait]
impl TableMetaFunc for FuseBlockStatistics {
    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("block_location", TableDataType::String),
            TableField::new("column_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("column_name", TableDataType::String),
            TableField::new("statistics", TableDataType::String),
        ])
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let limit = limit.unwrap_or(usize::MAX);
        let schema = tbl.schema();
        let estimated_rows = std::cmp::min(
            snapshot.summary.block_count as usize * schema.num_fields().max(1),
            limit,
        );

        let mut block_locations = Vec::with_capacity(estimated_rows);
        let mut column_ids = Vec::with_capacity(estimated_rows);
        let mut column_names = Vec::with_capacity(estimated_rows);
        let mut statistics = Vec::with_capacity(estimated_rows);

        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), schema.clone());

        let mut num_rows = 0;
        let chunk_size = std::cmp::min(
            ctx.get_settings().get_max_threads()? as usize * 4,
            snapshot.summary.block_count as usize,
        )
        .max(1);
        'outer: for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for segment in segments {
                let segment = segment?;

                for block in segment.blocks.iter() {
                    let block = block.as_ref();
                    let col_stats = block.col_stats.iter().collect::<BTreeMap<_, _>>();
                    let spatial_stats = block
                        .spatial_stats
                        .as_ref()
                        .map(|stats| stats.iter().collect::<BTreeMap<_, _>>());

                    for (column_id, column_stat) in col_stats {
                        let Ok(field) = schema.field_of_column_id(*column_id) else {
                            continue;
                        };
                        block_locations.push(block.location.0.clone());
                        column_ids.push(*column_id as u64);
                        column_names.push(field.name().to_string());

                        let distinct_count = column_stat
                            .distinct_of_values
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "NULL".to_string());

                        let statistic = format!(
                            "column[min={}, max={}, null_count={}, in_memory_size={}, distinct_count={}]",
                            column_stat.min,
                            column_stat.max,
                            column_stat.null_count,
                            column_stat.in_memory_size,
                            distinct_count
                        );
                        statistics.push(statistic);

                        num_rows += 1;
                        if num_rows >= limit {
                            break 'outer;
                        }
                    }

                    if let Some(spatial_stats) = &spatial_stats {
                        for (column_id, spatial_stat) in spatial_stats {
                            let Ok(field) = schema.field_of_column_id(**column_id) else {
                                continue;
                            };
                            block_locations.push(block.location.0.clone());
                            column_ids.push(**column_id as u64);
                            column_names.push(field.name().to_string());

                            let statistic = format!(
                                "spatial[min_x={}, min_y={}, max_x={}, max_y={}, srid={}, has_null={}, has_empty_rect={}, is_valid={}]",
                                spatial_stat.min_x.0,
                                spatial_stat.min_y.0,
                                spatial_stat.max_x.0,
                                spatial_stat.max_y.0,
                                spatial_stat.srid,
                                spatial_stat.has_null,
                                spatial_stat.has_empty_rect,
                                spatial_stat.is_valid
                            );
                            statistics.push(statistic);

                            num_rows += 1;
                            if num_rows >= limit {
                                break 'outer;
                            }
                        }
                    }
                }
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(block_locations),
            UInt64Type::from_data(column_ids),
            StringType::from_data(column_names),
            StringType::from_data(statistics),
        ]))
    }
}
