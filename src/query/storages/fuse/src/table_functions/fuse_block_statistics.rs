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
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::variant::cast_scalar_to_variant;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SpatialStatistics;
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
            TableField::new(
                "statistics",
                TableDataType::Nullable(Box::new(TableDataType::Variant)),
            ),
            TableField::new(
                "spatial_statistics",
                TableDataType::Nullable(Box::new(TableDataType::Variant)),
            ),
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
        let func_ctx = ctx.get_function_context()?;
        let estimated_rows = std::cmp::min(
            snapshot.summary.block_count as usize * schema.num_fields().max(1),
            limit,
        );

        let mut block_locations = Vec::with_capacity(estimated_rows);
        let mut column_ids = Vec::with_capacity(estimated_rows);
        let mut column_names = Vec::with_capacity(estimated_rows);
        let mut statistics = Vec::with_capacity(estimated_rows);
        let mut spatial_statistics = Vec::with_capacity(estimated_rows);

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
                        let stat = build_column_statistics_variant(
                            column_stat,
                            field.data_type().remove_nullable(),
                            &func_ctx,
                        );
                        statistics.push(Some(stat));
                        spatial_statistics.push(None);

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
                            statistics.push(None);
                            let stat = build_spatial_statistics_variant(spatial_stat, &func_ctx);
                            spatial_statistics.push(Some(stat));

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
            VariantType::from_opt_data(statistics),
            VariantType::from_opt_data(spatial_statistics),
        ]))
    }
}

fn build_column_statistics_variant(
    column_stat: &databend_storages_common_table_meta::meta::ColumnStatistics,
    field_type: TableDataType,
    func_ctx: &FunctionContext,
) -> Vec<u8> {
    let scalar = Scalar::Tuple(vec![
        column_stat.min.clone(),
        column_stat.max.clone(),
        Scalar::Number(NumberScalar::UInt64(column_stat.null_count)),
        Scalar::Number(NumberScalar::UInt64(column_stat.in_memory_size)),
        column_stat
            .distinct_of_values
            .map(|value| Scalar::Number(NumberScalar::UInt64(value)))
            .unwrap_or(Scalar::Null),
    ]);
    let data_type = TableDataType::Tuple {
        fields_name: vec![
            "min".to_string(),
            "max".to_string(),
            "null_count".to_string(),
            "in_memory_size".to_string(),
            "distinct_count".to_string(),
        ],
        fields_type: vec![
            field_type.clone(),
            field_type,
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
        ],
    };

    build_variant(scalar, &data_type, func_ctx)
}

fn build_spatial_statistics_variant(
    spatial_stat: &SpatialStatistics,
    func_ctx: &FunctionContext,
) -> Vec<u8> {
    let scalar = Scalar::Tuple(vec![
        Scalar::Number(NumberScalar::Float64(F64::from(spatial_stat.min_x.0))),
        Scalar::Number(NumberScalar::Float64(F64::from(spatial_stat.min_y.0))),
        Scalar::Number(NumberScalar::Float64(F64::from(spatial_stat.max_x.0))),
        Scalar::Number(NumberScalar::Float64(F64::from(spatial_stat.max_y.0))),
        Scalar::Number(NumberScalar::Int32(spatial_stat.srid)),
        Scalar::Boolean(spatial_stat.has_null),
        Scalar::Boolean(spatial_stat.has_empty_rect),
        Scalar::Boolean(spatial_stat.is_valid),
    ]);
    let data_type = TableDataType::Tuple {
        fields_name: vec![
            "min_x".to_string(),
            "min_y".to_string(),
            "max_x".to_string(),
            "max_y".to_string(),
            "srid".to_string(),
            "has_null".to_string(),
            "has_empty_rect".to_string(),
            "is_valid".to_string(),
        ],
        fields_type: vec![
            TableDataType::Number(NumberDataType::Float64),
            TableDataType::Number(NumberDataType::Float64),
            TableDataType::Number(NumberDataType::Float64),
            TableDataType::Number(NumberDataType::Float64),
            TableDataType::Number(NumberDataType::Int32),
            TableDataType::Boolean,
            TableDataType::Boolean,
            TableDataType::Boolean,
        ],
    };

    build_variant(scalar, &data_type, func_ctx)
}

fn build_variant(scalar: Scalar, data_type: &TableDataType, func_ctx: &FunctionContext) -> Vec<u8> {
    let mut buf = Vec::new();
    cast_scalar_to_variant(scalar.as_ref(), &func_ctx.tz, &mut buf, Some(data_type));
    buf
}
