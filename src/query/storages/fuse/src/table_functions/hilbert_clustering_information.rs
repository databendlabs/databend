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

use std::sync::Arc;

use chrono::Utc;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::table::ClusterType;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;

use crate::io::SegmentsIO;
use crate::table_functions::parse_db_tb_args;
use crate::table_functions::string_literal;
use crate::table_functions::SimpleArgFunc;
use crate::table_functions::SimpleArgFuncTemplate;
use crate::FuseTable;

pub struct HilbertClusteringInfoArgs {
    database_name: String,
    table_name: String,
}

impl From<&HilbertClusteringInfoArgs> for TableArgs {
    fn from(args: &HilbertClusteringInfoArgs) -> Self {
        let tbl_args = vec![
            string_literal(args.database_name.as_str()),
            string_literal(args.table_name.as_str()),
        ];
        TableArgs::new_positioned(tbl_args)
    }
}

impl TryFrom<(&str, TableArgs)> for HilbertClusteringInfoArgs {
    type Error = ErrorCode;
    fn try_from(
        (func_name, table_args): (&str, TableArgs),
    ) -> std::result::Result<Self, Self::Error> {
        let (database_name, table_name) = parse_db_tb_args(&table_args, func_name)?;

        Ok(Self {
            database_name,
            table_name,
        })
    }
}

pub type HilbertClusteringInfoFunc = SimpleArgFuncTemplate<HilbertClusteringInfo>;
pub struct HilbertClusteringInfo;

#[async_trait::async_trait]
impl SimpleArgFunc for HilbertClusteringInfo {
    type Args = HilbertClusteringInfoArgs;

    fn schema() -> TableSchemaRef {
        HilbertClusteringInfoImpl::schema()
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        args: &Self::Args,
        _plan: &DataSourcePlan,
    ) -> Result<DataBlock> {
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_table(
                &tenant_id,
                args.database_name.as_str(),
                args.table_name.as_str(),
            )
            .await?;

        let tbl = FuseTable::try_from_table(tbl.as_ref())?;

        HilbertClusteringInfoImpl::new(ctx.clone(), tbl)
            .get_clustering_info()
            .await
    }
}

struct HilbertClusteringInfoImpl<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
}

impl<'a> HilbertClusteringInfoImpl<'a> {
    fn new(ctx: Arc<dyn TableContext>, table: &'a FuseTable) -> Self {
        Self { ctx, table }
    }

    #[async_backtrace::framed]
    async fn get_clustering_info(&self) -> Result<DataBlock> {
        let Some(cluster_key_str) = self.table.cluster_key_str() else {
            return Err(ErrorCode::UnclusteredTable(format!(
                "Unclustered table {}",
                self.table.table_info.desc
            )));
        };
        let cluster_type = self
            .table
            .get_option(OPT_KEY_CLUSTER_TYPE, ClusterType::Linear);
        if matches!(cluster_type, ClusterType::Linear) {
            return Err(ErrorCode::UnsupportedClusterType(
                "Unsupported `linear` type, please use `clustering_information` instead",
            ));
        }

        let snapshot = self.table.read_table_snapshot().await?;
        let now = Utc::now();
        let timestamp = snapshot
            .as_ref()
            .map_or(now, |s| s.timestamp.unwrap_or(now))
            .timestamp_micros();
        let mut total_segment_count = 0;
        let mut stable_segment_count = 0;
        let mut partial_segment_count = 0;
        let mut unclustered_segment_count = 0;
        if let Some(snapshot) = snapshot {
            let total_count = snapshot.segments.len();
            total_segment_count = total_count as u64;
            let chunk_size = std::cmp::min(
                self.ctx.get_settings().get_max_threads()? as usize * 4,
                total_count,
            )
            .max(1);
            let segments_io = SegmentsIO::create(
                self.ctx.clone(),
                self.table.operator.clone(),
                self.table.schema(),
            );
            for chunk in snapshot.segments.chunks(chunk_size) {
                let segments = segments_io
                    .read_segments_old::<Arc<CompactSegmentInfo>>(chunk, true)
                    .await?;
                for segment in segments {
                    let segment = segment?;
                    let Some(level) = segment.summary.cluster_stats.as_ref().map(|v| v.level)
                    else {
                        unclustered_segment_count += 1;
                        continue;
                    };
                    if level == -1 {
                        stable_segment_count += 1;
                    } else {
                        partial_segment_count += 1;
                    }
                }
            }
        }
        Ok(DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(cluster_key_str.to_string())),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String("hilbert".to_string())),
                ),
                BlockEntry::new(
                    DataType::Timestamp,
                    Value::Scalar(Scalar::Timestamp(timestamp)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(total_segment_count))),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(stable_segment_count))),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(partial_segment_count))),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(
                        unclustered_segment_count,
                    ))),
                ),
            ],
            1,
        ))
    }

    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("cluster_key", TableDataType::String),
            TableField::new("type", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp),
            TableField::new(
                "total_segment_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "stable_segment_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "partial_segment_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "unclustered_segment_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
        ])
    }
}
