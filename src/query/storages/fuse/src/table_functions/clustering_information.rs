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

use chrono::Utc;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::parse_table_name;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_sql::analyze_cluster_keys;
use databend_common_sql::parse_cluster_keys;
use databend_storages_common_table_meta::meta::SegmentInfo;
use jsonb::Value as JsonbValue;
use serde::Deserialize;
use serde::Serialize;

use crate::FuseTable;
use crate::Table;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::statistics::BlockOverlapDepth;
use crate::statistics::calculate_block_overlap_depths;
use crate::statistics::get_min_max_stats;
use crate::statistics::prepare_cluster_key_exprs;
use crate::table_functions::SimpleArgFunc;
use crate::table_functions::SimpleArgFuncTemplate;
use crate::table_functions::parse_db_tb_opt_args;
use crate::table_functions::string_literal;

pub struct ClusteringInformationArgs {
    database_name: String,
    table_name: String,
    cluster_key: Option<String>,
}

impl From<&ClusteringInformationArgs> for TableArgs {
    fn from(args: &ClusteringInformationArgs) -> Self {
        let mut tbl_args = Vec::new();
        tbl_args.push(string_literal(args.database_name.as_str()));
        tbl_args.push(string_literal(args.table_name.as_str()));
        if let Some(arg_cluster_key) = &args.cluster_key {
            tbl_args.push(string_literal(arg_cluster_key));
        }
        TableArgs::new_positioned(tbl_args)
    }
}

impl TryFrom<(&str, TableArgs)> for ClusteringInformationArgs {
    type Error = ErrorCode;
    fn try_from(
        (func_name, table_args): (&str, TableArgs),
    ) -> std::result::Result<Self, Self::Error> {
        let (database_name, table_name, cluster_key) =
            parse_db_tb_opt_args(&table_args, func_name)?;

        Ok(Self {
            database_name,
            table_name,
            cluster_key,
        })
    }
}

pub type ClusteringInformationFunc = SimpleArgFuncTemplate<ClusteringInformation>;
pub struct ClusteringInformation;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusteringInformationResponse {
    pub cluster_key: String,
    #[serde(rename = "type")]
    pub cluster_type: String,
    pub timestamp: i64,
    pub info: serde_json::Value,
}

#[async_backtrace::framed]
pub async fn get_clustering_information(
    ctx: Arc<dyn TableContext>,
    table: &FuseTable,
    cluster_key: &Option<String>,
) -> Result<ClusteringInformationResponse> {
    ClusteringInformationImpl::new(ctx, table)
        .get_clustering_info(cluster_key)
        .await
}

#[async_trait::async_trait]
impl SimpleArgFunc for ClusteringInformation {
    type Args = ClusteringInformationArgs;

    fn schema() -> TableSchemaRef {
        ClusteringInformationImpl::schema()
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        args: &Self::Args,
        _plan: &DataSourcePlan,
    ) -> Result<DataBlock> {
        let (table_name, branch_name) = parse_table_name(args.table_name.as_str())?;
        let current_catalog = ctx.get_current_catalog();
        let tbl = ctx
            .get_table_with_branch(
                &current_catalog,
                args.database_name.as_str(),
                &table_name,
                branch_name.as_deref(),
            )
            .await?;
        let tbl = FuseTable::try_from_table(tbl.as_ref())?;
        let info = get_clustering_information(ctx.clone(), tbl, &args.cluster_key).await?;
        build_block(info)
    }
}

struct ClusteringInformationImpl<'a> {
    ctx: Arc<dyn TableContext>,
    table: &'a FuseTable,
}

impl<'a> ClusteringInformationImpl<'a> {
    fn new(ctx: Arc<dyn TableContext>, table: &'a FuseTable) -> Self {
        Self { ctx, table }
    }

    #[async_backtrace::framed]
    async fn get_clustering_info(
        &self,
        cluster_key: &Option<String>,
    ) -> Result<ClusteringInformationResponse> {
        match (self.table.cluster_key_meta(), cluster_key) {
            (None, None) => Err(ErrorCode::UnclusteredTable(format!(
                "Unclustered table {}",
                self.table.table_info.desc,
            ))),
            _ => {
                // Enforces linear clustering evaluation of keys, allowing users to examine clustering
                // information without defining cluster keys.
                //
                // Currently, only linear clustering is supported.
                self.get_linear_clustering_info(cluster_key).await
            }
        }
    }

    #[async_backtrace::framed]
    async fn get_linear_clustering_info(
        &self,
        cluster_key: &Option<String>,
    ) -> Result<ClusteringInformationResponse> {
        let mut default_cluster_key_id = None;
        let (cluster_key, exprs) = match (self.table.cluster_key_str(), cluster_key) {
            (a, Some(b)) => {
                let (cluster_key, exprs) =
                    analyze_cluster_keys(self.ctx.clone(), Arc::new(self.table.clone()), b)?;
                let exprs = exprs
                    .into_iter()
                    .map(|expr| expr.project_column_ref(|index| Ok(index.as_usize())))
                    .collect::<Result<Vec<_>>>()?;
                if a.is_some() && a.unwrap() == cluster_key {
                    default_cluster_key_id = self.table.cluster_key_id();
                }
                (cluster_key, exprs)
            }
            (Some(a), None) => {
                let cluster_keys = self.table.resolve_cluster_keys().unwrap();
                let exprs = parse_cluster_keys(
                    self.ctx.clone(),
                    Arc::new(self.table.clone()),
                    cluster_keys,
                )?;
                default_cluster_key_id = self.table.cluster_key_id();
                (a.to_string(), exprs)
            }
            _ => {
                unreachable!("Unclustered table {}", self.table.table_info.desc);
            }
        };

        let cluster_type = "linear".to_string();

        let snapshot = self.table.read_table_snapshot().await?;
        let now = Utc::now();
        let timestamp = snapshot
            .as_ref()
            .map_or(now, |s| s.timestamp.unwrap_or(now))
            .timestamp_micros();
        if snapshot.is_none() {
            return Ok(ClusteringInformationResponse {
                cluster_key,
                cluster_type,
                timestamp,
                info: serde_json::to_value(LinerClusterStatistics::default())?,
            });
        }
        let snapshot = snapshot.unwrap();

        let schema = self.table.schema();
        let scalar_exprs = exprs
            .into_iter()
            .filter(|expr| !matches!(expr.data_type().remove_nullable(), DataType::Vector(_)))
            .collect::<Vec<_>>();
        let scalar_cluster_key_types = scalar_exprs
            .iter()
            .map(|v| {
                let data_type = v.data_type();
                if matches!(*data_type, DataType::String) {
                    data_type.wrap_nullable()
                } else {
                    data_type.clone()
                }
            })
            .collect::<Vec<_>>();
        let prepared_cluster_key_exprs = prepare_cluster_key_exprs(&scalar_exprs, schema.as_ref());

        let mut ranges = Vec::with_capacity(snapshot.summary.block_count as usize);
        let mut constant_block_count = 0;

        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.table.operator.clone(),
            self.table.schema(),
        );
        let total_block_count = snapshot.summary.block_count;
        let chunk_size = self.ctx.get_settings().get_max_threads()? as usize * 4;
        for chunk in snapshot.segments.chunks(chunk_size) {
            let segments: Vec<Result<SegmentInfo>> = segments_io.read_segments(chunk, true).await?;

            for segment in segments {
                let segment = segment?;
                for block in segment.blocks {
                    let (min, max) = get_min_max_stats(
                        &prepared_cluster_key_exprs,
                        &block.col_stats,
                        block.cluster_stats.as_ref(),
                        default_cluster_key_id,
                    );
                    assert_eq!(min.len(), max.len());
                    if min == max {
                        constant_block_count += 1;
                    }
                    ranges.push((min, max));
                }
            }
        }
        drop(snapshot);

        let stats = if scalar_cluster_key_types.is_empty() {
            vec![BlockOverlapDepth::default(); ranges.len()]
        } else {
            calculate_block_overlap_depths(&ranges, &scalar_cluster_key_types)?
        };
        if stats.is_empty() {
            return Ok(ClusteringInformationResponse {
                cluster_key,
                cluster_type,
                timestamp,
                info: serde_json::to_value(LinerClusterStatistics {
                    total_block_count,
                    ..Default::default()
                })?,
            });
        }

        let mut sum_overlap = 0;
        let mut sum_depth = 0;
        let length = stats.len();
        let mut depth_counts = BTreeMap::new();
        let bucket_counts = stats.into_iter().fold(BTreeMap::new(), |mut acc, stat| {
            sum_overlap += stat.overlap;
            sum_depth += stat.depth;
            depth_counts
                .entry(stat.depth)
                .and_modify(|v| *v += 1)
                .or_insert(1);

            let bucket = get_buckets(stat.depth);
            acc.entry(bucket).and_modify(|v| *v += 1).or_insert(1);
            acc
        });
        // round the float to 4 decimal places.
        let average_depth = (10000.0 * sum_depth as f64 / length as f64).round() / 10000.0;
        let average_overlaps = (10000.0 * sum_overlap as f64 / length as f64).round() / 10000.0;

        let block_depth_histogram =
            bucket_counts
                .into_iter()
                .fold(BTreeMap::new(), |mut acc, (bucket, count)| {
                    acc.insert(format!("{:05}", bucket), count);
                    acc
                });
        let p95_depth = percentile_depth(&depth_counts, length, 95);
        let p99_depth = percentile_depth(&depth_counts, length, 99);

        let info = LinerClusterStatistics {
            total_block_count,
            constant_block_count,
            average_overlaps,
            average_depth,
            p95_depth,
            p99_depth,
            block_depth_histogram,
        };
        Ok(ClusteringInformationResponse {
            cluster_key,
            cluster_type,
            timestamp,
            info: serde_json::to_value(info)?,
        })
    }

    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("cluster_key", TableDataType::String),
            TableField::new("type", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp),
            TableField::new("info", TableDataType::Variant),
        ])
    }
}

fn build_block(info: ClusteringInformationResponse) -> Result<DataBlock> {
    Ok(DataBlock::new(
        vec![
            BlockEntry::new_const_column_arg::<StringType>(info.cluster_key, 1),
            BlockEntry::new_const_column_arg::<StringType>(info.cluster_type, 1),
            BlockEntry::new_const_column_arg::<TimestampType>(info.timestamp, 1),
            BlockEntry::new_const_column_arg::<VariantType>(
                JsonbValue::from(info.info).to_vec(),
                1,
            ),
        ],
        1,
    ))
}

#[derive(Serialize, Default)]
struct LinerClusterStatistics {
    total_block_count: u64,
    constant_block_count: u64,
    average_overlaps: f64,
    average_depth: f64,
    p95_depth: usize,
    p99_depth: usize,
    block_depth_histogram: BTreeMap<String, u64>,
}

/// The histogram contains buckets with widths:
/// 1 to 16 with increments of 1.
/// For buckets larger than 16, increments of twice the width of the previous bucket (e.g. 32, 64, 128, …).
/// e.g. If val is 2, the bucket is 2. If val is 18, the bucket is 32.
fn get_buckets(val: usize) -> u32 {
    let mut val = val as u32;
    if val <= 16 || val & (val - 1) == 0 {
        return val;
    }

    val |= val >> 1;
    val |= val >> 2;
    val |= val >> 4;
    val |= val >> 8;
    val |= val >> 16;
    val + 1
}

fn percentile_depth(
    depth_counts: &BTreeMap<usize, u64>,
    total_count: usize,
    percentile: u64,
) -> usize {
    if total_count == 0 {
        return 0;
    }

    let rank = ((total_count as u64) * percentile).div_ceil(100);
    let mut seen = 0;
    for (depth, count) in depth_counts {
        seen += count;
        if seen >= rank {
            return *depth;
        }
    }

    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percentile_depth_uses_nearest_rank() {
        let depth_counts = BTreeMap::from([(1, 3), (2, 2), (3, 1), (20, 1)]);

        assert_eq!(percentile_depth(&depth_counts, 7, 50), 2);
        assert_eq!(percentile_depth(&depth_counts, 7, 95), 20);
        assert_eq!(percentile_depth(&depth_counts, 7, 99), 20);
        assert_eq!(percentile_depth(&BTreeMap::new(), 0, 99), 0);
    }
}
