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

use std::cmp;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::parse_table_name;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::compare_scalars;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_sql::analyze_cluster_keys;
use databend_common_sql::parse_cluster_keys;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::table::ClusterType;
use jsonb::Value as JsonbValue;
use serde::Serialize;

use crate::FuseTable;
use crate::Table;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::statistics::get_min_max_stats;
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
        ClusteringInformationImpl::new(ctx.clone(), tbl)
            .get_clustering_info(&args.cluster_key)
            .await
    }
}

struct ClusteringStatisticsWrapper<T> {
    cluster_key: String,
    cluster_type: String,
    timestamp: i64,
    info: T,
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
    async fn get_clustering_info(&self, cluster_key: &Option<String>) -> Result<DataBlock> {
        match (self.table.cluster_type(), cluster_key) {
            (Some(ClusterType::Hilbert), None) => self.get_hilbert_clustering_info().await,
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
    async fn get_linear_clustering_info(&self, cluster_key: &Option<String>) -> Result<DataBlock> {
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
            return build_block(ClusteringStatisticsWrapper {
                cluster_key,
                cluster_type,
                timestamp,
                info: LinerClusterStatistics::default(),
            });
        }
        let snapshot = snapshot.unwrap();

        let schema = self.table.schema();

        // Gather all cluster statistics points to a hash Map.
        // Key: The cluster statistics points.
        // Value: 0: The block indexes with key as min value;
        //        1: The block indexes with key as max value;
        let mut points_map: HashMap<Vec<Scalar>, (Vec<u64>, Vec<u64>)> = HashMap::new();
        let mut constant_block_count = 0;
        let mut index = 0;

        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.table.operator.clone(),
            self.table.schema(),
        );
        let total_block_count = snapshot.summary.block_count;
        let chunk_size = self.ctx.get_settings().get_max_threads()? as usize * 4;
        for chunk in snapshot.segments.chunks(chunk_size) {
            let segments: Vec<Result<SegmentInfo>> = segments_io.read_segments(chunk, true).await?;

            for segment in segments.into_iter().flatten() {
                for block in segment.blocks {
                    let (min, max) = get_min_max_stats(
                        &exprs,
                        &block.col_stats,
                        block.cluster_stats.as_ref(),
                        default_cluster_key_id,
                        schema.as_ref(),
                    );
                    assert_eq!(min.len(), max.len());
                    if min == max {
                        constant_block_count += 1;
                    }

                    points_map
                        .entry(min)
                        .and_modify(|v| v.0.push(index))
                        .or_insert((vec![index], vec![]));
                    points_map
                        .entry(max)
                        .and_modify(|v| v.1.push(index))
                        .or_insert((vec![], vec![index]));
                    index += 1;
                }
            }
        }
        drop(snapshot);

        // calculate overlaps and depth.
        let mut stats = Vec::new();
        // key: the block index.
        // value: (overlaps, depth).
        let mut unfinished_parts: HashMap<u64, (usize, usize)> = HashMap::new();
        let (keys, values): (Vec<_>, Vec<_>) = points_map.into_iter().unzip();
        let cluster_key_types = exprs
            .into_iter()
            .map(|v| {
                let data_type = v.data_type();
                if matches!(*data_type, DataType::String) {
                    data_type.wrap_nullable()
                } else {
                    data_type.clone()
                }
            })
            .collect::<Vec<_>>();
        let indices = compare_scalars(keys, &cluster_key_types)?;
        for idx in indices.into_iter() {
            let start = &values[idx as usize].0;
            let end = &values[idx as usize].1;
            let point_depth = unfinished_parts.len() + start.len();

            unfinished_parts.values_mut().for_each(|(overlaps, depth)| {
                *overlaps += start.len();
                *depth = cmp::max(*depth, point_depth);
            });

            start.iter().for_each(|idx| {
                unfinished_parts.insert(*idx, (point_depth - 1, point_depth));
            });

            end.iter().for_each(|idx| {
                if let Some(v) = unfinished_parts.remove(idx) {
                    stats.push(v);
                }
            });
        }

        let mut sum_overlap = 0;
        let mut sum_depth = 0;
        let length = stats.len();
        let mp = stats
            .into_iter()
            .fold(BTreeMap::new(), |mut acc, (overlap, depth)| {
                sum_overlap += overlap;
                sum_depth += depth;

                let bucket = get_buckets(depth);
                acc.entry(bucket).and_modify(|v| *v += 1).or_insert(1);
                acc
            });
        // round the float to 4 decimal places.
        let average_depth = (10000.0 * sum_depth as f64 / length as f64).round() / 10000.0;
        let average_overlaps = (10000.0 * sum_overlap as f64 / length as f64).round() / 10000.0;

        let block_depth_histogram =
            mp.into_iter()
                .fold(BTreeMap::new(), |mut acc, (bucket, count)| {
                    acc.insert(format!("{:05}", bucket), count);
                    acc
                });

        let info = LinerClusterStatistics {
            total_block_count,
            constant_block_count,
            average_overlaps,
            average_depth,
            block_depth_histogram,
        };
        let info = ClusteringStatisticsWrapper {
            cluster_key,
            cluster_type,
            timestamp,
            info,
        };

        build_block(info)
    }

    #[async_backtrace::framed]
    async fn get_hilbert_clustering_info(&self) -> Result<DataBlock> {
        let Some((cluster_key_id, cluster_key_str)) = self.table.cluster_key_meta() else {
            unreachable!("Unclustered table {}", self.table.table_info.desc);
        };

        let snapshot = self.table.read_table_snapshot().await?;
        let now = Utc::now();
        let timestamp = snapshot
            .as_ref()
            .map_or(now, |s| s.timestamp.unwrap_or(now))
            .timestamp_micros();
        let mut total_segment_count = 0;
        let mut total_block_count = 0;
        let mut stable_segment_count = 0;
        let mut stable_block_count = 0;
        let mut partial_segment_count = 0;
        let mut partial_block_count = 0;
        let mut unclustered_segment_count = 0;
        let mut unclustered_block_count = 0;
        if let Some(snapshot) = snapshot {
            let total_count = snapshot.segments.len();
            total_segment_count = total_count as u64;
            total_block_count = snapshot.summary.block_count;
            let chunk_size = cmp::min(
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
                    .read_segments::<Arc<CompactSegmentInfo>>(chunk, true)
                    .await?;
                for segment in segments {
                    let segment = segment?;
                    if segment
                        .summary
                        .cluster_stats
                        .as_ref()
                        .is_none_or(|v| v.cluster_key_id != cluster_key_id)
                    {
                        unclustered_segment_count += 1;
                        unclustered_block_count += segment.summary.block_count;
                        continue;
                    }
                    let level = segment.summary.cluster_stats.as_ref().unwrap().level;
                    if level == -1 {
                        stable_block_count += segment.summary.block_count;
                        stable_segment_count += 1;
                    } else {
                        partial_block_count += segment.summary.block_count;
                        partial_segment_count += 1;
                    }
                }
            }
        }

        let info = HilbertClusterStatistics {
            total_segment_count,
            stable_segment_count,
            partial_segment_count,
            unclustered_segment_count,
            total_block_count,
            stable_block_count,
            partial_block_count,
            unclustered_block_count,
        };

        let stats = ClusteringStatisticsWrapper {
            cluster_key: cluster_key_str.to_string(),
            cluster_type: "hilbert".to_string(),
            timestamp,
            info,
        };

        build_block(stats)
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

fn build_block<A: Serialize>(info: ClusteringStatisticsWrapper<A>) -> Result<DataBlock> {
    Ok(DataBlock::new(
        vec![
            BlockEntry::new_const_column_arg::<StringType>(info.cluster_key, 1),
            BlockEntry::new_const_column_arg::<StringType>(info.cluster_type, 1),
            BlockEntry::new_const_column_arg::<TimestampType>(info.timestamp, 1),
            BlockEntry::new_const_column_arg::<VariantType>(
                JsonbValue::from(serde_json::to_value(&info.info)?).to_vec(),
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
    block_depth_histogram: BTreeMap<String, u64>,
}

#[derive(Serialize)]
struct HilbertClusterStatistics {
    total_segment_count: u64,
    stable_segment_count: u64,
    partial_segment_count: u64,
    unclustered_segment_count: u64,
    total_block_count: u64,
    stable_block_count: u64,
    partial_block_count: u64,
    unclustered_block_count: u64,
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
