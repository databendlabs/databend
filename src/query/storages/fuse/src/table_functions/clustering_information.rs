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
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::compare_scalars;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::analyze_cluster_keys;
use databend_storages_common_index::statistics_to_domain;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::table::ClusterType;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use jsonb::Value as JsonbValue;
use log::warn;
use serde_json::json;
use serde_json::Value as JsonValue;

use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::cmp_with_null;
use crate::table_functions::parse_db_tb_opt_args;
use crate::table_functions::string_literal;
use crate::table_functions::SimpleTableFunc;
use crate::FuseTable;
use crate::Table;

pub struct ClusteringInformationTable {
    args: ClusteringInformationArgs,
}

#[async_trait::async_trait]
impl SimpleTableFunc for ClusteringInformationTable {
    fn get_engine_name(&self) -> String {
        "clustering_information".to_owned()
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> TableSchemaRef {
        match &self.args.cluster_option {
            ClusteringOption::Linear(_) => ClusteringInformation::schema(),
            ClusteringOption::Hilbert => HilbertClusteringInfo::schema(),
        }
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        _: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_table(
                &tenant_id,
                self.args.database_name.as_str(),
                self.args.table_name.as_str(),
            )
            .await?;

        let tbl = FuseTable::try_from_table(tbl.as_ref())?;
        let res = match &self.args.cluster_option {
            ClusteringOption::Linear(cluster_key) => {
                ClusteringInformation::new(
                    ctx.clone(),
                    tbl,
                    &self.args.database_name,
                    &self.args.table_name,
                    cluster_key.clone(),
                )
                .get_clustering_info()
                .await
            }
            ClusteringOption::Hilbert => {
                HilbertClusteringInfo::new(
                    ctx.clone(),
                    tbl,
                    &self.args.database_name,
                    &self.args.table_name,
                )
                .get_clustering_info()
                .await
            }
        }?;
        Ok(Some(res))
    }

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let (database_name, table_name, cluster_opt_str) =
            parse_db_tb_opt_args(&table_args, func_name)?;
        let cluster_option = match cluster_opt_str.map(|v| v.to_lowercase()).as_deref() {
            Some("hilbert") => ClusteringOption::Hilbert,
            Some("linear") | None => ClusteringOption::Linear(None),
            Some(other) => ClusteringOption::Linear(Some(other.to_string())),
        };

        Ok(Self {
            args: ClusteringInformationArgs {
                database_name,
                table_name,
                cluster_option,
            },
        })
    }
}

enum ClusteringOption {
    Linear(Option<String>),
    Hilbert,
}

struct ClusteringInformationArgs {
    database_name: String,
    table_name: String,
    cluster_option: ClusteringOption,
}

impl From<&ClusteringInformationArgs> for TableArgs {
    fn from(args: &ClusteringInformationArgs) -> Self {
        let mut tbl_args = Vec::new();
        tbl_args.push(string_literal(args.database_name.as_str()));
        tbl_args.push(string_literal(args.table_name.as_str()));
        match &args.cluster_option {
            ClusteringOption::Linear(cluster_keys) => {
                if let Some(cluster_keys) = cluster_keys {
                    tbl_args.push(string_literal(cluster_keys));
                }
            }
            ClusteringOption::Hilbert => {
                tbl_args.push(string_literal("hilbert"));
            }
        }

        TableArgs::new_positioned(tbl_args)
    }
}

struct ClusteringStatistics {
    cluster_key: String,
    cluster_type: String,
    timestamp: i64,
    total_block_count: u64,
    constant_block_count: u64,
    average_overlaps: f64,
    average_depth: f64,
    block_depth_histogram: JsonValue,
}

struct ClusteringInformation<'a> {
    ctx: Arc<dyn TableContext>,
    table: &'a FuseTable,
    database_name: &'a str,
    table_name: &'a str,
    cluster_key: Option<String>,
}

impl<'a> ClusteringInformation<'a> {
    fn new(
        ctx: Arc<dyn TableContext>,
        table: &'a FuseTable,
        database_name: &'a str,
        table_name: &'a str,
        cluster_key: Option<String>,
    ) -> Self {
        Self {
            ctx,
            table,
            database_name,
            table_name,
            cluster_key,
        }
    }

    #[async_backtrace::framed]
    async fn get_clustering_info(&self) -> Result<DataBlock> {
        let mut default_cluster_key_id = None;
        let (cluster_key, exprs) = match (self.table.cluster_key_str(), &self.cluster_key) {
            (a, Some(b)) => {
                let (cluster_key, exprs) =
                    analyze_cluster_keys(self.ctx.clone(), Arc::new(self.table.clone()), b)?;
                let exprs = exprs
                    .iter()
                    .map(|k| {
                        k.project_column_ref(|index| {
                            self.table.schema().field(*index).name().to_string()
                        })
                    })
                    .collect::<Vec<_>>();
                if a.is_some() && a.unwrap() == &cluster_key {
                    default_cluster_key_id = self.table.cluster_key_meta.clone().map(|v| v.0);
                }
                (cluster_key, exprs)
            }
            (Some(a), None) => {
                let exprs = self.table.linear_cluster_keys(self.ctx.clone());
                let exprs = exprs
                    .iter()
                    .map(|k| k.as_expr(&BUILTIN_FUNCTIONS))
                    .collect();
                default_cluster_key_id = self.table.cluster_key_meta.clone().map(|v| v.0);
                (a.clone(), exprs)
            }
            _ => {
                return Err(ErrorCode::UnclusteredTable(format!(
                    "Unclustered table '{}'.'{}'",
                    self.database_name, self.table_name
                )));
            }
        };

        if default_cluster_key_id.is_some() {
            let typ = self
                .table
                .get_option(OPT_KEY_CLUSTER_TYPE, ClusterType::Linear);
            if matches!(typ, ClusterType::Hilbert) {
                return Err(ErrorCode::UnsupportedClusterType(format!(
                    "Table '{}'.'{}' uses hilbert clustering, please use `clustering_information('{}', '{}', 'hilbert')` instead",
                    self.database_name, self.table_name, self.database_name, self.table_name
                )));
            }
        }
        let cluster_type = "linear".to_string();

        let snapshot = self.table.read_table_snapshot().await?;
        let now = Utc::now();
        let timestamp = snapshot
            .as_ref()
            .map_or(now, |s| s.timestamp.unwrap_or(now))
            .timestamp_micros();
        if snapshot.is_none() {
            return self.build_block(ClusteringStatistics {
                cluster_key,
                cluster_type,
                timestamp,
                total_block_count: 0,
                constant_block_count: 0,
                average_overlaps: 0.0,
                average_depth: 0.0,
                block_depth_histogram: json!({}),
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
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;

            for segment in segments.into_iter().flatten() {
                for block in &segment.blocks {
                    let (min, max) =
                        get_min_max_stats(&exprs, block, schema.clone(), default_cluster_key_id);
                    assert_eq!(min.len(), max.len());
                    let (min, max) = match min.iter().cmp_by(max.iter(), cmp_with_null) {
                        Ordering::Equal => {
                            constant_block_count += 1;
                            (min, max)
                        }
                        Ordering::Less => (min, max),
                        Ordering::Greater => {
                            warn!(
                                "clustering_information: please check your data and perform recluster to resort."
                            );
                            (max, min)
                        }
                    };

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
                acc.entry(bucket).and_modify(|v| *v += 1).or_insert(1u32);
                acc
            });
        // round the float to 4 decimal places.
        let average_depth = (10000.0 * sum_depth as f64 / length as f64).round() / 10000.0;
        let average_overlaps = (10000.0 * sum_overlap as f64 / length as f64).round() / 10000.0;

        let map_len = mp.len();
        let objects = mp.into_iter().fold(
            serde_json::Map::with_capacity(map_len),
            |mut acc, (bucket, count)| {
                acc.insert(format!("{:05}", bucket), json!(count));
                acc
            },
        );
        let block_depth_histogram = JsonValue::Object(objects);
        let info = ClusteringStatistics {
            cluster_key,
            cluster_type,
            timestamp,
            total_block_count,
            constant_block_count,
            average_overlaps,
            average_depth,
            block_depth_histogram,
        };

        self.build_block(info)
    }

    fn build_block(&self, info: ClusteringStatistics) -> Result<DataBlock> {
        Ok(DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(info.cluster_key)),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(info.cluster_type)),
                ),
                BlockEntry::new(
                    DataType::Timestamp,
                    Value::Scalar(Scalar::Timestamp(info.timestamp)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(info.total_block_count))),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(
                        info.constant_block_count,
                    ))),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::Float64),
                    Value::Scalar(Scalar::Number(NumberScalar::Float64(
                        info.average_overlaps.into(),
                    ))),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::Float64),
                    Value::Scalar(Scalar::Number(NumberScalar::Float64(
                        info.average_depth.into(),
                    ))),
                ),
                BlockEntry::new(
                    DataType::Variant,
                    Value::Scalar(Scalar::Variant(
                        JsonbValue::from(&info.block_depth_histogram).to_vec(),
                    )),
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
                "total_block_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "constant_block_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "average_overlaps",
                TableDataType::Number(NumberDataType::Float64),
            ),
            TableField::new(
                "average_depth",
                TableDataType::Number(NumberDataType::Float64),
            ),
            TableField::new("block_depth_histogram", TableDataType::Variant),
        ])
    }
}

struct HilbertClusteringInfo<'a> {
    ctx: Arc<dyn TableContext>,
    table: &'a FuseTable,

    database_name: &'a str,
    table_name: &'a str,
}

impl<'a> HilbertClusteringInfo<'a> {
    fn new(
        ctx: Arc<dyn TableContext>,
        table: &'a FuseTable,
        database_name: &'a str,
        table_name: &'a str,
    ) -> Self {
        Self {
            ctx,
            table,
            database_name,
            table_name,
        }
    }

    #[async_backtrace::framed]
    async fn get_clustering_info(&self) -> Result<DataBlock> {
        let Some((cluster_key_id, cluster_key_str)) = self.table.cluster_key_meta() else {
            return Err(ErrorCode::UnclusteredTable(format!(
                "Unclustered table '{}'.'{}'",
                self.database_name, self.table_name
            )));
        };
        let cluster_type = self
            .table
            .get_option(OPT_KEY_CLUSTER_TYPE, ClusterType::Linear);
        if matches!(cluster_type, ClusterType::Linear) {
            return Err(ErrorCode::UnsupportedClusterType( format!(
                "Table '{}'.'{}' uses linear clustering, please use `clustering_information('{}', '{}')` instead",
                self.database_name,self.table_name, self.database_name,self.table_name)));
        }

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
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(total_block_count))),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(stable_segment_count))),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(stable_block_count))),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(partial_segment_count))),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(partial_block_count))),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(
                        unclustered_segment_count,
                    ))),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(
                        unclustered_block_count,
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
                "total_block_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "stable_segment_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "stable_block_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "partial_segment_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "partial_block_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "unclustered_segment_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "unclustered_block_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
        ])
    }
}

fn get_min_max_stats(
    exprs: &[Expr<String>],
    block: &BlockMeta,
    schema: Arc<TableSchema>,
    default_key_id: Option<u32>,
) -> (Vec<Scalar>, Vec<Scalar>) {
    if let Some(default_key_id) = default_key_id {
        if let Some(v) = block.cluster_stats.as_ref() {
            if v.cluster_key_id == default_key_id {
                return (v.min.clone(), v.max.clone());
            }
        }
    }

    let func_ctx = FunctionContext::default();
    let mut mins = Vec::with_capacity(exprs.len());
    let mut maxs = Vec::with_capacity(exprs.len());
    let col_stats = &block.col_stats;
    for expr in exprs {
        // Since the hilbert index does not calc domain, set min max directly.
        if expr.data_type().remove_nullable() == DataType::Binary {
            mins.push(Scalar::Binary(vec![]));
            maxs.push(Scalar::Binary(vec![0xFF, 40]));
            continue;
        }
        let input_domains = expr
            .column_refs()
            .into_iter()
            .map(|(name, ty)| {
                let column_ids = schema.leaf_columns_of(&name);
                let stats = column_ids
                    .iter()
                    .filter_map(|column_id| col_stats.get(column_id))
                    .collect();

                let domain = statistics_to_domain(stats, &ty);
                (name, domain)
            })
            .collect();

        let (_, domain_opt) =
            ConstantFolder::fold_with_domain(expr, &input_domains, &func_ctx, &BUILTIN_FUNCTIONS);
        let domain = domain_opt.unwrap_or_else(|| Domain::full(expr.data_type()));
        let (min, max) = domain.to_minmax();
        mins.push(min);
        maxs.push(max);
    }
    (mins, maxs)
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
