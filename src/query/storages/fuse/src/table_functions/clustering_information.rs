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
use std::time::Instant;

use chrono::Utc;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::parse_table_name;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::BinaryColumn;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_pipeline_transforms::sorts::core::RowConverter;
use databend_common_pipeline_transforms::sorts::core::VariableRowConverter;
use databend_common_sql::analyze_cluster_keys;
use databend_common_sql::parse_cluster_keys;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::ClusterType;
use jsonb::Value as JsonbValue;
use log::info;
use serde::Deserialize;
use serde::Serialize;

use crate::FuseTable;
use crate::Table;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::statistics::cluster_key_types_for_depth;
use crate::statistics::get_min_max_stats;
use crate::statistics::hilbert_bounds_for_diagnostics;
use crate::statistics::hilbert_diagnostics;
use crate::statistics::prepare_cluster_key_exprs;
use crate::statistics::sort_endpoints;
use crate::statistics::with_request_pool;
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
    if table.cluster_key_meta().is_none() && cluster_key.is_none() {
        return Err(ErrorCode::UnclusteredTable(format!(
            "Unclustered table {}",
            table.table_info.desc,
        )));
    }
    ClusteringInformationImpl { ctx, table }
        .calculate_clustering_info(cluster_key)
        .await
}

#[async_trait::async_trait]
impl SimpleArgFunc for ClusteringInformation {
    type Args = ClusteringInformationArgs;

    fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("cluster_key", TableDataType::String),
            TableField::new("type", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp),
            TableField::new("info", TableDataType::Variant),
        ])
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
}

struct ResolvedClusterKey {
    display: String,
    stats_exprs: Vec<Expr<usize>>,
    default_key_id: Option<u32>,
    is_hilbert: bool,
}

impl ResolvedClusterKey {
    fn cluster_type(&self) -> String {
        if self.is_hilbert {
            ClusterType::Hilbert
        } else {
            ClusterType::Linear
        }
        .to_string()
    }
}

struct CollectedMetadata {
    key_types: Vec<DataType>,
    endpoint_builders: Option<Vec<ColumnBuilder>>,
    block_count: usize,
    constant_block_count: u64,
}

struct ClusteringInformationImpl<'a> {
    ctx: Arc<dyn TableContext>,
    table: &'a FuseTable,
}

impl ClusteringInformationImpl<'_> {
    fn resolve_cluster_key(&self, cluster_key: &Option<String>) -> Result<ResolvedClusterKey> {
        let table: Arc<dyn Table> = Arc::new(self.table.clone());
        match (self.table.cluster_key_str(), cluster_key) {
            (default_key, Some(custom_key)) => {
                let (normalized_key, custom_exprs) =
                    analyze_cluster_keys(self.ctx.clone(), table.clone(), custom_key)?;
                if default_key == Some(normalized_key.as_str()) {
                    return self.resolve_default_cluster_key(table, normalized_key);
                }

                let stats_exprs = custom_exprs
                    .into_iter()
                    .map(|expr| expr.project_column_ref(|index| Ok(index.as_usize())))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .filter(|expr| {
                        !matches!(expr.data_type().remove_nullable(), DataType::Vector(_))
                    })
                    .collect();
                Ok(ResolvedClusterKey {
                    display: normalized_key,
                    stats_exprs,
                    default_key_id: None,
                    is_hilbert: false,
                })
            }
            (Some(default_key), None) => {
                self.resolve_default_cluster_key(table, default_key.to_string())
            }
            _ => unreachable!("Unclustered table {}", self.table.table_info.desc),
        }
    }

    fn resolve_default_cluster_key(
        &self,
        table: Arc<dyn Table>,
        display: String,
    ) -> Result<ResolvedClusterKey> {
        let parsed = parse_cluster_keys(
            self.ctx.clone(),
            table,
            self.table.resolve_cluster_keys().unwrap(),
        )?;
        let is_hilbert = parsed.is_hilbert();
        Ok(ResolvedClusterKey {
            display,
            stats_exprs: parsed.into_stats_keys(),
            default_key_id: self.table.cluster_key_id(),
            is_hilbert,
        })
    }

    async fn collect_metadata(
        &self,
        snapshot: &TableSnapshot,
        key: &ResolvedClusterKey,
    ) -> Result<CollectedMetadata> {
        let key_types = cluster_key_types_for_depth(&key.stats_exprs);
        debug_assert!(
            key_types.is_empty()
                || key_types
                    .iter()
                    .all(VariableRowConverter::support_data_type),
            "validated scalar cluster keys must support comparable-row encoding",
        );

        if key.is_hilbert && key_types.len() != 2 {
            return Err(ErrorCode::Internal(format!(
                "Hilbert clustering information requires exactly two key expressions, got {}",
                key_types.len()
            )));
        }
        let capacity = snapshot.summary.block_count as usize;
        // `clustering_information` intentionally measures only the declared cluster-key domains
        // across the whole table. PARTITION BY boundaries are outside this function's metric, even
        // though pruning and recluster execution are partition-local. Changing that established
        // definition and its reported values should be handled in a separate compatibility PR.
        let mut endpoint_builders = (!key_types.is_empty()).then(|| {
            key_types
                .iter()
                .map(|ty| ColumnBuilder::with_capacity(ty, capacity.saturating_mul(2)))
                .collect::<Vec<_>>()
        });
        let hilbert_key_id = key.default_key_id.filter(|_| key.is_hilbert);
        let prepared_exprs =
            prepare_cluster_key_exprs(&key.stats_exprs, self.table.schema().as_ref());
        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.table.operator.clone(),
            self.table.schema(),
        );
        let mut constant_block_count = 0u64;
        let mut block_count = 0usize;
        let chunk_size = self.ctx.get_settings().get_max_threads()?.max(1) as usize * 4;

        for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for segment in segments {
                let segment = segment?;
                for block in segment.blocks {
                    block_count += 1;
                    if let Some(cluster_key_id) = hilbert_key_id {
                        let bounds = hilbert_bounds_for_diagnostics(
                            &prepared_exprs,
                            &block.col_stats,
                            block.cluster_stats.as_ref(),
                            cluster_key_id,
                        );
                        constant_block_count +=
                            u64::from(bounds[0] == bounds[1] && bounds[2] == bounds[3]);
                        let builders = endpoint_builders.as_mut().ok_or_else(|| {
                            ErrorCode::Internal(
                                "Hilbert clustering information has no endpoint columns"
                                    .to_string(),
                            )
                        })?;
                        builders[0].push(bounds[0].as_ref());
                        builders[0].push(bounds[1].as_ref());
                        builders[1].push(bounds[2].as_ref());
                        builders[1].push(bounds[3].as_ref());
                    } else if let Some(builders) = endpoint_builders.as_mut() {
                        let (min, max) = get_min_max_stats(
                            &prepared_exprs,
                            &block.col_stats,
                            block.cluster_stats.as_ref(),
                            key.default_key_id,
                        );
                        debug_assert_eq!(min.len(), max.len());
                        constant_block_count += u64::from(min == max);
                        push_endpoint(builders, &min);
                        push_endpoint(builders, &max);
                    } else {
                        constant_block_count += 1;
                    }
                }
            }
        }

        Ok(CollectedMetadata {
            key_types,
            endpoint_builders,
            block_count,
            constant_block_count,
        })
    }

    fn build_hilbert_response(
        &self,
        key: ResolvedClusterKey,
        timestamp: i64,
        total_block_count: u64,
        metadata: CollectedMetadata,
        total_start: Instant,
    ) -> Result<ClusteringInformationResponse> {
        let cluster_type = key.cluster_type();
        let calculation_start = Instant::now();
        let (max_depth, overlap_pairs) = hilbert_diagnostics(
            metadata.endpoint_builders.unwrap_or_default(),
            &metadata.key_types,
            self.ctx.get_settings().get_max_threads()? as usize,
        )?;
        let info = HilbertClusterStatistics {
            total_block_count,
            constant_block_count: metadata.constant_block_count,
            // Each unordered pair contributes one overlapping neighbor to both rectangles.
            average_overlaps: rounded_average(
                overlap_pairs.saturating_mul(2),
                metadata.block_count,
            ),
            max_depth,
        };
        info!(
            "clustering_information: finished table={} cluster_type={} blocks={} constant_blocks={} average_overlaps={} max_depth={} calculation_elapsed={:?} total_elapsed={:?}",
            self.table.table_info.desc,
            cluster_type,
            info.total_block_count,
            info.constant_block_count,
            info.average_overlaps,
            info.max_depth,
            calculation_start.elapsed(),
            total_start.elapsed(),
        );
        Ok(ClusteringInformationResponse {
            cluster_key: key.display,
            cluster_type,
            timestamp,
            info: serde_json::to_value(info)?,
        })
    }

    fn build_linear_response(
        &self,
        key: ResolvedClusterKey,
        timestamp: i64,
        total_block_count: u64,
        metadata: CollectedMetadata,
        total_start: Instant,
    ) -> Result<ClusteringInformationResponse> {
        let cluster_type = key.cluster_type();
        let calculation_start = Instant::now();
        let CollectedMetadata {
            key_types,
            endpoint_builders,
            block_count,
            constant_block_count,
        } = metadata;
        let aggregate = match endpoint_builders {
            Some(builders) => {
                let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
                with_request_pool(max_threads, move |_| {
                    let sorted = sort_endpoints(builders, &key_types)?;
                    sweep_exact_statistics(&sorted.keys, &sorted.order)
                })?
            }
            None => LinearClusteringStatistics {
                block_count,
                depth_counts: if block_count == 0 {
                    BTreeMap::new()
                } else {
                    BTreeMap::from([(0, block_count as u64)])
                },
                ..Default::default()
            },
        };

        let info = if aggregate.block_count == 0 {
            LinearClusterStatistics {
                total_block_count,
                ..Default::default()
            }
        } else {
            LinearClusterStatistics {
                total_block_count,
                constant_block_count,
                average_overlaps: rounded_average(aggregate.sum_overlap, aggregate.block_count),
                average_depth: rounded_average(aggregate.sum_depth, aggregate.block_count),
                p95_depth: percentile_depth(&aggregate.depth_counts, aggregate.block_count, 95),
                p99_depth: percentile_depth(&aggregate.depth_counts, aggregate.block_count, 99),
                block_depth_histogram: {
                    let mut histogram = BTreeMap::new();
                    for (depth, count) in &aggregate.depth_counts {
                        let depth = *depth as u32;
                        let bucket = if depth <= 16 || depth.is_power_of_two() {
                            depth
                        } else {
                            depth.next_power_of_two()
                        };
                        *histogram.entry(format!("{:05}", bucket)).or_insert(0) += count;
                    }
                    histogram
                },
            }
        };
        info!(
            "clustering_information: finished table={} cluster_type={} blocks={} constant_blocks={} average_overlaps={} average_depth={} p95_depth={} p99_depth={} calculation_elapsed={:?} total_elapsed={:?}",
            self.table.table_info.desc,
            cluster_type,
            info.total_block_count,
            info.constant_block_count,
            info.average_overlaps,
            info.average_depth,
            info.p95_depth,
            info.p99_depth,
            calculation_start.elapsed(),
            total_start.elapsed(),
        );
        Ok(ClusteringInformationResponse {
            cluster_key: key.display,
            cluster_type,
            timestamp,
            info: serde_json::to_value(info)?,
        })
    }

    #[async_backtrace::framed]
    async fn calculate_clustering_info(
        &self,
        cluster_key: &Option<String>,
    ) -> Result<ClusteringInformationResponse> {
        let key = self.resolve_cluster_key(cluster_key)?;
        let cluster_type = key.cluster_type();
        let total_start = Instant::now();
        let now = Utc::now();
        let Some(snapshot) = self.table.read_table_snapshot().await? else {
            let cluster_type = key.cluster_type();
            let info = if key.is_hilbert {
                serde_json::to_value(HilbertClusterStatistics::default())?
            } else {
                serde_json::to_value(LinearClusterStatistics::default())?
            };
            return Ok(ClusteringInformationResponse {
                cluster_key: key.display,
                cluster_type,
                timestamp: now.timestamp_micros(),
                info,
            });
        };
        let timestamp = snapshot.timestamp.unwrap_or(now).timestamp_micros();
        info!(
            "clustering_information: started table={} cluster_type={} segments={} blocks={}",
            self.table.table_info.desc,
            cluster_type,
            snapshot.segments.len(),
            snapshot.summary.block_count,
        );

        let total_block_count = snapshot.summary.block_count;
        let metadata_start = Instant::now();
        let metadata = self.collect_metadata(&snapshot, &key).await?;
        info!(
            "clustering_information: metadata collected table={} segments={} blocks={} elapsed={:?}",
            self.table.table_info.desc,
            snapshot.segments.len(),
            total_block_count,
            metadata_start.elapsed(),
        );
        if key.is_hilbert {
            self.build_hilbert_response(key, timestamp, total_block_count, metadata, total_start)
        } else {
            self.build_linear_response(key, timestamp, total_block_count, metadata, total_start)
        }
    }
}

fn push_endpoint(builders: &mut [ColumnBuilder], endpoint: &[Scalar]) {
    debug_assert_eq!(builders.len(), endpoint.len());
    for (builder, value) in builders.iter_mut().zip(endpoint) {
        builder.push(value.as_ref());
    }
}

// `sum_overlap` counts both directions for each overlapping block pair.
#[derive(Debug, Default)]
struct LinearClusteringStatistics {
    pub block_count: usize,
    pub sum_overlap: u64,
    pub sum_depth: u64,
    pub depth_counts: BTreeMap<usize, u64>,
}

// Parents point to the next position with an equal-or-greater stabbing depth.
fn find_root(parent: &mut [u32], mut node: usize) -> usize {
    let mut root = node;
    while parent[root] as usize != root {
        root = parent[root] as usize;
    }
    while node != root {
        let next = parent[node] as usize;
        parent[node] = root as u32;
        node = next;
    }
    root
}

/// Exact closed-interval sweep. The monotone stack plus path-compressed union-find answers each
/// block's `[open, close]` range maximum in amortized near-constant time after endpoint sorting.
/// A close encountered before its caller-assigned open returns an internal error instead of
/// indexing the uninitialized opening-position sentinel.
fn sweep_exact_statistics(
    keys: &BinaryColumn,
    order: &[u32],
) -> Result<LinearClusteringStatistics> {
    let block_count = keys.len() / 2;
    let mut open_positions = vec![u32::MAX; block_count];
    let mut parent = Vec::<u32>::with_capacity(keys.len());
    let mut point_depths = Vec::<u32>::with_capacity(keys.len());
    let mut monotone_stack = Vec::<u32>::with_capacity(keys.len());
    let mut depth_counts = Vec::<u64>::new();
    let mut active = 0usize;
    let mut overlap_pairs = 0u64;
    let mut sum_depth = 0u64;
    let mut point_position = 0usize;
    let mut offset = 0usize;

    while offset < order.len() {
        // SAFETY: `order` is the permutation of `0..keys.len()` returned by `sort_endpoints`.
        let key = unsafe { keys.index_unchecked(order[offset] as usize) };
        // Process every equal endpoint as one coordinate. Starts contribute before ends are
        // removed, so blocks touching at this value overlap under closed-interval semantics.
        let mut end = offset + 1;
        while end < order.len()
            // SAFETY: the same sorted-permutation invariant applies to every order entry.
            && unsafe { keys.index_unchecked(order[end] as usize) } == key
        {
            end += 1;
        }

        let endpoints = &order[offset..end];
        let starts = endpoints
            .iter()
            .filter(|endpoint| **endpoint & 1 == 0)
            .count();
        let ends = endpoints.len() - starts;
        // Active intervals and starts are subsets of the validated block set, so point depth fits
        // the u32 representation used by the monotone stack and union-find arrays.
        let point_depth = active + starts;

        // Every new start overlaps all currently active blocks and all other starts at this same
        // coordinate. Each unordered pair is counted exactly once, at its later start.
        overlap_pairs = overlap_pairs
            .saturating_add((active as u64).saturating_mul(starts as u64))
            .saturating_add((starts as u64).saturating_mul(starts.saturating_sub(1) as u64) / 2);

        for endpoint in endpoints {
            if *endpoint & 1 == 0 {
                // There can be no more distinct coordinates than validated endpoint rows, so the
                // coordinate position fits the same u32 indexing limit as endpoint IDs.
                open_positions[*endpoint as usize / 2] = point_position as u32;
            }
        }

        while monotone_stack
            .last()
            .is_some_and(|position| point_depths[*position as usize] <= point_depth as u32)
        {
            let position = monotone_stack.pop().unwrap() as usize;
            parent[position] = point_position as u32;
        }
        parent.push(point_position as u32);
        point_depths.push(point_depth as u32);
        monotone_stack.push(point_position as u32);

        for endpoint in endpoints {
            if *endpoint & 1 == 1 {
                let block_id = *endpoint as usize / 2;
                let open_position = open_positions[block_id];
                if open_position == u32::MAX {
                    return Err(ErrorCode::Internal(format!(
                        "clustering information block {block_id} has a maximum endpoint before its minimum"
                    )));
                }
                let root = find_root(&mut parent, open_position as usize);
                let depth = point_depths[root] as usize;
                sum_depth = sum_depth.saturating_add(depth as u64);
                if depth >= depth_counts.len() {
                    depth_counts.resize(depth + 1, 0);
                }
                depth_counts[depth] += 1;
            }
        }

        // Starts and ends at this coordinate both participated in `point_depth`; remove closes only
        // after completing their range-maximum queries to preserve closed intervals.
        active = active + starts - ends;
        point_position += 1;
        offset = end;
    }
    debug_assert_eq!(active, 0);

    Ok(LinearClusteringStatistics {
        block_count,
        sum_overlap: overlap_pairs.saturating_mul(2),
        sum_depth,
        depth_counts: depth_counts
            .into_iter()
            .enumerate()
            .filter_map(|(depth, count)| (count != 0).then_some((depth, count)))
            .collect(),
    })
}

#[derive(Serialize, Default)]
struct LinearClusterStatistics {
    total_block_count: u64,
    constant_block_count: u64,
    average_overlaps: f64,
    average_depth: f64,
    p95_depth: usize,
    p99_depth: usize,
    block_depth_histogram: BTreeMap<String, u64>,
}

/// Exact scalable 2D diagnostics. Per-block average depth and its histogram are intentionally
/// omitted: computing each MBR's maximum internal stabbing depth requires an output-sensitive
/// overlap structure and can degrade to quadratic work for dense layouts.
#[derive(Serialize, Default)]
struct HilbertClusterStatistics {
    total_block_count: u64,
    constant_block_count: u64,
    average_overlaps: f64,
    max_depth: usize,
}

fn rounded_average(sum: u64, count: usize) -> f64 {
    if count == 0 {
        return 0.0;
    }
    (10000.0 * sum as f64 / count as f64).round() / 10000.0
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
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::number::NumberScalar;

    use super::*;

    fn int(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }

    fn calculate_linear(ranges: &[(i32, i32)], threads: usize) -> LinearClusteringStatistics {
        let ty = DataType::Number(NumberDataType::Int32);
        let mut builder = ColumnBuilder::with_capacity(&ty, ranges.len() * 2);
        for (min, max) in ranges {
            builder.push(int(*min).as_ref());
            builder.push(int(*max).as_ref());
        }
        with_request_pool(threads, move |_| {
            let sorted = sort_endpoints(vec![builder], std::slice::from_ref(&ty))?;
            sweep_exact_statistics(&sorted.keys, &sorted.order)
        })
        .unwrap()
    }

    #[test]
    fn test_linear_rejects_invalid_ranges() {
        let ty = DataType::Number(NumberDataType::Int32);
        assert!(sort_endpoints(Vec::new(), std::slice::from_ref(&ty)).is_err());

        let mut builder = ColumnBuilder::with_capacity(&ty, 2);
        builder.push(int(2).as_ref());
        builder.push(int(1).as_ref());
        let result = with_request_pool(1, move |_| {
            let sorted = sort_endpoints(vec![builder], std::slice::from_ref(&ty))?;
            sweep_exact_statistics(&sorted.keys, &sorted.order)
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_linear_boundary_semantics() {
        let actual = calculate_linear(&[(1, 2), (2, 3), (2, 2), (4, 5)], 4);
        assert_eq!(actual.sum_overlap, 6);
        assert_eq!(actual.sum_depth, 10);
        assert_eq!(actual.depth_counts, BTreeMap::from([(1, 1), (3, 3)]));
    }

    #[test]
    fn test_linear_random_differential() {
        let mut state = 0x8f3c_6d27_a491_b5e1u64;
        for case in 0..300 {
            let block_count = (next_random(&mut state) % 16) as usize;
            let ranges = (0..block_count)
                .map(|_| {
                    let left = (next_random(&mut state) % 12) as i32;
                    let right = (next_random(&mut state) % 12) as i32;
                    (left.min(right), left.max(right))
                })
                .collect::<Vec<_>>();
            let actual = calculate_linear(&ranges, if case % 2 == 0 { 1 } else { 4 });
            let overlap_pairs = (0..block_count)
                .flat_map(|left| ((left + 1)..block_count).map(move |right| (left, right)))
                .filter(|(left, right)| {
                    ranges[*left].0 <= ranges[*right].1 && ranges[*right].0 <= ranges[*left].1
                })
                .count() as u64;
            let mut depths = BTreeMap::new();
            let mut sum_depth = 0;
            for (min, max) in &ranges {
                let depth = (*min..=*max)
                    .map(|point| {
                        ranges
                            .iter()
                            .filter(|(a, b)| *a <= point && point <= *b)
                            .count()
                    })
                    .max()
                    .unwrap_or(0);
                sum_depth += depth as u64;
                *depths.entry(depth).or_insert(0) += 1;
            }
            assert_eq!(
                actual.sum_overlap,
                overlap_pairs * 2,
                "case {case}: {ranges:?}"
            );
            assert_eq!(actual.sum_depth, sum_depth, "case {case}: {ranges:?}");
            assert_eq!(actual.depth_counts, depths, "case {case}: {ranges:?}");
        }
    }

    fn next_random(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *state
    }
}
