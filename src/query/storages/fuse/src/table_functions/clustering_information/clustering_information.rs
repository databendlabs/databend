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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::number::NumberScalar;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use itertools::Itertools;
use jsonb::Value as JsonbValue;
use serde_json::json;
use serde_json::Value as JsonValue;
use storages_common_table_meta::meta::SegmentInfo;

use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::cmp_with_null;
use crate::FuseTable;
use crate::Table;

pub struct ClusteringInformation<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
}

struct ClusteringStatistics {
    total_block_count: u64,
    constant_block_count: u64,
    unclustered_block_count: u64,
    average_overlaps: f64,
    average_depth: f64,
    block_depth_histogram: JsonValue,
}

impl Default for ClusteringStatistics {
    fn default() -> Self {
        ClusteringStatistics {
            total_block_count: 0,
            constant_block_count: 0,
            unclustered_block_count: 0,
            average_overlaps: 0.0,
            average_depth: 0.0,
            block_depth_histogram: json!({}),
        }
    }
}

impl<'a> ClusteringInformation<'a> {
    pub fn new(ctx: Arc<dyn TableContext>, table: &'a FuseTable) -> Self {
        Self { ctx, table }
    }

    #[async_backtrace::framed]
    pub async fn get_clustering_info(&self) -> Result<DataBlock> {
        if self.table.cluster_key_meta.is_none() {
            return Err(ErrorCode::UnclusteredTable(format!(
                "Unclustered table {}",
                self.table.table_info.desc
            )));
        }

        let snapshot = self.table.read_table_snapshot().await?;
        if snapshot.is_none() {
            return self.build_block(ClusteringStatistics::default());
        }
        let snapshot = snapshot.unwrap();

        // Gather all cluster statistics points to a hash Map.
        // Key: The cluster statistics points.
        // Value: 0: The block indexes with key as min value;
        //        1: The block indexes with key as max value;
        let mut points_map: HashMap<Vec<Scalar>, (Vec<u64>, Vec<u64>)> = HashMap::new();
        let mut constant_block_count = 0;
        let mut unclustered_block_count = 0;
        let mut index = 0;

        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.table.operator.clone(),
            self.table.schema(),
        );
        let default_cluster_key_id = self.table.cluster_key_meta.clone().unwrap().0;
        let total_block_count = snapshot.summary.block_count;
        let chunk_size = self.ctx.get_settings().get_max_threads()? as usize * 4;
        for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<Arc<SegmentInfo>>(chunk, true)
                .await?;

            for segment in segments.into_iter().flatten() {
                for block in &segment.blocks {
                    if let Some(cluster_stats) = &block.cluster_stats {
                        if cluster_stats.cluster_key_id != default_cluster_key_id {
                            unclustered_block_count += 1;
                        } else {
                            if cluster_stats.is_const() {
                                constant_block_count += 1;
                            }
                            points_map
                                .entry(cluster_stats.min())
                                .and_modify(|v| v.0.push(index))
                                .or_insert((vec![index], vec![]));
                            points_map
                                .entry(cluster_stats.max())
                                .and_modify(|v| v.1.push(index))
                                .or_insert((vec![], vec![index]));
                            index += 1;
                        }
                    } else {
                        unclustered_block_count += 1;
                    }
                }
            }
        }
        drop(snapshot);

        // calculate overlaps and depth.
        let mut stats = Vec::new();
        // key: the block index.
        // value: (overlaps, depth).
        let mut unfinished_parts: HashMap<u64, (usize, usize)> = HashMap::new();
        for (_, (start, end)) in points_map
            .into_iter()
            .sorted_by(|(a, _), (b, _)| a.iter().cmp_by(b.iter(), cmp_with_null))
        {
            let point_depth = unfinished_parts.len() + start.len();

            unfinished_parts.values_mut().for_each(|(overlaps, depth)| {
                *overlaps += start.len();
                *depth = cmp::max(*depth, point_depth);
            });

            start.iter().for_each(|&idx| {
                unfinished_parts.insert(idx, (point_depth - 1, point_depth));
            });

            end.iter().for_each(|idx| {
                if let Some(v) = unfinished_parts.remove(idx) {
                    stats.push(v);
                }
            });
        }
        assert!(unfinished_parts.is_empty());

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
            total_block_count,
            constant_block_count,
            unclustered_block_count,
            average_overlaps,
            average_depth,
            block_depth_histogram,
        };

        self.build_block(info)
    }

    fn build_block(&self, info: ClusteringStatistics) -> Result<DataBlock> {
        let cluster_by_keys = self
            .table
            .cluster_key_str()
            .ok_or(ErrorCode::Internal("It's a bug"))?;
        Ok(DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(cluster_by_keys.as_bytes().to_vec())),
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
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt64(
                        info.unclustered_block_count,
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

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("cluster_by_keys", TableDataType::String),
            TableField::new(
                "total_block_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "constant_block_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "unclustered_block_count",
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
