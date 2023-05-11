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
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use jsonb::Value as JsonbValue;
use serde_json::json;
use serde_json::Value as JsonValue;
use storages_common_table_meta::meta::BlockMeta;

use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::FuseTable;
use crate::Table;

pub struct ClusteringInformation<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
    pub plain_cluster_keys: String,
    pub cluster_keys: Vec<RemoteExpr<String>>,
}

struct ClusteringStatistics {
    total_block_count: u64,
    total_constant_block_count: u64,
    average_overlaps: f64,
    average_depth: f64,
    block_depth_histogram: JsonValue,
}

impl Default for ClusteringStatistics {
    fn default() -> Self {
        ClusteringStatistics {
            total_block_count: 0,
            total_constant_block_count: 0,
            average_overlaps: 0.0,
            average_depth: 0.0,
            block_depth_histogram: json!({}),
        }
    }
}

impl<'a> ClusteringInformation<'a> {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        table: &'a FuseTable,
        plain_cluster_keys: String,
        cluster_keys: Vec<RemoteExpr<String>>,
    ) -> Self {
        Self {
            ctx,
            table,
            plain_cluster_keys,
            cluster_keys,
        }
    }

    #[async_backtrace::framed]
    pub async fn get_clustering_info(&self) -> Result<DataBlock> {
        let snapshot = self.table.read_table_snapshot().await?;

        let mut info = ClusteringStatistics::default();
        if let Some(snapshot) = snapshot {
            let segment_locations = &snapshot.segments;
            let segments_io = SegmentsIO::create(
                self.ctx.clone(),
                self.table.operator.clone(),
                self.table.schema(),
            );
            let segments = segments_io
                .read_segments(segment_locations, true)
                .await?
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            if !segments.is_empty() {
                let blocks = segments.iter().flat_map(|s| s.blocks.iter());
                info = self.get_clustering_stats(blocks)?
            }
        };

        let cluster_by_keys = self.plain_cluster_keys.clone();

        Ok(DataBlock::new(
            vec![
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Scalar(Scalar::String(cluster_by_keys.as_bytes().to_vec())),
                },
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::UInt64),
                    value: Value::Scalar(Scalar::Number(NumberScalar::UInt64(
                        info.total_block_count,
                    ))),
                },
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::UInt64),
                    value: Value::Scalar(Scalar::Number(NumberScalar::UInt64(
                        info.total_constant_block_count,
                    ))),
                },
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::Float64),
                    value: Value::Scalar(Scalar::Number(NumberScalar::Float64(
                        info.average_overlaps.into(),
                    ))),
                },
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::Float64),
                    value: Value::Scalar(Scalar::Number(NumberScalar::Float64(
                        info.average_depth.into(),
                    ))),
                },
                BlockEntry {
                    data_type: DataType::Variant,
                    value: Value::Scalar(Scalar::Variant(
                        JsonbValue::from(&info.block_depth_histogram).to_vec(),
                    )),
                },
            ],
            1,
        ))
    }

    fn get_min_max_stats(&self, block: &BlockMeta) -> Result<(Vec<Scalar>, Vec<Scalar>)> {
        if self.table.cluster_keys(self.ctx.clone()) != self.cluster_keys
            || block.cluster_stats.is_none()
        {
            // Todo(zhyass): support manually specifying the cluster key.
            return Err(ErrorCode::Unimplemented("Unimplemented"));
        }

        let cluster_key_id = block.cluster_stats.clone().unwrap().cluster_key_id;
        let default_cluster_key_id = self.table.cluster_key_meta.clone().unwrap().0;
        if cluster_key_id != default_cluster_key_id {
            return Err(ErrorCode::Unimplemented("Unimplemented"));
        }

        let cluster_stats = block.cluster_stats.clone().unwrap();
        Ok((cluster_stats.min, cluster_stats.max))
    }

    fn get_clustering_stats<'b>(
        &self,
        blocks: impl Iterator<Item = &'b Arc<BlockMeta>>,
    ) -> Result<ClusteringStatistics> {
        // Gather all cluster statistics points to a sorted Map.
        // Key: The cluster statistics points.
        // Value: 0: The block indexes with key as min value;
        //        1: The block indexes with key as max value;
        let mut points_map: BTreeMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = BTreeMap::new();
        let mut total_constant_block_count = 0;
        let mut total_block_count = 0;
        for (i, block) in blocks.enumerate() {
            let (min, max) = self.get_min_max_stats(block.as_ref())?;
            if min.eq(&max) {
                total_constant_block_count += 1;
            }

            points_map
                .entry(min.clone())
                .and_modify(|v| v.0.push(i))
                .or_insert((vec![i], vec![]));

            points_map
                .entry(max.clone())
                .and_modify(|v| v.1.push(i))
                .or_insert((vec![], vec![i]));
            total_block_count += 1;
        }

        // calculate overlaps and depth.
        let mut stats = Vec::new();
        // key: the block index.
        // value: (overlaps, depth).
        let mut unfinished_parts: HashMap<usize, (usize, usize)> = HashMap::new();
        for (start, end) in points_map.values() {
            let point_depth = unfinished_parts.len() + start.len();

            for (_, val) in unfinished_parts.iter_mut() {
                val.0 += start.len();
                val.1 = cmp::max(val.1, point_depth);
            }

            start.iter().for_each(|&idx| {
                unfinished_parts.insert(idx, (point_depth - 1, point_depth));
            });

            end.iter().for_each(|&idx| {
                let stat = unfinished_parts.remove(&idx).unwrap();
                stats.push(stat);
            });
        }
        assert_eq!(unfinished_parts.len(), 0);

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

        let objects = mp.iter().fold(
            serde_json::Map::with_capacity(mp.len()),
            |mut acc, (bucket, count)| {
                acc.insert(format!("{:05}", bucket), json!(count));
                acc
            },
        );
        let block_depth_histogram = JsonValue::Object(objects);

        Ok(ClusteringStatistics {
            total_block_count,
            total_constant_block_count,
            average_overlaps,
            average_depth,
            block_depth_histogram,
        })
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("cluster_by_keys", TableDataType::String),
            TableField::new(
                "total_block_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "total_constant_block_count",
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

// The histogram contains buckets with widths:
// 1 to 16 with increments of 1.
// For buckets larger than 16, increments of twice the width of the previous bucket (e.g. 32, 64, 128, …).
// e.g. If val is 2, the bucket is 2. If val is 18, the bucket is 32.
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
