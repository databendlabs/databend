// Copyright 2022 Datafuse Labs.
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
use std::collections::HashSet;

use common_arrow::arrow::compute::sort as arrow_sort;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use common_planners::ExpressionMonotonicityVisitor;
use common_planners::RequireColumnsVisitor;
use itertools::Itertools;
use serde_json::json;

use crate::storages::fuse::meta::BlockMeta;
use crate::storages::index::range_filter::check_maybe_monotonic;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ClusterStatistics {
    pub min: Vec<DataValue>,
    pub max: Vec<DataValue>,
}

#[derive(Clone)]
pub struct ClusteringInformationExecutor {
    blocks: Vec<BlockMeta>,
    // (start, end).
    points_map: BTreeMap<Vec<DataValue>, (Vec<usize>, Vec<usize>)>,
    const_block_count: usize,
}

pub struct ClusteringInformation {
    pub total_block_count: u64,
    pub total_constant_block_count: u64,
    pub average_overlaps: f64,
    pub average_depth: f64,
    pub block_depth_histogram: VariantValue,
}

impl ClusteringInformationExecutor {
    pub fn create_by_cluster(blocks: Vec<BlockMeta>) -> Result<Self> {
        let mut points_map: BTreeMap<Vec<DataValue>, (Vec<usize>, Vec<usize>)> = BTreeMap::new();
        let mut const_block_count = 0;
        for (i, block) in blocks.iter().enumerate() {
            // Todo(zhyass): if cluster_stats is none.
            let min = block.cluster_stats.clone().unwrap().min;
            let max = block.cluster_stats.clone().unwrap().max;
            if min.eq(&max) {
                const_block_count += 1;
            }

            points_map
                .entry(min.clone())
                .and_modify(|v| v.0.push(i))
                .or_insert((vec![i], vec![]));

            points_map
                .entry(max.clone())
                .and_modify(|v| v.1.push(i))
                .or_insert((vec![], vec![i]));
        }

        Ok(ClusteringInformationExecutor {
            blocks,
            points_map,
            const_block_count,
        })
    }

    pub fn execute(&self) -> Result<ClusteringInformation> {
        if self.blocks.is_empty() {
            return Ok(ClusteringInformation {
                total_block_count: 0,
                total_constant_block_count: 0,
                average_overlaps: 0.0,
                average_depth: 0.0,
                block_depth_histogram: VariantValue::from(json!(null)),
            });
        }

        let mut statis = Vec::new();
        let mut unfinished_parts: HashMap<usize, (usize, usize)> = HashMap::new();
        for (key, (start, end)) in &self.points_map {
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
                statis.push(stat);
            });
        }
        assert_eq!(unfinished_parts.len(), 0);

        let mut sum_overlap = 0;
        let mut sum_depth = 0;
        let length = statis.len();
        let mp = statis
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
        let block_depth_histogram = VariantValue::from(serde_json::Value::Object(objects));

        Ok(ClusteringInformation {
            total_block_count: self.blocks.len() as u64,
            total_constant_block_count: self.const_block_count as u64,
            average_overlaps,
            average_depth,
            block_depth_histogram,
        })
    }
}

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
