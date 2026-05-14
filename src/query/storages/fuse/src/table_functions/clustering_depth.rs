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
use std::collections::HashMap;

use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::compare_scalars;
use databend_common_expression::types::DataType;

#[derive(Clone, Copy, Default)]
pub(super) struct BlockOverlapDepth {
    pub overlap: usize,
    pub depth: usize,
}

pub(super) fn calculate_block_overlap_depths(
    ranges: &[(Vec<Scalar>, Vec<Scalar>)],
    cluster_key_types: &[DataType],
) -> Result<Vec<BlockOverlapDepth>> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }

    let mut points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = HashMap::new();
    for (index, (min, max)) in ranges.iter().enumerate() {
        points_map
            .entry(min.clone())
            .and_modify(|v| v.0.push(index))
            .or_insert((vec![index], vec![]));
        points_map
            .entry(max.clone())
            .and_modify(|v| v.1.push(index))
            .or_insert((vec![], vec![index]));
    }

    let mut stats = vec![BlockOverlapDepth::default(); ranges.len()];
    let mut unfinished_parts: HashMap<usize, BlockOverlapDepth> = HashMap::new();
    let (keys, values): (Vec<_>, Vec<_>) = points_map.into_iter().unzip();
    let indices = compare_scalars(keys, cluster_key_types)?;
    for idx in indices.into_iter() {
        let start = &values[idx as usize].0;
        let end = &values[idx as usize].1;
        let point_depth = unfinished_parts.len() + start.len();

        unfinished_parts.values_mut().for_each(|stat| {
            stat.overlap += start.len();
            stat.depth = cmp::max(stat.depth, point_depth);
        });

        start.iter().for_each(|idx| {
            unfinished_parts.insert(*idx, BlockOverlapDepth {
                overlap: point_depth - 1,
                depth: point_depth,
            });
        });

        end.iter().for_each(|idx| {
            if let Some(stat) = unfinished_parts.remove(idx) {
                stats[*idx] = stat;
            }
        });
    }

    Ok(stats)
}
