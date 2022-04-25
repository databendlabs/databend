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

use std::collections::HashMap;

use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::storages::fuse::meta::BlockMeta;

#[derive(Clone)]
pub struct Points {
    points_map: HashMap<Vec<DataValue>, (Vec<BlockMeta>, Vec<BlockMeta>)>,
    const_blocks: Vec<BlockMeta>,
}

fn gather_parts(blocks: Vec<BlockMeta>, keys: Vec<u32>) -> Result<Points> {
    // 入参， Vec<BlockMeta>, Vec<u32>
    // 所有的blocks.
    // 获取其cluster key的minmax统计信息。 min： Vec<DataValue>, max: Vec<DataValue>.需要知道cluster key的index. Vec<u32>,
    let mut const_blocks = Vec::new();
    let mut points_map: HashMap<Vec<DataValue>, (Vec<BlockMeta>, Vec<BlockMeta>)> = HashMap::new();
    let size = keys.len();
    for block in blocks {
        let mut min = Vec::with_capacity(size);
        let mut max = Vec::with_capacity(size);
        for key in &keys {
            let stat = block.col_stats.get(key).ok_or_else(|| {
                ErrorCode::UnknownException(format!(
                    "Unable to get the colStats by ColumnId: {}",
                    key
                ))
            })?;
            min.push(stat.min);
            max.push(stat.max);
        }

        if min.eq(&max) {
            const_blocks.push(block);
            continue;
        }

        match points_map.get_mut(&min) {
            None => {
                points_map.insert(min, (vec![block.clone()], vec![]));
            }
            Some((v, _)) => {
                v.push(block.clone());
            }
        };

        match points_map.get_mut(&max) {
            None => {
                points_map.insert(max, (vec![], vec![block.clone()]));
            }
            Some((_, v)) => {
                v.push(block.clone());
            }
        };
    }
    Ok(Points {
        points_map,
        const_blocks,
    })
}

fn sort_points(points: Vec<DataValue>) {}
