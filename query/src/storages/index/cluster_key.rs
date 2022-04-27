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
use std::collections::HashMap;

use common_arrow::arrow::compute::sort as arrow_sort;
use common_datavalues::prelude::*;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::storages::fuse::meta::BlockMeta;

#[derive(Clone)]
pub struct Points {
    blocks: Vec<BlockMeta>,
    // (start, end).
    points_map: HashMap<Vec<DataValue>, (Vec<usize>, Vec<usize>)>,
    const_blocks: Vec<usize>,
    fields: Vec<DataField>,
    //statis: Vec<Statis>,
}

fn gather_parts(blocks: Vec<BlockMeta>, keys: Vec<u32>, schema: DataSchemaRef) -> Result<Points> {
    // 入参， Vec<BlockMeta>, Vec<u32>
    // 所有的blocks.
    // 获取其cluster key的minmax统计信息。 min： Vec<DataValue>, max: Vec<DataValue>.需要知道cluster key的index. Vec<u32>,
    let mut const_blocks = Vec::new();
    let mut points_map: HashMap<Vec<DataValue>, (Vec<usize>, Vec<usize>)> = HashMap::new();
    let size = keys.len();
    for (idx, block) in blocks.iter().enumerate() {
        let mut min = Vec::with_capacity(size);
        let mut max = Vec::with_capacity(size);
        for key in &keys {
            let stat = block.col_stats.get(key).ok_or_else(|| {
                ErrorCode::UnknownException(format!(
                    "Unable to get the colStats by ColumnId: {}",
                    key
                ))
            })?;
            min.push(stat.min.clone());
            max.push(stat.max.clone());
        }

        if min.eq(&max) {
            const_blocks.push(idx);
            continue;
        }

        match points_map.get_mut(&min) {
            None => {
                points_map.insert(min, (vec![idx], vec![]));
            }
            Some((v, _)) => {
                v.push(idx);
            }
        };

        match points_map.get_mut(&max) {
            None => {
                points_map.insert(max, (vec![], vec![idx]));
            }
            Some((_, v)) => {
                v.push(idx);
            }
        };
    }
    let fields = keys
        .iter()
        .map(|key| schema.field(*key as usize).clone())
        .collect::<Vec<_>>();
    Ok(Points {
        blocks,
        points_map,
        const_blocks,
        fields,
        //statis: Vec::new(),
    })
}

fn sort_points(points: Points) -> Result<Vec<Vec<DataValue>>> {
    let mut values = Vec::with_capacity(points.fields.len());
    for _ in 0..points.fields.len() {
        values.push(Vec::<DataValue>::with_capacity(points.points_map.len()));
    }

    for value in points.points_map.keys() {
        for (i, v) in value.iter().enumerate() {
            values[i].push(v.clone());
        }
    }

    let order_columns = values
        .iter()
        .zip(points.fields.iter())
        .map(|(value, field)| Ok(field.data_type().create_column(value)?.as_arrow_array()))
        .collect::<Result<Vec<_>>>()?;

    let order_arrays = order_columns
        .iter()
        .map(|array| arrow_sort::SortColumn {
            values: array.as_ref(),
            options: Some(arrow_sort::SortOptions {
                descending: false,
                nulls_first: false,
            }),
        })
        .collect::<Vec<_>>();

    let indices = arrow_sort::lexsort_to_indices::<u32>(&order_arrays, None)?;

    let mut results = Vec::with_capacity(points.points_map.len());

    for indice in indices.values().iter() {
        let result = values
            .iter()
            .map(|v| v.get(*indice as usize).unwrap().clone())
            .collect::<Vec<_>>();
        results.push(result);
    }

    Ok(results)
}

#[derive(Clone)]
pub struct Statis {
    overlap: usize,
    depth: usize,
}

fn get_statis(keys: Vec<Vec<DataValue>>, points: Points) -> Result<Vec<Statis>> {
    let mut statis: Vec<Statis> = Vec::new();
    let mut unfinished_parts: HashMap<usize, Statis> = HashMap::new();
    for key in keys {
        let (start, end) = points.points_map.get(&key).ok_or_else(|| {
            ErrorCode::UnknownException(format!("Unable to get the points by key: {:?}", key))
        })?;

        let point_depth = unfinished_parts.len() + start.len();

        for (_, val) in unfinished_parts.iter_mut() {
            val.overlap += start.len();
            val.depth = cmp::max(val.depth, point_depth);
        }

        start.iter().for_each(|&idx| {
            unfinished_parts.insert(idx, Statis {
                overlap: point_depth,
                depth: point_depth,
            });
        });

        end.iter().for_each(|&idx| {
            let stat = unfinished_parts.remove(&idx).unwrap();
            statis.push(stat);
        });
    }
    assert_eq!(unfinished_parts.len(), 0);
    Ok(statis)
}
