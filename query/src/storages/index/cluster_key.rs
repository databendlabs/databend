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
use std::collections::HashSet;

use common_arrow::arrow::compute::sort as arrow_sort;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use common_planners::ExpressionMonotonicityVisitor;
use common_planners::Expressions;
use common_planners::RequireColumnsVisitor;

use crate::storages::fuse::meta::BlockMeta;
use crate::storages::index::range_filter::check_maybe_monotonic;

#[derive(Clone)]
pub struct Points {
    blocks: Vec<BlockMeta>,
    // (start, end).
    points_map: HashMap<Vec<DataValue>, (Vec<usize>, Vec<usize>)>,
    const_blocks: Vec<usize>,
    fields: Vec<DataField>,
    //statis: Vec<Statis>,
}

fn gather_parts(
    schema: DataSchemaRef,
    blocks: Vec<BlockMeta>,
    args: Expressions,
) -> Result<Points> {
    // The cluster key value need store in the block meta.
    let mut keys = Vec::new();
    let mut is_columns = Vec::new();
    for arg in &args {
        if !check_maybe_monotonic(arg)? {
            return Err(ErrorCode::UnknownException(
                "Only support the monotonic expression",
            ));
        }
        let cols = RequireColumnsVisitor::collect_columns_from_expr(arg)?;
        let key = cols
            .iter()
            .map(|v| schema.column_with_name(v).unwrap())
            .collect::<Vec<_>>();
        keys.push(key);
        is_columns.push(matches!(arg, Expression::Column(_)));
    }

    let mut points_map: HashMap<Vec<DataValue>, (Vec<usize>, Vec<usize>)> = HashMap::new();
    let mut const_blocks = Vec::new();
    for (b_i, block) in blocks.iter().enumerate() {
        let mut is_positive = true;
        let mut min_vec = Vec::with_capacity(args.len());
        let mut max_vec = Vec::with_capacity(args.len());
        for (k_i, key) in keys.clone().into_iter().enumerate() {
            if is_columns[k_i] {
                let stat = block.col_stats.get(&(key[0].0 as u32)).ok_or_else(|| {
                    ErrorCode::UnknownException(format!(
                        "Unable to get the colStats by ColumnId: {}",
                        key[0].0
                    ))
                })?;
                min_vec.push(stat.min.clone());
                max_vec.push(stat.max.clone());
                continue;
            }

            let mut variables = HashMap::with_capacity(keys.len());
            for (f_i, field) in key {
                let stat = block.col_stats.get(&(f_i as u32)).ok_or_else(|| {
                    ErrorCode::UnknownException(format!(
                        "Unable to get the colStats by ColumnId: {}",
                        f_i
                    ))
                })?;

                let min_col = field.data_type().create_constant_column(&stat.min, 1)?;
                let variable_left = Some(ColumnWithField::new(min_col, field.clone()));

                let max_col = field.data_type().create_constant_column(&stat.max, 1)?;
                let variable_right = Some(ColumnWithField::new(max_col, field.clone()));
                variables.insert(field.name().clone(), (variable_left, variable_right));
            }

            let monotonicity = ExpressionMonotonicityVisitor::check_expression(
                schema.clone(),
                &args[k_i],
                variables,
                false,
            );
            if !monotonicity.is_monotonic {
                return Err(ErrorCode::UnknownException(
                    "Only support the monotonic expression",
                ));
            }

            if k_i == 0 {
                is_positive = monotonicity.is_positive;
            } else if is_positive != monotonicity.is_positive {
                return Err(ErrorCode::UnknownException(
                    "Only support the same monotonic expressions",
                ));
            }

            let (min, max) = if is_positive {
                (
                    monotonicity.left.unwrap().column().get(0),
                    monotonicity.right.unwrap().column().get(0),
                )
            } else {
                (
                    monotonicity.right.unwrap().column().get(0),
                    monotonicity.left.unwrap().column().get(0),
                )
            };

            min_vec.push(min);
            max_vec.push(max);
        }

        if min_vec.eq(&max_vec) {
            const_blocks.push(b_i);
            continue;
        }

        match points_map.get_mut(&min_vec) {
            None => {
                points_map.insert(min_vec, (vec![b_i], vec![]));
            }
            Some((v, _)) => {
                v.push(b_i);
            }
        };

        match points_map.get_mut(&max_vec) {
            None => {
                points_map.insert(max_vec, (vec![], vec![b_i]));
            }
            Some((_, v)) => {
                v.push(b_i);
            }
        };
    }

    let fields = args
        .iter()
        .map(|arg| arg.to_data_field(&schema))
        .collect::<Result<Vec<_>>>()?;

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
