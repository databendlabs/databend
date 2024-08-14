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

use std::collections::HashMap;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ValueType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COLUMN_ID;
use databend_common_functions::aggregates::eval_aggr;
use databend_storages_common_index::Index;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

// Don't change this value
// 0.04f--> 10 buckets
const DISTINCT_ERROR_RATE: f64 = 0.04;

pub fn calc_column_distinct_of_values(column: &Column, rows: usize) -> Result<u64> {
    let distinct_values = eval_aggr(
        "approx_count_distinct",
        vec![Scalar::Number(NumberScalar::Float64(
            DISTINCT_ERROR_RATE.into(),
        ))],
        &[column.clone()],
        rows,
    )?;
    let col = NumberType::<u64>::try_downcast_column(&distinct_values.0).unwrap();
    Ok(col[0])
}

pub fn get_traverse_columns_dfs(data_block: &DataBlock) -> traverse::TraverseResult {
    traverse::traverse_values_dfs(data_block.columns())
}

pub fn gen_columns_statistics(
    data_block: &DataBlock,
    column_distinct_count: Option<HashMap<FieldIndex, usize>>,
    schema: &TableSchemaRef,
) -> Result<StatisticsOfColumns> {
    let mut statistics = StatisticsOfColumns::new();
    let rows = data_block.num_rows();

    let leaves = get_traverse_columns_dfs(data_block)?;
    let leaf_column_ids = schema.to_leaf_column_ids();
    for ((col_idx, col, data_type), column_id) in leaves.iter().zip(leaf_column_ids) {
        // Ignore the range index does not supported type.
        if !RangeIndex::supported_type(data_type) {
            continue;
        }

        // Ignore the origin block row number column.
        if column_id == ORIGIN_BLOCK_ROW_NUM_COLUMN_ID {
            continue;
        }

        match col {
            Value::Scalar(s) => {
                let (distinct_of_values, unset_bits) = if *s == Scalar::Null {
                    (0, rows)
                } else {
                    (1, 0)
                };

                // when we read it back from parquet, it is a Column instead of Scalar
                // only need consider RangeIndex::supported_type
                let in_memory_size = s.as_ref().memory_size();
                let mut in_memory_size = if matches!(data_type.remove_nullable(), DataType::String)
                {
                    in_memory_size * rows + 8 * (rows + 1)
                } else {
                    in_memory_size * rows
                };
                if data_type.is_nullable() {
                    in_memory_size += rows.saturating_add(7) / 8
                }

                let col_stats = ColumnStatistics::new(
                    s.clone(),
                    s.clone(),
                    unset_bits as u64,
                    in_memory_size as u64,
                    Some(distinct_of_values),
                );

                statistics.insert(column_id, col_stats);
            }
            Value::Column(col) => {
                // later, during the evaluation of expressions, name of field does not matter
                let mut min = Scalar::Null;
                let mut max = Scalar::Null;

                let (mins, _) = eval_aggr("min", vec![], &[col.clone()], rows)?;
                let (maxs, _) = eval_aggr("max", vec![], &[col.clone()], rows)?;

                if mins.len() > 0 {
                    min = if let Some(v) = mins.index(0) {
                        if let Some(v) = v.to_owned().trim_min() {
                            v
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }

                if maxs.len() > 0 {
                    max = if let Some(v) = maxs.index(0) {
                        if let Some(v) = v.to_owned().trim_max() {
                            v
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }

                let (is_all_null, bitmap) = col.validity();
                let unset_bits = match (is_all_null, bitmap) {
                    (true, _) => rows,
                    (false, Some(bitmap)) => bitmap.unset_bits(),
                    (false, None) => 0,
                };

                // use distinct count calculated by the xor hash function to avoid repetitive operation.
                let distinct_of_values = match (col_idx, &column_distinct_count) {
                    (Some(col_idx), Some(ref column_distinct_count)) => {
                        if let Some(value) = column_distinct_count.get(col_idx) {
                            // value calculated by xor hash function include NULL, need to subtract one.
                            if unset_bits > 0 {
                                *value as u64 - 1
                            } else {
                                *value as u64
                            }
                        } else {
                            calc_column_distinct_of_values(col, rows)?
                        }
                    }
                    (_, _) => calc_column_distinct_of_values(col, rows)?,
                };

                let in_memory_size = col.memory_size() as u64;
                let col_stats = ColumnStatistics::new(
                    min,
                    max,
                    unset_bits as u64,
                    in_memory_size,
                    Some(distinct_of_values),
                );

                statistics.insert(column_id, col_stats);
            }
        }
    }
    Ok(statistics)
}

pub fn scalar_min_max(data_type: &DataType, scalar: Scalar) -> Option<(Scalar, Scalar)> {
    if RangeIndex::supported_type(data_type) {
        if let Some((min, Some(max))) = scalar
            .clone()
            .trim_min()
            .map(|min| (min, scalar.trim_max()))
        {
            return Some((min, max));
        }
    }
    None
}

pub mod traverse {
    use databend_common_expression::types::map::KvPair;
    use databend_common_expression::types::AnyType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::BlockEntry;
    use databend_common_expression::Column;
    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::Value;

    use super::*;

    pub type TraverseResult = Result<Vec<(Option<usize>, Value<AnyType>, DataType)>>;

    // traverses columns and collects the leaves in depth first manner
    pub fn traverse_values_dfs(columns: &[BlockEntry]) -> TraverseResult {
        let mut leaves = vec![];
        for (idx, entry) in columns.iter().enumerate() {
            let data_type = &entry.data_type;
            match &entry.value {
                Value::Scalar(s) => {
                    traverse_scalar_recursive(Some(idx), s, data_type, &mut leaves)?;
                }
                Value::Column(c) => {
                    traverse_column_recursive(Some(idx), c, data_type, &mut leaves)?;
                }
            }
        }
        Ok(leaves)
    }

    fn traverse_column_recursive(
        idx: Option<usize>,
        column: &Column,
        data_type: &DataType,
        leaves: &mut Vec<(Option<usize>, Value<AnyType>, DataType)>,
    ) -> Result<()> {
        match data_type.remove_nullable() {
            DataType::Tuple(inner_types) => {
                let inner_columns = if data_type.is_nullable() {
                    let nullable_column = column.as_nullable().unwrap();
                    nullable_column.column.as_tuple().unwrap()
                } else {
                    column.as_tuple().unwrap()
                };
                for (inner_column, inner_type) in inner_columns.iter().zip(inner_types.iter()) {
                    crate::statistics::traverse::traverse_column_recursive(
                        None,
                        inner_column,
                        inner_type,
                        leaves,
                    )?;
                }
            }
            DataType::Array(inner_type) => {
                let array_column = if data_type.is_nullable() {
                    let nullable_column = column.as_nullable().unwrap();
                    nullable_column.column.as_array().unwrap()
                } else {
                    column.as_array().unwrap()
                };
                crate::statistics::traverse::traverse_column_recursive(
                    None,
                    &array_column.values,
                    &inner_type,
                    leaves,
                )?;
            }
            DataType::Map(inner_type) => match *inner_type {
                DataType::Tuple(inner_types) => {
                    let map_column = if data_type.is_nullable() {
                        let nullable_column = column.as_nullable().unwrap();
                        nullable_column.column.as_map().unwrap()
                    } else {
                        column.as_map().unwrap()
                    };
                    let kv_column =
                        KvPair::<AnyType, AnyType>::try_downcast_column(&map_column.values)
                            .unwrap();
                    crate::statistics::traverse::traverse_column_recursive(
                        None,
                        &kv_column.keys,
                        &inner_types[0],
                        leaves,
                    )?;
                    crate::statistics::traverse::traverse_column_recursive(
                        None,
                        &kv_column.values,
                        &inner_types[1],
                        leaves,
                    )?;
                }
                _ => unreachable!(),
            },
            _ => {
                leaves.push((idx, Value::Column(column.clone()), data_type.clone()));
            }
        }
        Ok(())
    }

    /// Traverse the columns in DFS order, convert them to a flatten columns array sorted by leaf_index.
    /// We must ensure that each leaf node is traversed, otherwise we may get an incorrect leaf_index.
    ///
    /// For the `Array, `Map` and `Tuple` types, we should expand its inner columns.
    fn traverse_scalar_recursive(
        idx: Option<usize>,
        scalar: &Scalar,
        data_type: &DataType,
        leaves: &mut Vec<(Option<usize>, Value<AnyType>, DataType)>,
    ) -> Result<()> {
        match data_type.remove_nullable() {
            DataType::Tuple(inner_types) => {
                let inner_scalars = if data_type.is_nullable() && *scalar == Scalar::Null {
                    inner_types.iter().map(Scalar::default_value).collect()
                } else {
                    scalar.as_tuple().unwrap().clone()
                };
                for (inner_scalar, inner_type) in inner_scalars.iter().zip(inner_types.iter()) {
                    traverse_scalar_recursive(None, inner_scalar, inner_type, leaves)?;
                }
            }
            DataType::Array(inner_type) => {
                let array_column = if data_type.is_nullable() && *scalar == Scalar::Null {
                    ColumnBuilder::with_capacity(&inner_type, 0).build()
                } else {
                    scalar.as_array().unwrap().clone()
                };
                traverse_column_recursive(None, &array_column, &inner_type, leaves)?;
            }
            DataType::Map(inner_type) => match *inner_type {
                DataType::Tuple(ref inner_types) => {
                    let map_column = if data_type.is_nullable() && *scalar == Scalar::Null {
                        Column::Tuple(
                            inner_types
                                .iter()
                                .map(|t| ColumnBuilder::with_capacity(t, 0).build())
                                .collect(),
                        )
                    } else {
                        scalar.as_map().unwrap().clone()
                    };
                    traverse_column_recursive(None, &map_column, &inner_type, leaves)?;
                }
                _ => unreachable!(),
            },
            _ => {
                leaves.push((idx, Value::Scalar(scalar.clone()), data_type.clone()));
            }
        }
        Ok(())
    }
}

// Impls of this trait should preserve the property of min/max statistics:
//
// the trimmed max should be larger than the non-trimmed one (if possible).
// and the trimmed min should be lesser than the non-trimmed one (if possible).
pub trait Trim: Sized {
    fn trim_min(self) -> Option<Self>;
    fn trim_max(self) -> Option<Self>;
    fn may_be_trimmed(&self) -> bool;
}

pub const STATS_REPLACEMENT_CHAR: char = '\u{FFFD}';
pub const STATS_STRING_PREFIX_LEN: usize = 16;

impl Trim for Scalar {
    fn trim_min(self) -> Option<Self> {
        match self {
            Scalar::String(mut s) => {
                if s.len() <= STATS_STRING_PREFIX_LEN {
                    Some(Scalar::String(s))
                } else {
                    // find the character boundary to prevent String::truncate from panic
                    let vs = s.as_str();
                    let slice = match vs.char_indices().nth(STATS_STRING_PREFIX_LEN) {
                        None => vs,
                        Some((idx, _)) => &vs[..idx],
                    };

                    // do truncate
                    Some(Scalar::String({
                        s.truncate(slice.len());
                        s
                    }))
                }
            }
            v => Some(v),
        }
    }

    fn trim_max(self) -> Option<Self> {
        match self {
            Scalar::String(v) => {
                if v.len() <= STATS_STRING_PREFIX_LEN {
                    // if number of bytes is lesser, just return
                    Some(Scalar::String(v))
                } else {
                    // no need to trim, less than STRING_PREFIX_LEN chars
                    let number_of_chars = v.as_str().chars().count();
                    if number_of_chars <= STATS_STRING_PREFIX_LEN {
                        return Some(Scalar::String(v));
                    }

                    // slice the input (at the boundary of chars), takes at most STRING_PREFIX_LEN chars
                    let vs = v.as_str();
                    let sliced = match vs.char_indices().nth(STATS_STRING_PREFIX_LEN) {
                        None => vs,
                        Some((idx, _)) => &vs[..idx],
                    };

                    // find the position to replace the char with REPLACEMENT_CHAR
                    // in reversed order, break at the first one we met
                    let mut idx = None;
                    for (i, c) in sliced.char_indices().rev() {
                        if c < STATS_REPLACEMENT_CHAR {
                            idx = Some(i);
                            break;
                        }
                    }

                    // grab the replacement_point
                    let replacement_point = idx?;

                    // rebuild the string (since the len of result string is rather small)
                    let mut r = String::with_capacity(STATS_STRING_PREFIX_LEN);
                    for (i, c) in sliced.char_indices() {
                        if i < replacement_point {
                            r.push(c)
                        } else {
                            r.push(STATS_REPLACEMENT_CHAR);
                        }
                    }

                    Some(Scalar::String(r))
                }
            }
            v => Some(v),
        }
    }

    fn may_be_trimmed(&self) -> bool {
        match self {
            Scalar::String(s) => s.len() >= STATS_STRING_PREFIX_LEN,
            _ => false,
        }
    }
}
