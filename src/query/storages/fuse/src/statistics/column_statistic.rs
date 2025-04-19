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
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_functions::aggregates::eval_aggr;
use databend_storages_common_index::Index;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

use crate::statistics::traverse_values_dfs;

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
        vec![],
    )?;
    let col = NumberType::<u64>::try_downcast_column(&distinct_values.0).unwrap();
    Ok(col[0])
}

pub fn gen_columns_statistics(
    data_block: &DataBlock,
    column_distinct_count: Option<HashMap<ColumnId, usize>>,
    schema: &TableSchemaRef,
) -> Result<StatisticsOfColumns> {
    let mut statistics = StatisticsOfColumns::new();
    let rows = data_block.num_rows();

    let leaves = traverse_values_dfs(data_block.columns(), schema.fields())?;
    for (column_id, col, data_type) in leaves {
        match col {
            Value::Scalar(s) => {
                let (distinct_of_values, unset_bits) =
                    if s == Scalar::Null { (0, rows) } else { (1, 0) };

                // when we read it back from parquet, it is a Column instead of Scalar
                let in_memory_size = s.as_ref().estimated_scalar_repeat_size(rows, &data_type);
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

                let (mins, _) = eval_aggr("min", vec![], &[col.clone()], rows, vec![])?;
                let (maxs, _) = eval_aggr("max", vec![], &[col.clone()], rows, vec![])?;

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
                    (false, Some(bitmap)) => bitmap.null_count(),
                    (false, None) => 0,
                };

                // use distinct count calculated by the xor hash function to avoid repetitive operation.
                let distinct_of_values = if let Some(value) = column_distinct_count
                    .as_ref()
                    .and_then(|v| v.get(&column_id))
                {
                    // value calculated by xor hash function include NULL, need to subtract one.
                    if unset_bits > 0 {
                        *value as u64 - 1
                    } else {
                        *value as u64
                    }
                } else {
                    calc_column_distinct_of_values(&col, rows)?
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
