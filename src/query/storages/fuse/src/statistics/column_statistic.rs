//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::ColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataValue;
use common_exception::Result;
use common_functions::aggregates::eval_aggr;
use common_storages_index::MinMaxIndex;
use common_storages_index::SupportedType;
use common_storages_table_meta::meta::ColumnStatistics;
use common_storages_table_meta::meta::StatisticsOfColumns;

pub fn calc_column_distinct_of_values(
    column: &Arc<dyn Column>,
    column_field: ColumnWithField,
) -> Result<u64> {
    let distinct_values = eval_aggr(
        "approx_count_distinct",
        vec![],
        &[column_field],
        column.len(),
    )?;
    distinct_values.get(0).as_u64()
}

pub fn get_traverse_columns_dfs(data_block: &DataBlock) -> Result<Vec<(Option<usize>, ColumnRef)>> {
    traverse::traverse_columns_dfs(data_block.columns())
}

pub fn gen_columns_statistics(
    data_block: &DataBlock,
    column_distinct_count: Option<HashMap<usize, usize>>,
) -> Result<StatisticsOfColumns> {
    let mut statistics = StatisticsOfColumns::new();

    let leaves = traverse::traverse_columns_dfs(data_block.columns())?;

    for (idx, (col_idx, col)) in leaves.iter().enumerate() {
        let col_data_type = col.data_type();
        if !MinMaxIndex::is_supported_type(&col_data_type) {
            continue;
        }

        // later, during the evaluation of expressions, name of field does not matter
        let data_field = DataField::new("", col_data_type);
        let column_field = ColumnWithField::new(col.clone(), data_field);
        let mut min = DataValue::Null;
        let mut max = DataValue::Null;

        let rows = col.len();

        let mins = eval_aggr("min", vec![], &[column_field.clone()], rows)?;

        if mins.len() > 0 {
            min = if let Some(v) = mins.get(0).trim_min() {
                v
            } else {
                continue;
            }
        }

        let maxs = eval_aggr("max", vec![], &[column_field.clone()], rows)?;
        if maxs.len() > 0 {
            max = if let Some(v) = maxs.get(0).trim_max() {
                v
            } else {
                continue;
            }
        }

        // use distinct count calculated by the xor hash function to avoid repetitive operation.
        let distinct_of_values = match (col_idx, &column_distinct_count) {
            (Some(col_idx), Some(ref column_distinct_count)) => {
                if let Some(value) = column_distinct_count.get(col_idx) {
                    *value as u64
                } else {
                    calc_column_distinct_of_values(col, column_field)?
                }
            }
            (_, _) => calc_column_distinct_of_values(col, column_field)?,
        };

        let (is_all_null, bitmap) = col.validity();
        let unset_bits = match (is_all_null, bitmap) {
            (true, _) => rows,
            (false, Some(bitmap)) => bitmap.unset_bits(),
            (false, None) => 0,
        };

        let in_memory_size = col.memory_size() as u64;
        let col_stats = ColumnStatistics {
            min,
            max,
            null_count: unset_bits as u64,
            in_memory_size,
            distinct_of_values: Some(distinct_of_values),
        };

        statistics.insert(idx as u32, col_stats);
    }
    Ok(statistics)
}

pub mod traverse {
    use common_datavalues::ColumnRef;
    use common_datavalues::DataTypeImpl;
    use common_datavalues::Series;
    use common_datavalues::StructColumn;

    use super::*;

    // traverses columns and collects the leaves in depth first manner
    pub fn traverse_columns_dfs(columns: &[ColumnRef]) -> Result<Vec<(Option<usize>, ColumnRef)>> {
        let mut leaves = vec![];
        for (idx, col) in columns.iter().enumerate() {
            traverse_recursive(Some(idx), col, &mut leaves)?;
        }
        Ok(leaves)
    }

    fn traverse_recursive(
        idx: Option<usize>,
        column: &ColumnRef,
        leaves: &mut Vec<(Option<usize>, ColumnRef)>,
    ) -> Result<()> {
        match column.data_type() {
            DataTypeImpl::Struct(_) => {
                let full_column = column.convert_full_column();
                let struct_col: &StructColumn = Series::check_get(&full_column)?;
                for col in struct_col.values() {
                    traverse_recursive(None, col, leaves)?
                }
            }
            _ => {
                leaves.push((idx, column.clone()));
            }
        }
        Ok(())
    }
}

// Impls of this trait should preserves the property of min/max statistics:
//
// the trimmed max should be larger than the non-trimmed one (if possible).
// and the trimmed min should be lesser than the non-trimmed one (if possible).
pub trait Trim: Sized {
    fn trim_min(self) -> Option<Self>;
    fn trim_max(self) -> Option<Self>;
}

pub const STATS_REPLACEMENT_CHAR: char = '\u{FFFD}';
pub const STATS_STRING_PREFIX_LEN: usize = 16;

impl Trim for DataValue {
    fn trim_min(self) -> Option<Self> {
        match self {
            DataValue::String(bytes) => match String::from_utf8(bytes) {
                Ok(mut v) => {
                    if v.len() <= STATS_STRING_PREFIX_LEN {
                        Some(DataValue::String(v.into_bytes()))
                    } else {
                        // find the character boundary to prevent String::truncate from panic
                        let vs = v.as_str();
                        let slice = match vs.char_indices().nth(STATS_STRING_PREFIX_LEN) {
                            None => vs,
                            Some((idx, _)) => &vs[..idx],
                        };

                        // do truncate
                        Some(DataValue::String({
                            v.truncate(slice.len());
                            v.into_bytes()
                        }))
                    }
                }
                Err(_) => {
                    // if failed to convert the bytes into (utf-8)string, just ignore it.
                    None
                }
            },
            v => Some(v),
        }
    }

    fn trim_max(self) -> Option<Self> {
        match self {
            DataValue::String(bytes) => match String::from_utf8(bytes) {
                Ok(v) => {
                    if v.len() <= STATS_STRING_PREFIX_LEN {
                        // if number of bytes is lesser, just return
                        Some(DataValue::String(v.into_bytes()))
                    } else {
                        // no need to trim, less than STRING_RREFIX_LEN chars
                        let number_of_chars = v.as_str().chars().count();
                        if number_of_chars <= STATS_STRING_PREFIX_LEN {
                            return Some(DataValue::String(v.into_bytes()));
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

                        Some(DataValue::String(r.into_bytes()))
                    }
                }
                Err(_) => {
                    // if failed to convert the bytes into (utf-8)string, just ignore it.
                    None
                }
            },
            v => Some(v),
        }
    }
}
