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

use std::collections::BTreeMap;
use std::collections::HashMap;

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::NumberType;
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
        &[column.clone().into()],
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
    col_stats_truncate_lens: &BTreeMap<ColumnId, usize>,
    mut column_min_max: HashMap<ColumnId, (Option<Scalar>, Option<Scalar>)>,
) -> Result<StatisticsOfColumns> {
    let mut statistics = StatisticsOfColumns::new();
    let rows = data_block.num_rows();

    let leaves = traverse_values_dfs(data_block.columns(), schema.fields())?;
    for (column_id, col, data_type) in leaves {
        match col {
            Value::Scalar(s) => {
                let (distinct_of_values, unset_bits) =
                    if s == Scalar::Null { (0, rows) } else { (1, 0) };

                let Some((min, max)) = trim_column_min_max(
                    s.clone(),
                    s.clone(),
                    col_stats_truncate_lens.get(&column_id).copied(),
                ) else {
                    continue;
                };

                // when we read it back from parquet, it is a Column instead of Scalar
                let in_memory_size = s.as_ref().estimated_scalar_repeat_size(rows, &data_type);
                let col_stats = ColumnStatistics::new(
                    min,
                    max,
                    unset_bits as u64,
                    in_memory_size as u64,
                    Some(distinct_of_values),
                );

                statistics.insert(column_id, col_stats);
            }
            Value::Column(col) => {
                let (cached_min, cached_max) =
                    column_min_max.remove(&column_id).unwrap_or_default();
                let need_min = cached_min.is_none();
                let need_max = cached_max.is_none();
                let mut min = cached_min.unwrap_or(Scalar::Null);
                let mut max = cached_max.unwrap_or(Scalar::Null);

                if col.len() > 0 {
                    if need_min || need_max {
                        let entries = [col.clone().into()];
                        if need_min {
                            let (mins, _) = eval_aggr("min", vec![], &entries, rows, vec![])?;
                            let Some(value) = mins.index(0) else {
                                continue;
                            };
                            min = value.to_owned();
                        }
                        if need_max {
                            let (maxs, _) = eval_aggr("max", vec![], &entries, rows, vec![])?;
                            let Some(value) = maxs.index(0) else {
                                continue;
                            };
                            max = value.to_owned();
                        }
                    }

                    let Some((trimmed_min, trimmed_max)) = trim_column_min_max(
                        min,
                        max,
                        col_stats_truncate_lens.get(&column_id).copied(),
                    ) else {
                        continue;
                    };
                    min = trimmed_min;
                    max = trimmed_max;
                }

                let (is_all_null, bitmap) = col.validity();
                let unset_bits = match (is_all_null, bitmap) {
                    (_, Some(bitmap)) => bitmap.null_count(),
                    (true, None) => rows,
                    (false, None) => 0,
                };

                // use distinct count calculated by the xor hash function to avoid repetitive operation.
                let distinct_of_values = if let Some(&value) = column_distinct_count
                    .as_ref()
                    .and_then(|v| v.get(&column_id))
                {
                    value as u64
                } else {
                    calc_column_distinct_of_values(&col, rows)?
                };

                let in_memory_size = col.memory_size(false) as u64;
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
}

pub const END_OF_UNICODE_RANGE: char = '\u{10FFFF}';
pub const STATS_STRING_PREFIX_LEN: usize = 16;
const MAX_AUTO_STATS_STRING_PREFIX_LEN: usize = 32;
const MAX_AUTO_STATS_STRING_COMMON_PREFIX_LEN: usize = MAX_AUTO_STATS_STRING_PREFIX_LEN - 1;

fn auto_stats_string_prefix_len(min: &str, max: &str) -> usize {
    let common_prefix_len = min
        .chars()
        .zip(max.chars())
        .take(MAX_AUTO_STATS_STRING_COMMON_PREFIX_LEN)
        .take_while(|(min_char, max_char)| min_char == max_char)
        .count();

    if common_prefix_len < STATS_STRING_PREFIX_LEN {
        STATS_STRING_PREFIX_LEN
    } else {
        common_prefix_len
            .saturating_add(1)
            .min(MAX_AUTO_STATS_STRING_PREFIX_LEN)
    }
}

pub(crate) fn trim_column_min_max(
    min: Scalar,
    max: Scalar,
    string_truncate_len: Option<usize>,
) -> Option<(Scalar, Scalar)> {
    match (min, max) {
        (Scalar::String(min), Scalar::String(max)) => {
            let truncate_len =
                string_truncate_len.unwrap_or_else(|| auto_stats_string_prefix_len(&min, &max));
            Some((
                Scalar::String(trim_string_min_with_len(min, truncate_len)?),
                Scalar::String(trim_string_max_with_len(max, truncate_len)?),
            ))
        }
        (min, max) => Some((min.trim_min()?, max.trim_max()?)),
    }
}

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
                        if c < END_OF_UNICODE_RANGE {
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
                            r.push(END_OF_UNICODE_RANGE);
                        }
                    }

                    Some(Scalar::String(r))
                }
            }
            v => Some(v),
        }
    }
}

pub fn trim_string_min_with_len(s: String, len: usize) -> Option<String> {
    if s.len() <= len {
        return Some(s);
    }
    let vs = s.as_str();
    let slice = match vs.char_indices().nth(len) {
        None => vs,
        Some((idx, _)) => &vs[..idx],
    };
    let mut result = String::with_capacity(len);
    result.push_str(slice);
    Some(result)
}

pub fn trim_string_max_with_len(s: String, len: usize) -> Option<String> {
    if s.len() <= len {
        return Some(s);
    }
    let number_of_chars = s.as_str().chars().count();
    if number_of_chars <= len {
        return Some(s);
    }
    let vs = s.as_str();
    let sliced = match vs.char_indices().nth(len) {
        None => vs,
        Some((idx, _)) => &vs[..idx],
    };
    let mut idx = None;
    for (i, c) in sliced.char_indices().rev() {
        if c < END_OF_UNICODE_RANGE {
            idx = Some(i);
            break;
        }
    }
    let replacement_point = idx?;
    let mut r = String::with_capacity(len);
    for (i, c) in sliced.char_indices() {
        if i < replacement_point {
            r.push(c)
        } else {
            r.push(END_OF_UNICODE_RANGE);
        }
    }
    Some(r)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::sync::Arc;

    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::StringType;

    use super::MAX_AUTO_STATS_STRING_COMMON_PREFIX_LEN;
    use super::MAX_AUTO_STATS_STRING_PREFIX_LEN;
    use super::auto_stats_string_prefix_len;
    use super::gen_columns_statistics;
    use crate::statistics::END_OF_UNICODE_RANGE;
    use crate::statistics::STATS_STRING_PREFIX_LEN;
    use crate::statistics::Trim;

    #[test]
    fn test_auto_stats_string_prefix_len() {
        assert_eq!(
            auto_stats_string_prefix_len("abcdefghijklmnoa", "abcdefghijklmnoz"),
            STATS_STRING_PREFIX_LEN
        );

        let prefix = "abcdefghijklmnop";
        assert_eq!(
            auto_stats_string_prefix_len(
                &format!("{prefix}a-min-suffix"),
                &format!("{prefix}z-max-suffix"),
            ),
            STATS_STRING_PREFIX_LEN + 1
        );

        let long_prefix = "a".repeat(MAX_AUTO_STATS_STRING_COMMON_PREFIX_LEN);
        assert_eq!(
            auto_stats_string_prefix_len(
                &format!("{long_prefix}x-min"),
                &format!("{long_prefix}y-max"),
            ),
            MAX_AUTO_STATS_STRING_PREFIX_LEN
        );

        let capped_prefix = "好".repeat(MAX_AUTO_STATS_STRING_PREFIX_LEN);
        assert_eq!(
            auto_stats_string_prefix_len(
                &format!("{capped_prefix}甲"),
                &format!("{capped_prefix}乙"),
            ),
            MAX_AUTO_STATS_STRING_PREFIX_LEN
        );
    }

    #[test]
    fn test_cached_min_max_uses_adaptive_string_prefix() {
        let prefix = "abcdefghijklmnop";
        let min = [prefix, "a-min-suffix"].concat();
        let max = [prefix, "z-max-suffix"].concat();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "a",
            TableDataType::String,
        )]));
        let block = DataBlock::new_from_columns(vec![StringType::from_data(vec![
            min.as_str(),
            max.as_str(),
        ])]);
        let cached = HashMap::from([(0, (Some(Scalar::String(min)), Some(Scalar::String(max))))]);

        let stats =
            gen_columns_statistics(&block, None, &schema, &BTreeMap::new(), cached).unwrap();

        assert_eq!(stats[&0].min(), &Scalar::String([prefix, "a"].concat()));
        assert_eq!(
            stats[&0].max(),
            &Scalar::String([prefix, &END_OF_UNICODE_RANGE.to_string()].concat())
        );
    }

    #[test]
    fn test_trim_max() {
        {
            let invalid = END_OF_UNICODE_RANGE
                .to_string()
                .repeat(STATS_STRING_PREFIX_LEN + 1);
            assert_eq!(Scalar::String(invalid).trim_max(), None);
        }

        {
            let s = END_OF_UNICODE_RANGE
                .to_string()
                .repeat(STATS_STRING_PREFIX_LEN);
            let scalar = Scalar::String(s.clone());
            assert_eq!(scalar.clone().trim_max(), Some(scalar));
        }

        {
            let s = '👍'.to_string().repeat(STATS_STRING_PREFIX_LEN + 1);
            let res = Scalar::String(s.clone()).trim_max();
            let mut exp = '👍'.to_string().repeat(STATS_STRING_PREFIX_LEN - 1);
            exp.push(END_OF_UNICODE_RANGE);
            assert_eq!(res, Some(Scalar::String(exp)));
        }

        {
            let mut s = '👍'.to_string().repeat(STATS_STRING_PREFIX_LEN - 1);
            s.push(END_OF_UNICODE_RANGE);
            s.push(END_OF_UNICODE_RANGE);

            let res = Scalar::String(s.clone()).trim_max();

            let mut exp = '👍'.to_string().repeat(STATS_STRING_PREFIX_LEN - 2);
            exp.push(END_OF_UNICODE_RANGE);
            exp.push(END_OF_UNICODE_RANGE);

            assert_eq!(res, Some(Scalar::String(exp)));
        }
    }
}
