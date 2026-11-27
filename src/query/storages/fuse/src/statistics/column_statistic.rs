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

                let (min, max) = trim_column_min_max(
                    s.clone(),
                    s.clone(),
                    col_stats_truncate_lens.get(&column_id).copied(),
                );

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

                    (min, max) = trim_column_min_max(
                        min,
                        max,
                        col_stats_truncate_lens.get(&column_id).copied(),
                    );
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
    RangeIndex::supported_type(data_type).then(|| (scalar.clone().trim_min(), scalar.trim_max()))
}

// Trimming must preserve the bounds: trimmed min <= original min, and
// trimmed max >= original max. A max that has no trimmed upper bound becomes
// `Scalar::Null`, i.e. unknown.
pub trait Trim: Sized {
    fn trim_min(self) -> Self;
    fn trim_max(self) -> Self;
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

/// Trim the bounds of a column to what its statistics store.
///
/// String bounds are cut to a prefix. A prefix is a lower bound as it is, so
/// a column with values always keeps its lower bound. As an upper bound the
/// prefix has to be incremented, which a prefix of U+10FFFF only does not
/// allow: no string of that length bounds the value from above. The upper
/// bound is then null, i.e. unknown, and the rest of the statistics stay.
pub(crate) fn trim_column_min_max(
    min: Scalar,
    max: Scalar,
    string_truncate_len: Option<usize>,
) -> (Scalar, Scalar) {
    match (min, max) {
        (Scalar::String(min), Scalar::String(max)) => {
            let truncate_len =
                string_truncate_len.unwrap_or_else(|| auto_stats_string_prefix_len(&min, &max));
            (
                Scalar::String(trim_string_min_with_len(min, truncate_len)),
                trim_string_max_with_len(max, truncate_len).map_or(Scalar::Null, Scalar::String),
            )
        }
        (min, max) => (min.trim_min(), max.trim_max()),
    }
}

impl Trim for Scalar {
    fn trim_min(self) -> Self {
        match self {
            Scalar::String(s) => {
                Scalar::String(trim_string_min_with_len(s, STATS_STRING_PREFIX_LEN))
            }
            v => v,
        }
    }

    fn trim_max(self) -> Self {
        match self {
            Scalar::String(s) => trim_string_max_with_len(s, STATS_STRING_PREFIX_LEN)
                .map_or(Scalar::Null, Scalar::String),
            v => v,
        }
    }
}

pub fn trim_string_min_with_len(s: String, len: usize) -> String {
    if s.len() <= len {
        return s;
    }
    match s.char_indices().nth(len) {
        Some((idx, _)) => s[..idx].to_owned(),
        None => s,
    }
}

/// Returns an upper bound of `s` with at most `len` scalars, or `None` if
/// there is none.
///
/// A value with more than `len` scalars gets a bound of exactly `len` scalars:
/// its prefix with the last scalar that can be incremented advanced. If the
/// first `len` scalars of `s` are all U+10FFFF, nothing can be advanced and no
/// string of that length is an upper bound. The bound is then left unknown
/// rather than made as long as the value, so that the size of the statistics
/// stays bounded.
pub fn trim_string_max_with_len(s: String, len: usize) -> Option<String> {
    if s.len() <= len {
        return Some(s);
    }
    let Some((end, _)) = s.char_indices().nth(len) else {
        return Some(s);
    };
    let prefix = &s[..end];

    // Carry past trailing U+10FFFF scalars of the prefix.
    let (trailing_max_chars, (idx, c)) = prefix
        .char_indices()
        .rev()
        .enumerate()
        .find(|(_, (_, c))| *c < END_OF_UNICODE_RANGE)?;

    let mut result = String::with_capacity(end);
    result.push_str(&prefix[..idx]);
    result.push(next_scalar(c));
    // The incremented scalar makes the bound greater than the original value,
    // so pad with the smallest scalar for the tightest fixed-length bound.
    // Keep `len` scalars: readers infer may_be_truncated from the stored length
    // and must not mistake a shortened successor for an exact max.
    result.extend(std::iter::repeat_n('\0', trailing_max_chars));
    Some(result)
}

fn next_scalar(c: char) -> char {
    debug_assert!(c < END_OF_UNICODE_RANGE);
    // Surrogates (U+D800..=U+DFFF) are not valid Unicode scalars.
    match c {
        '\u{D7FF}' => '\u{E000}',
        _ => char::from_u32(c as u32 + 1).unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::sync::Arc;

    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::ColumnRef;
    use databend_common_expression::Constant;
    use databend_common_expression::DataBlock;
    use databend_common_expression::Expr;
    use databend_common_expression::FromData;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::type_check::check_function;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;
    use databend_common_functions::BUILTIN_FUNCTIONS;
    use databend_storages_common_index::RangeIndex;

    use super::MAX_AUTO_STATS_STRING_COMMON_PREFIX_LEN;
    use super::MAX_AUTO_STATS_STRING_PREFIX_LEN;
    use super::auto_stats_string_prefix_len;
    use super::gen_columns_statistics;
    use super::scalar_min_max;
    use super::trim_column_min_max;
    use super::trim_string_max_with_len;
    use super::trim_string_min_with_len;
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
        assert_eq!(stats[&0].max(), &Scalar::String([prefix, "{"].concat()));
    }

    #[test]
    fn test_trim_string_max_with_len() {
        for (input, len, expected) in [
            ("", 4, ""),
            ("abc", 4, "abc"),
            ("abcd", 4, "abcd"),
            ("中文", 4, "中文"),
            ("abcd-suffix", 4, "abce"),
            ("ab\u{10FFFF}\u{10FFFF}-suffix", 4, "ac\0\0"),
            ("a\u{10FFFF}\u{10FFFF}\u{10FFFF}-suffix", 4, "b\0\0\0"),
            ("abc\u{D7FF}-suffix", 4, "abc\u{E000}"),
            ("abc\u{10FFFE}-suffix", 4, "abc\u{10FFFF}"),
            ("\u{7F}-suffix", 1, "\u{80}"),
            ("\u{7FF}-suffix", 1, "\u{800}"),
            ("\u{FFFF}-suffix", 1, "\u{10000}"),
            ("\0-suffix", 1, "\u{1}"),
            ("a\0\0z-suffix", 4, "a\0\0{"),
            ("\u{10FFFF}", 1, "\u{10FFFF}"),
        ] {
            assert_eq!(
                trim_string_max_with_len(input.to_string(), len).as_deref(),
                Some(expected),
                "input={input:?}, len={len}"
            );
        }

        // A value longer than a prefix of U+10FFFF only has no upper bound of
        // the prefix's length, whatever follows the prefix.
        for (input, len) in [
            ("\u{10FFFF}-suffix", 1),
            ("\u{10FFFF}\u{10FFFF}-suffix", 1),
            ("\u{10FFFF}\u{10FFFF}z", 1),
            ("\u{10FFFF}\u{7F}", 1),
            ("\u{10FFFF}\u{10FFFF}", 1),
            ("\u{10FFFF}\u{10FFFF}\u{10FFFF}\u{10FFFF}z", 4),
        ] {
            assert_eq!(
                trim_string_max_with_len(input.to_string(), len),
                None,
                "input={input:?}, len={len}"
            );
        }
    }

    #[test]
    fn test_trim_string_max_bound_properties() {
        for len in [1, 4, STATS_STRING_PREFIX_LEN, 40, 4096] {
            for trailing_max_chars in 0..len.min(4) {
                let prefix = "a".repeat(len - trailing_max_chars - 1);
                for c in ['\0', 'a', '中', '👍', '\u{D7FF}', '\u{10FFFE}'] {
                    let input = format!(
                        "{prefix}{c}{}-suffix",
                        END_OF_UNICODE_RANGE.to_string().repeat(trailing_max_chars)
                    );
                    let min = trim_string_min_with_len(input.clone(), len);
                    let max = trim_string_max_with_len(input.clone(), len).unwrap();
                    let old_max = format!(
                        "{prefix}{}",
                        END_OF_UNICODE_RANGE
                            .to_string()
                            .repeat(trailing_max_chars + 1)
                    );

                    assert!(min <= input);
                    assert!(input < max);
                    assert!(max <= old_max);
                    assert_eq!(max.chars().count(), len);
                    // Preserve the length-based may_be_truncated check.
                    assert!(max.len() >= len);
                }
            }
        }
    }

    #[test]
    fn test_string_successor_improves_range_pruning() {
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "a",
            TableDataType::String,
        )]));
        for (input, threshold, old_bound) in [
            ("abcd-suffix", "abcf", "abc\u{10FFFF}"),
            (
                "ab\u{10FFFF}\u{10FFFF}-suffix",
                "ac0",
                "a\u{10FFFF}\u{10FFFF}\u{10FFFF}",
            ),
        ] {
            let block = DataBlock::new_from_columns(vec![StringType::from_data(vec![input])]);
            let mut stats = gen_columns_statistics(
                &block,
                None,
                &schema,
                &BTreeMap::from([(0, 4)]),
                HashMap::new(),
            )
            .unwrap();
            let expr = check_function(
                None,
                "gte",
                &[],
                &[
                    Expr::ColumnRef(ColumnRef {
                        span: None,
                        id: "a".to_string(),
                        data_type: DataType::String,
                        display_name: "a".to_string(),
                    }),
                    Expr::Constant(Constant {
                        span: None,
                        scalar: Scalar::String(threshold.to_string()),
                        data_type: DataType::String,
                    }),
                ],
                &BUILTIN_FUNCTIONS,
            )
            .unwrap();
            let index = RangeIndex::try_create(
                FunctionContext::default(),
                &expr,
                schema.clone(),
                Default::default(),
            )
            .unwrap();

            assert!(!index.apply(&stats, Some(1), None, None, |_| false).unwrap());
            stats.get_mut(&0).unwrap().max = Scalar::String(old_bound.to_string());
            assert!(index.apply(&stats, Some(1), None, None, |_| false).unwrap());
        }
    }

    #[test]
    fn test_trim_column_min_max() {
        let max_chars = |n| END_OF_UNICODE_RANGE.to_string().repeat(n);

        // The upper bound carries past trailing U+10FFFF.
        let prefix = format!("a{}", max_chars(STATS_STRING_PREFIX_LEN - 1));
        let value = Scalar::String(format!("{prefix}-suffix"));
        let len = Some(STATS_STRING_PREFIX_LEN);
        let (min, max) = trim_column_min_max(value.clone(), value, len);
        assert_eq!(min, Scalar::String(prefix));
        assert_eq!(
            max,
            Scalar::String(format!("b{}", "\0".repeat(STATS_STRING_PREFIX_LEN - 1)))
        );
        assert!(max.as_string().unwrap().len() >= STATS_STRING_PREFIX_LEN);

        // A prefix of U+10FFFF only cannot carry: the lower bound stays and
        // the upper bound is unknown.
        for kept in [1, 4, STATS_STRING_PREFIX_LEN] {
            let value = Scalar::String(format!("{}z", max_chars(kept)));
            let (min, max) =
                trim_column_min_max(Scalar::String("a".to_string()), value.clone(), Some(kept));
            assert_eq!(min, Scalar::String("a".to_string()));
            assert_eq!(max, Scalar::Null);

            let (min, max) = trim_column_min_max(value.clone(), value, Some(kept));
            assert_eq!(min, Scalar::String(max_chars(kept)));
            assert_eq!(max, Scalar::Null);
        }
        // Without an explicit length, the bounds of a column are cut where
        // they differ, 16 characters in at the earliest.
        let value = Scalar::String(format!("{}z", max_chars(STATS_STRING_PREFIX_LEN)));
        let (min, max) = trim_column_min_max(Scalar::String("a".to_string()), value, None);
        assert_eq!(min, Scalar::String("a".to_string()));
        assert_eq!(max, Scalar::Null);

        // Other bounds are stored as they are.
        let (min, max) = trim_column_min_max(Scalar::from(1_i32), Scalar::from(2_i32), Some(4));
        assert_eq!((min, max), (Scalar::from(1_i32), Scalar::from(2_i32)));
        let (min, max) = trim_column_min_max(Scalar::Null, Scalar::Null, Some(4));
        assert_eq!((min, max), (Scalar::Null, Scalar::Null));
    }

    #[test]
    fn test_scalar_min_max_with_carry() {
        let prefix = format!(
            "a{}",
            END_OF_UNICODE_RANGE
                .to_string()
                .repeat(STATS_STRING_PREFIX_LEN - 1)
        );
        let scalar = Scalar::String(format!("{prefix}-suffix"));
        let (min, max) = scalar_min_max(&DataType::String, scalar).unwrap();

        assert_eq!(min, Scalar::String(prefix));
        assert_eq!(
            max,
            Scalar::String(format!("b{}", "\0".repeat(STATS_STRING_PREFIX_LEN - 1)))
        );
        assert!(max.as_string().unwrap().len() >= STATS_STRING_PREFIX_LEN);
    }

    #[test]
    fn test_unknown_string_max_keeps_the_rest_of_the_statistics() {
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "a",
            TableDataType::String.wrap_nullable(),
        )]));
        let unbounded = format!("{}z", END_OF_UNICODE_RANGE.to_string().repeat(4));
        let block = DataBlock::new_from_columns(vec![StringType::from_opt_data(vec![
            Some("a"),
            None,
            Some(unbounded.as_str()),
        ])]);

        let stats = gen_columns_statistics(
            &block,
            None,
            &schema,
            &BTreeMap::from([(0, 4)]),
            HashMap::new(),
        )
        .unwrap();

        let stats = &stats[&0];
        assert_eq!(stats.min(), &Scalar::String("a".to_string()));
        assert_eq!(stats.max(), &Scalar::Null);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.distinct_of_values, Some(2));
        assert!(!stats.is_all_null(3));
    }

    #[test]
    fn test_trim_max() {
        {
            // No string of the prefix's length is an upper bound, whether or
            // not a later scalar could be incremented: the bound is unknown.
            let prefix = END_OF_UNICODE_RANGE
                .to_string()
                .repeat(STATS_STRING_PREFIX_LEN);
            for suffix in [
                "\u{10FFFF}",
                "z",
                "z-suffix",
                "\u{10FFFF}\u{10FFFF}z-suffix",
            ] {
                let res = Scalar::String(format!("{prefix}{suffix}")).trim_max();
                assert_eq!(res, Scalar::Null, "suffix={suffix:?}");
            }
        }

        {
            let s = END_OF_UNICODE_RANGE
                .to_string()
                .repeat(STATS_STRING_PREFIX_LEN);
            let scalar = Scalar::String(s);
            assert_eq!(scalar.clone().trim_max(), scalar);
        }

        {
            let s = '👍'.to_string().repeat(STATS_STRING_PREFIX_LEN + 1);
            let res = Scalar::String(s.clone()).trim_max();
            let mut exp = '👍'.to_string().repeat(STATS_STRING_PREFIX_LEN - 1);
            exp.push('👎');
            assert_eq!(res, Scalar::String(exp));
        }

        {
            let mut s = '👍'.to_string().repeat(STATS_STRING_PREFIX_LEN - 1);
            s.push(END_OF_UNICODE_RANGE);
            s.push(END_OF_UNICODE_RANGE);

            let res = Scalar::String(s.clone()).trim_max();

            let mut exp = '👍'.to_string().repeat(STATS_STRING_PREFIX_LEN - 2);
            exp.push('👎');
            exp.push('\0');

            assert_eq!(res, Scalar::String(exp));
        }
    }

    /// Reducing statistics takes null bounds for "no value", so the elements
    /// of an array must get a lower bound as soon as one of them has a value.
    #[test]
    fn test_array_elements_with_a_value_have_a_lower_bound() {
        let element = TableDataType::Number(NumberDataType::Int32).wrap_nullable();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "a",
            TableDataType::Array(Box::new(element)),
        )]));
        let data_type = DataType::from(schema.field(0).data_type());
        let stats_of = |elements: Vec<Option<i32>>| {
            let mut builder = ColumnBuilder::with_capacity(&data_type, 1);
            builder.push(Scalar::Array(Int32Type::from_opt_data(elements)).as_ref());
            let block = DataBlock::new_from_columns(vec![builder.build()]);
            let stats =
                gen_columns_statistics(&block, None, &schema, &BTreeMap::new(), HashMap::new())
                    .unwrap();
            stats[&0].clone()
        };
        let five = Scalar::Number(5_i32.into());

        let empty = stats_of(vec![]);
        assert_eq!((empty.min(), empty.max()), (&Scalar::Null, &Scalar::Null));
        assert_eq!(empty.null_count, 0);

        let nulls = stats_of(vec![None, None]);
        assert_eq!((nulls.min(), nulls.max()), (&Scalar::Null, &Scalar::Null));
        assert_eq!(nulls.null_count, 2);

        // The null count is of elements: two in a block of one row.
        let with_value = stats_of(vec![None, None, Some(5)]);
        assert_eq!((with_value.min(), with_value.max()), (&five, &five));
        assert_eq!(with_value.null_count, 2);
    }
}
