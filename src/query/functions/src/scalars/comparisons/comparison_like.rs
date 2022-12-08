// Copyright 2021 Datafuse Labs.
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

use common_datavalues::prelude::*;

use super::comparison::StringSearchCreator;
use super::utils::StringSearchImpl;

pub type ComparisonLikeFunction = StringSearchCreator<false, StringSearchLike>;
pub type ComparisonNotLikeFunction = StringSearchCreator<true, StringSearchLike>;

#[derive(Clone)]
pub struct StringSearchLike;

impl StringSearchImpl for StringSearchLike {
    /// QUOTE: (From arrow2::arrow::compute::like::a_like_binary)
    fn vector_vector(
        lhs: &StringColumn,
        rhs: &StringColumn,
        op: impl Fn(bool) -> bool,
    ) -> BooleanColumn {
        let mut builder: ColumnBuilder<bool> = ColumnBuilder::with_capacity(lhs.len());

        for (lhs_value, rhs_value) in lhs.scalar_iter().zip(rhs.scalar_iter()) {
            builder.append(op(like(lhs_value, rhs_value)));
        }
        builder.build_column()
    }

    /// QUOTE: (From arrow2::arrow::compute::like::a_like_binary_scalar)
    fn vector_const(lhs: &StringColumn, rhs: &[u8], op: impl Fn(bool) -> bool) -> BooleanColumn {
        match check_pattern_type(rhs, false) {
            PatternType::OrdinalStr => {
                BooleanColumn::from_iterator(lhs.scalar_iter().map(|x| op(x == rhs)))
            }
            PatternType::EndOfPercent => {
                // fast path, can use starts_with
                let starts_with = &rhs[..rhs.len() - 1];
                BooleanColumn::from_iterator(
                    lhs.scalar_iter().map(|x| op(x.starts_with(starts_with))),
                )
            }
            PatternType::StartOfPercent => {
                // fast path, can use ends_with
                let ends_with = &rhs[1..];
                BooleanColumn::from_iterator(lhs.scalar_iter().map(|x| op(x.ends_with(ends_with))))
            }
            PatternType::PatternStr => {
                BooleanColumn::from_iterator(lhs.scalar_iter().map(|x| op(like(x, rhs))))
            }
        }
    }
}

#[inline]
fn is_like_pattern_escape(c: char) -> bool {
    c == '%' || c == '_' || c == '\\'
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum PatternType {
    // e.g. 'Arrow'
    OrdinalStr,
    // e.g. 'A%row'
    PatternStr,
    // e.g. '%rrow'
    StartOfPercent,
    // e.g. 'Arro%'
    EndOfPercent,
}

/// Check the like pattern type.
///
/// is_pruning: indicate whether to be called on range_filter for pruning.
///
/// For example:
///
/// 'a\\%row'
/// '\\%' will be escaped to a percent. Need transform to `a%row`.
///
/// If is_pruning is true, will be called on range_filter:L379.
/// OrdinalStr is returned, because the pattern can be transformed by range_filter:L382.
///
/// If is_pruning is false, will be called on like.rs:L74.
/// PatternStr is returned, because the pattern cannot be used directly on like.rs:L76.
#[inline]
pub fn check_pattern_type(pattern: &[u8], is_pruning: bool) -> PatternType {
    let len = pattern.len();
    if len == 0 {
        return PatternType::OrdinalStr;
    }

    let mut index = 0;
    let start_percent = pattern[0] == b'%';
    if start_percent {
        if is_pruning {
            return PatternType::PatternStr;
        }
        index += 1;
    }

    while index < len {
        match pattern[index] {
            b'_' => return PatternType::PatternStr,
            b'%' => {
                if index == len - 1 && !start_percent {
                    return PatternType::EndOfPercent;
                }
                return PatternType::PatternStr;
            }
            b'\\' => {
                if index < len - 1 {
                    index += 1;
                    if !is_pruning && is_like_pattern_escape(pattern[index] as char) {
                        return PatternType::PatternStr;
                    }
                }
            }
            _ => {}
        }
        index += 1;
    }

    if start_percent {
        PatternType::StartOfPercent
    } else {
        PatternType::OrdinalStr
    }
}

#[inline]
fn decode_one(data: &[u8]) -> Option<(u8, usize)> {
    if data.is_empty() {
        None
    } else {
        Some((data[0], 1))
    }
}

#[inline]
/// Borrow from [tikv](https://github.com/tikv/tikv/blob/fe997db4db8a5a096f8a45c0db3eb3c2e5879262/components/tidb_query_expr/src/impl_like.rs)
fn like(haystack: &[u8], pattern: &[u8]) -> bool {
    // current search positions in pattern and target.
    let (mut px, mut tx) = (0, 0);
    // positions for backtrace.
    let (mut next_px, mut next_tx) = (0, 0);
    while px < pattern.len() || tx < haystack.len() {
        if let Some((c, mut poff)) = decode_one(&pattern[px..]) {
            let code: u32 = c.into();
            if code == '_' as u32 {
                if let Some((_, toff)) = decode_one(&haystack[tx..]) {
                    px += poff;
                    tx += toff;
                    continue;
                }
            } else if code == '%' as u32 {
                // update the backtrace point.
                next_px = px;
                px += poff;
                next_tx = tx;
                next_tx += if let Some((_, toff)) = decode_one(&haystack[tx..]) {
                    toff
                } else {
                    1
                };
                continue;
            } else {
                if code == '\\' as u32 && px + poff < pattern.len() {
                    px += poff;
                    poff = if let Some((_, off)) = decode_one(&pattern[px..]) {
                        off
                    } else {
                        break;
                    }
                }
                if let Some((_, toff)) = decode_one(&haystack[tx..]) {
                    if let std::cmp::Ordering::Equal =
                        haystack[tx..tx + toff].cmp(&pattern[px..px + poff])
                    {
                        tx += toff;
                        px += poff;
                        continue;
                    }
                }
            }
        }
        // mismatch and backtrace to last %.
        if 0 < next_tx && next_tx <= haystack.len() {
            px = next_px;
            tx = next_tx;
            continue;
        }
        return false;
    }
    true
}
