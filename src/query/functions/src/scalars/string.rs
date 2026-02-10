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

use std::cmp::Ordering;
use std::io::Write;

use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression::unify_string;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_expression::vectorize_with_builder_4_arg;
use databend_functions_scalar_decimal::register_decimal_to_uuid;
use stringslice::StringSlice;

use crate::srfs;

pub const ALL_STRING_FUNC_NAMES: &[&str] = &[
    "upper",
    "lower",
    "bit_length",
    "octet_length",
    "length",
    "lpad",
    "rpad",
    "insert",
    "replace",
    "translate",
    "to_uuid",
    "strcmp",
    "instr",
    "position",
    "locate",
    "quote",
    "reverse",
    "ascii",
    "ltrim",
    "rtrim",
    "trim",
    "trim_leading",
    "trim_trailing",
    "trim_both",
    "to_hex",
    "bin",
    "oct",
    "to_hex",
    "repeat",
    "ord",
    "soundex",
    "space",
    "left",
    "right",
    "substr",
    "split",
    "split_part",
    "concat",
    "concat_ws",
    "regexp_instr",
    "regexp_like",
    "regexp_replace",
    "regexp_substr",
];

/// Functions that works with all strings, allow other types to be casted to string.
pub const PURE_STRING_FUNC_NAMES: &[&str] = &["concat", "concat_ws"];

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("to_string", &["to_varchar", "to_text"]);
    registry.register_aliases("upper", &["ucase"]);
    registry.register_aliases("lower", &["lcase"]);
    registry.register_aliases("length", &[
        "char_length",
        "character_length",
        "length_utf8",
    ]);
    registry.register_aliases("substr", &[
        "substring",
        "mid",
        "substr_utf8",
        "substring_utf8",
    ]);

    registry
        .scalar_builder("upper")
        .function()
        .typed_1_arg::<StringType, StringType>()
        .passthrough_nullable()
        .vectorized(vectorize_with_builder_1_arg::<StringType, StringType>(
            |val, output, _| {
                for ch in val.chars() {
                    if ch.is_ascii() {
                        output.put_char(ch.to_ascii_uppercase());
                    } else {
                        for x in ch.to_uppercase() {
                            output.put_char(x);
                        }
                    }
                }
                output.commit_row();
            },
        ))
        .register();

    registry
        .scalar_builder("lower")
        .function()
        .typed_1_arg::<StringType, StringType>()
        .passthrough_nullable()
        .vectorized(vectorize_with_builder_1_arg::<StringType, StringType>(
            |val, output, _| {
                for ch in val.chars() {
                    if ch.is_ascii() {
                        output.put_char(ch.to_ascii_lowercase());
                    } else {
                        for x in ch.to_lowercase() {
                            output.put_char(x);
                        }
                    }
                }
                output.commit_row();
            },
        ))
        .register();

    registry.register_1_arg::<StringType, NumberType<u64>, _>(
        "bit_length",
        |_, _| FunctionDomain::Full,
        |val, _| 8 * val.len() as u64,
    );

    registry
        .scalar_builder("octet_length")
        .function()
        .typed_1_arg::<StringType, NumberType<u64>>()
        .passthrough_nullable()
        .each_row(|val, _| val.len() as u64)
        .register();

    registry.register_1_arg::<StringType, NumberType<u64>, _>(
        "length",
        |_, _| FunctionDomain::Full,
        |val, _ctx| val.chars().count() as u64,
    );

    const MAX_PADDING_LENGTH: usize = 1000000;
    registry.register_passthrough_nullable_3_arg::<StringType, NumberType<u64>, StringType, StringType, _, _>(
        "lpad",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<StringType, NumberType<u64>, StringType, StringType>(
            |s, pad_len, pad, output, ctx| {
                let pad_len = pad_len as usize;
                let s_len = s.chars().count();
                if pad_len > MAX_PADDING_LENGTH {
                    ctx.set_error(output.len(), format!("padding length '{}' is too big, max is: '{}'", pad_len, MAX_PADDING_LENGTH));
                } else if pad_len <= s_len {
                    output.put_str(s.slice(..pad_len));
                } else if pad.is_empty() {
                    ctx.set_error(output.len(), format!("can't fill the '{}' length to '{}' with an empty pad string", s, pad_len));
                } else {
                    let mut remain_pad_len = pad_len - s_len;
                    let p_len = pad.chars().count();
                    while remain_pad_len > 0 {
                        if remain_pad_len < p_len {
                            output.put_str(pad.slice(..remain_pad_len));
                            remain_pad_len = 0;
                        } else {
                            output.put_str(pad);
                            remain_pad_len -= p_len;
                        }
                    }
                    output.put_str(s);
                }
                output.commit_row();
            }
        ),
    );

    registry.register_passthrough_nullable_4_arg::<StringType, NumberType<i64>, NumberType<i64>, StringType, StringType, _, _>(
        "insert",
        |_, _, _, _, _| FunctionDomain::Full,
        vectorize_with_builder_4_arg::<StringType, NumberType<i64>, NumberType<i64>, StringType, StringType>(
            |srcstr, pos, len, substr, output, _| {
                let pos = pos as usize;
                let len = len as usize;
                let srcstr_len = srcstr.chars().count();
                if pos < 1 || pos > srcstr_len {
                    output.put_str(srcstr);
                } else {
                    let pos = pos - 1;
                    output.put_str(srcstr.slice(0..pos));
                    output.put_str(substr);
                    if pos + len < srcstr_len {
                        output.put_str(srcstr.slice(pos + len .. ));
                    }
                }
                output.commit_row();
            }),
    );

    registry.register_passthrough_nullable_3_arg::<StringType, NumberType<u64>, StringType, StringType, _, _>(
        "rpad",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<StringType, NumberType<u64>, StringType, StringType>(
            |s, pad_len, pad, output, ctx| {
                let pad_len = pad_len as usize;
                let s_len = s.chars().count();
                if pad_len > MAX_PADDING_LENGTH {
                    ctx.set_error(output.len(), format!("padding length '{}' is too big, max is: '{}'", pad_len, MAX_PADDING_LENGTH));
                } else if pad_len <= s_len {
                    output.put_str(s.slice(..pad_len));
                } else if pad.is_empty() {
                    ctx.set_error(output.len(), format!("can't fill the '{}' length to '{}' with an empty pad string", s, pad_len));
                } else {
                    output.put_str(s);
                    let mut remain_pad_len = pad_len - s_len;
                    let p_len = pad.chars().count();
                    while remain_pad_len > 0 {
                        if remain_pad_len < p_len {
                            output.put_str(pad.slice(..remain_pad_len));
                            remain_pad_len = 0;
                        } else {
                            output.put_str(pad);
                            remain_pad_len -= p_len;
                        }
                    }
                }
                output.commit_row();
            }),
    );

    registry.register_passthrough_nullable_3_arg::<StringType, StringType, StringType, StringType, _, _>(
        "replace",
        |_, _, _, _| FunctionDomain::Full,
        vectorize_with_builder_3_arg::<StringType, StringType, StringType, StringType>(
            |str, from, to, output, _| {
                if from.is_empty() || from == to {
                    output.put_str(str);
                    output.commit_row();
                    return;
                }

                let mut last_end = 0;
                for (start, _) in str.match_indices(from) {
                    output.put_str(&str[last_end..start]);
                    output.put_str(to);
                    last_end = start + from.len();
                }
                output.put_str(&str[last_end..]);

                output.commit_row();
            }),
    );

    registry.register_passthrough_nullable_3_arg::<StringType, StringType, StringType, StringType, _, _>(
        "translate",
        |_, _, _, _| FunctionDomain::Full,
        vectorize_with_builder_3_arg::<StringType, StringType, StringType, StringType>(
            |str, from, to, output, _| {
                if from.is_empty() || from == to {
                    output.put_str(str);
                    output.commit_row();
                    return;
                }
                let to_len = to.chars().count();
                str.chars().for_each(|x| {
                    if let Some(index) = from.chars().position(|c| c == x) {
                        if index < to_len {
                            output.put_char(to.chars().nth(index).unwrap());
                        }
                    } else {
                        output.put_char(x);
                    }
                });
                output.commit_row();
            }),
    );

    register_decimal_to_uuid(registry);

    registry.register_2_arg::<StringType, StringType, NumberType<i8>, _>(
        "strcmp",
        |_, lhs, rhs| {
            let (d1, d2) = unify_string(lhs, rhs);
            if d1.min > d2.max {
                return FunctionDomain::Domain(SimpleDomain { min: 1, max: 1 });
            }
            if d1.max < d2.min {
                return FunctionDomain::Domain(SimpleDomain { min: -1, max: -1 });
            }
            if d1.min == d1.max && d2.min == d2.max && d1.min == d2.min {
                return FunctionDomain::Domain(SimpleDomain { min: 0, max: 0 });
            }
            FunctionDomain::Full
        },
        |s1, s2, _| match s1.cmp(s2) {
            Ordering::Equal => 0,
            Ordering::Greater => 1,
            Ordering::Less => -1,
        },
    );

    let find_at = |s: &str, substr: &str, pos: u64| {
        if substr.is_empty() {
            // the same behavior as MySQL, Postgres and Clickhouse
            return if pos == 0 { 1_u64 } else { pos };
        }

        let pos = pos as usize;
        if pos == 0 {
            return 0_u64;
        }
        let p = pos - 1;

        let src = s.slice(p..);
        if let Some(find_at) = src.find(substr) {
            (src[..find_at].chars().count() + p + 1) as u64
        } else {
            0_u64
        }
    };

    fn instr(s: &str, sub: &str, position: i64, occurrence: u64) -> u64 {
        if occurrence == 0 {
            return 0;
        }

        let s_len_chars = s.chars().count();
        let sub_len_chars = sub.chars().count();
        if sub_len_chars > s_len_chars {
            return 0;
        }

        if sub.is_empty() {
            return if position > 0 {
                // For forward search, the empty string matches at the position
                // such as: INSTR (' ABC ', ', 1) - > 1, INSTR (' ABC ', ' ', 4) - > 4
                // The effective range is from 1 to s_len_chars + 1
                if (position as usize) <= s_len_chars + 1 {
                    position as u64
                } else {
                    0
                }
            } else {
                // For reverse search, empty strings match positions after the end of the string
                // such as: INSTR('abc', '', -1) -> 4 (s_len_chars + 1)
                (s_len_chars + 1) as u64
            };
        }

        let mut char_to_byte_map = Vec::with_capacity(s_len_chars + 1);
        for (byte_idx, _) in s.char_indices() {
            char_to_byte_map.push(byte_idx);
        }
        char_to_byte_map.push(s.len());

        let get_slice_by_char_idx = |start_char_idx: usize, num_chars: usize| -> Option<&str> {
            if start_char_idx + num_chars > s_len_chars {
                return None;
            }
            let byte_start = char_to_byte_map[start_char_idx];
            let byte_end = char_to_byte_map[start_char_idx + num_chars];
            s.get(byte_start..byte_end)
        };

        let mut found_count = 0;

        if position > 0 {
            let start_char_idx_0_indexed = (position - 1) as usize;
            for current_char_idx in
                start_char_idx_0_indexed..=s_len_chars.saturating_sub(sub_len_chars)
            {
                if let Some(slice) = get_slice_by_char_idx(current_char_idx, sub_len_chars)
                    && slice == sub
                {
                    found_count += 1;
                    if found_count == occurrence {
                        return (current_char_idx + 1) as u64;
                    }
                }
            }
        } else {
            let search_start_char_0_indexed =
                s_len_chars.saturating_sub(position.unsigned_abs() as usize);

            let max_possible_match_start_idx =
                search_start_char_0_indexed.min(s_len_chars.saturating_sub(sub_len_chars));

            for current_char_idx in (0..=max_possible_match_start_idx).rev() {
                if let Some(slice) = get_slice_by_char_idx(current_char_idx, sub_len_chars)
                    && slice == sub
                {
                    found_count += 1;
                    if found_count == occurrence {
                        return (current_char_idx + 1) as u64;
                    }
                }
            }
        }

        0
    }

    registry.register_2_arg::<StringType, StringType, NumberType<u64>, _>(
        "instr",
        |_, _, _| FunctionDomain::Full,
        move |s: &str, substr: &str, _| find_at(s, substr, 1),
    );

    registry.register_3_arg::<StringType, StringType, NumberType<i64>, NumberType<u64>, _, _>(
        "instr",
        |_, _, _, _| FunctionDomain::Full,
        move |s: &str, substr: &str, pos, _| instr(s, substr, pos, 1),
    );

    registry.register_4_arg::<StringType, StringType, NumberType<i64>, NumberType<u64>, NumberType<u64>, _, _>(
        "instr",
        |_, _,_, _, _| FunctionDomain::Full,
        move |s: &str, substr: &str, pos, occurrence, _| instr(s, substr, pos, occurrence),
    );

    registry.register_2_arg::<StringType, StringType, NumberType<u64>, _>(
        "position",
        |_, _, _| FunctionDomain::Full,
        move |substr: &str, s: &str, _| find_at(s, substr, 1),
    );

    registry.register_2_arg::<StringType, StringType, NumberType<u64>, _>(
        "locate",
        |_, _, _| FunctionDomain::Full,
        move |substr: &str, s: &str, _| find_at(s, substr, 1),
    );

    registry.register_3_arg::<StringType, StringType, NumberType<u64>, NumberType<u64>, _, _>(
        "locate",
        |_, _, _, _| FunctionDomain::Full,
        move |substr: &str, s: &str, pos: u64, _| find_at(s, substr, pos),
    );

    registry
        .scalar_builder("quote")
        .function()
        .typed_1_arg::<StringType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<StringType, StringType>(
            |val, output, _| {
                for ch in val.chars() {
                    match ch {
                        '\0' => output.put_str("\\0"),
                        '\'' => output.put_str("\\'"),
                        '\"' => output.put_str("\\\""),
                        '\u{8}' => output.put_str("\\b"),
                        '\n' => output.put_str("\\n"),
                        '\r' => output.put_str("\\r"),
                        '\t' => output.put_str("\\t"),
                        '\\' => output.put_str("\\\\"),
                        c => output.put_char(c),
                    }
                }
                output.commit_row();
            },
        ))
        .register();

    registry
        .scalar_builder("reverse")
        .function()
        .typed_1_arg::<StringType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<StringType, StringType>(
            |val, output, _| {
                for char in val.chars().rev() {
                    output.put_char(char);
                }
                output.commit_row();
            },
        ))
        .register();

    registry.register_1_arg::<StringType, NumberType<u8>, _>(
        "ascii",
        |_, domain| {
            FunctionDomain::Domain(SimpleDomain {
                min: domain.min.as_bytes().first().map_or(0, |v| *v),
                max: domain
                    .max
                    .as_ref()
                    .and_then(|x| x.as_bytes().first())
                    .map_or(u8::MAX, |v| *v),
            })
        },
        |val, _| val.as_bytes().first().map_or(0, |v| *v),
    );

    registry.register_1_arg::<StringType, NumberType<u32>, _>(
        "unicode",
        |_, _| FunctionDomain::Full,
        |val, _| val.chars().next().map_or(0, |c| c as u32),
    );

    // Trim functions
    registry
        .scalar_builder("ltrim")
        .function()
        .typed_1_arg::<StringType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<StringType, StringType>(
            |val, output, _| {
                output.put_and_commit(val.trim_start_matches(' '));
            },
        ))
        .register();

    registry
        .scalar_builder("rtrim")
        .function()
        .typed_1_arg::<StringType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<StringType, StringType>(
            |val, output, _| {
                output.put_and_commit(val.trim_end_matches(' '));
            },
        ))
        .register();

    registry
        .scalar_builder("trim")
        .function()
        .typed_1_arg::<StringType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<StringType, StringType>(
            |val, output, _| {
                output.put_and_commit(val.trim_matches(' '));
            },
        ))
        .register();

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "ltrim",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, StringType, StringType>(
            |val, trim_str, output, _| {
                if trim_str.is_empty() {
                    output.put_and_commit(val);
                    return;
                }

                let p: Vec<_> = trim_str.chars().collect();
                output.put_and_commit(val.trim_start_matches(p.as_slice()));
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "trim_leading",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, StringType, StringType>(
            |val, trim_str, output, _| {
                if trim_str.is_empty() {
                    output.put_and_commit(val);
                    return;
                }

                output.put_and_commit(val.trim_start_matches(trim_str));
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "rtrim",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, StringType, StringType>(
            |val, trim_str, output, _| {
                if trim_str.is_empty() {
                    output.put_and_commit(val);
                    return;
                }

                let p: Vec<_> = trim_str.chars().collect();
                output.put_and_commit(val.trim_end_matches(p.as_slice()));
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "trim_trailing",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, StringType, StringType>(
            |val, trim_str, output, _| {
                if trim_str.is_empty() {
                    output.put_and_commit(val);
                    return;
                }

                output.put_and_commit(val.trim_end_matches(trim_str));
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "trim_both",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, StringType, StringType>(
            |val, trim_str, output, _| {
                if trim_str.is_empty() {
                    output.put_and_commit(val);
                    return;
                }

                let mut res = val;

                while res.starts_with(trim_str) {
                    res = &res[trim_str.len()..];
                }
                while res.ends_with(trim_str) {
                    res = &res[..res.len() - trim_str.len()];
                }

                output.put_and_commit(res);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "trim",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, StringType, StringType>(
            |val, trim_str, output, _| {
                if trim_str.is_empty() {
                    output.put_and_commit(val);
                    return;
                }

                let p: Vec<_> = trim_str.chars().collect();
                output.put_and_commit(val.trim_matches(p.as_slice()));
            },
        ),
    );

    registry
        .scalar_builder("to_hex")
        .function()
        .typed_1_arg::<StringType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<StringType, StringType>(
            |val, output, _| {
                let len = val.len() * 2;
                output.row_buffer.resize(len, 0);
                hex::encode_to_slice(val, &mut output.row_buffer).unwrap();
                output.commit_row();
            },
        ))
        .register();

    // TODO: generalize them to be alias of [CONV](https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_conv)
    // Tracking issue: https://github.com/datafuselabs/databend/issues/7242
    registry
        .scalar_builder("bin")
        .function()
        .typed_1_arg::<NumberType<i64>, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<NumberType<i64>, StringType>(
            |val, output, _| {
                write!(output.row_buffer, "{val:b}").unwrap();
                output.commit_row();
            },
        ))
        .register();

    registry
        .scalar_builder("oct")
        .function()
        .typed_1_arg::<NumberType<i64>, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<NumberType<i64>, StringType>(
            |val, output, _| {
                write!(output.row_buffer, "{val:o}").unwrap();
                output.commit_row();
            },
        ))
        .register();

    registry
        .scalar_builder("to_hex")
        .function()
        .typed_1_arg::<NumberType<i64>, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<NumberType<i64>, StringType>(
            |val, output, _| {
                write!(output.row_buffer, "{val:x}").unwrap();
                output.commit_row();
            },
        ))
        .register();

    const MAX_REPEAT_TIMES: u64 = 1000000;
    registry.register_passthrough_nullable_2_arg::<StringType, NumberType<u64>, StringType, _, _>(
        "repeat",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, NumberType<u64>, StringType>(
            |a, times, output, ctx| {
                if times > MAX_REPEAT_TIMES {
                    ctx.set_error(
                        output.len(),
                        format!(
                            "Too many times to repeat: ({}), maximum is: {}",
                            times, MAX_REPEAT_TIMES
                        ),
                    );
                } else {
                    (0..times).for_each(|_| output.put_str(a));
                }
                output.commit_row();
            },
        ),
    );

    registry.register_1_arg::<StringType, UInt64Type, _>(
        "ord",
        |_, _| FunctionDomain::Full,
        |str: &str, _| {
            let str = str.as_bytes();
            let mut res: u64 = 0;
            if !str.is_empty() {
                if str[0].is_ascii() {
                    res = str[0] as u64;
                } else {
                    for (p, _) in str.iter().enumerate() {
                        let s = &str[0..p + 1];
                        if std::str::from_utf8(s).is_ok() {
                            for (i, b) in s.iter().rev().enumerate() {
                                res += (*b as u64) * 256_u64.pow(i as u32);
                            }
                            break;
                        }
                    }
                }
            }
            res
        },
    );

    registry
        .scalar_builder("soundex")
        .function()
        .typed_1_arg::<StringType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<StringType, StringType>(
            soundex::soundex,
        ))
        .register();

    const MAX_SPACE_LENGTH: u64 = 1000000;
    registry
        .scalar_builder("space")
        .function()
        .typed_1_arg::<NumberType<u64>, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(vectorize_with_builder_1_arg::<NumberType<u64>, StringType>(
            |times, output, ctx| {
                if times > MAX_SPACE_LENGTH {
                    ctx.set_error(
                        output.len(),
                        format!("space length is too big, max is: {}", MAX_SPACE_LENGTH),
                    );
                } else {
                    for _ in 0..times {
                        output.put_char(' ');
                    }
                }
                output.commit_row();
            },
        ))
        .register();

    registry.register_passthrough_nullable_2_arg::<StringType, NumberType<u64>, StringType, _, _>(
        "left",
        |_, lhs, rhs| {
            let rm = rhs.min as usize;
            let min = if rm < lhs.min.chars().count() {
                lhs.min.slice(0..rm).to_string()
            } else {
                lhs.min.clone()
            };

            let rn = rhs.max as usize;
            let max = lhs.max.as_ref().map(|ln| {
                if rn < ln.chars().count() {
                    ln.slice(0..rn).to_string()
                } else {
                    ln.clone()
                }
            });

            FunctionDomain::Domain(StringDomain { min, max })
        },
        vectorize_with_builder_2_arg::<StringType, NumberType<u64>, StringType>(
            |s, n, output, _| {
                let n = n as usize;
                let s_len = s.chars().count();
                if n < s_len {
                    output.put_and_commit(s.slice(0..n));
                } else {
                    output.put_and_commit(s);
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, NumberType<u64>, StringType, _, _>(
        "right",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, NumberType<u64>, StringType>(
            |s, n, output, _| {
                let n = n as usize;
                let s_len = s.chars().count();
                if n < s_len {
                    output.put_and_commit(s.slice(s_len - n..));
                } else {
                    output.put_and_commit(s);
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, NumberType<i64>, StringType, _, _>(
        "substr",
        |_, lhs, rhs| {
            if rhs.min == rhs.max && rhs.min == 1 {
                FunctionDomain::Domain(lhs.clone())
            } else {
                FunctionDomain::Full
            }
        },
        vectorize_with_builder_2_arg::<StringType, NumberType<i64>, StringType>(
            |s, pos, output, _ctx| {
                substr(output, s, pos, s.len() as u64);
            },
        ),
    );

    registry.register_passthrough_nullable_3_arg::<StringType, NumberType<i64>, NumberType<u64>, StringType, _, _>(
        "substr",
             |_, arg1, arg2, arg3| {
                if arg2.min == arg2.max && arg2.min == 1 {
                    let rm = arg3.min as usize;
                    let min = if rm < arg1.min.chars().count() {
                        arg1.min.slice(0..rm).to_string()
                    } else {
                        arg1.min.clone()
                    };

                    let rn = arg3.max as usize;
                    let max = arg1.max.as_ref().map(|ln| {
                        if rn < ln.chars().count() {
                            ln.slice(0..rn).to_string()
                        } else {
                            ln.clone()
                        }
                    });

                    FunctionDomain::Domain(StringDomain { min, max })
                } else {
                    FunctionDomain::Full
                }
            },
             vectorize_with_builder_3_arg::<StringType, NumberType<i64>, NumberType<u64>, StringType>(|s, pos, len, output, _ctx| {
                substr(output, s, pos, len);
             }),
         );

    registry
        .register_passthrough_nullable_2_arg::<StringType, StringType, ArrayType<StringType>, _, _>(
            "split",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<StringType, StringType, ArrayType<StringType>>(
                |s, sep, output, _ctx| {
                    if s == sep {
                        output.builder.commit_row();
                    } else if sep.is_empty() {
                        output.builder.put_and_commit(s);
                    } else {
                        for v in s.split(sep) {
                            output.builder.put_and_commit(v);
                        }
                    }
                    output.commit_row();
                },
            ),
        );

    registry
        .register_passthrough_nullable_2_arg::<StringType, StringType, ArrayType<StringType>, _, _>(
            "regexp_split_to_array",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<StringType, StringType, ArrayType<StringType>>(
                |s, sep, output, ctx| {
                    let res = srfs::string::regexp_split_to_vec(s, sep, None, ctx);
                    for v in res {
                        output.builder.put_and_commit(v);
                    }
                    output.commit_row();
                },
            ),
        );

    registry
        .register_passthrough_nullable_3_arg::<StringType, StringType, StringType, ArrayType<StringType>, _, _>(
            "regexp_split_to_array",
            |_, _, _, _| FunctionDomain::Full,
            vectorize_with_builder_3_arg::<StringType, StringType, StringType, ArrayType<StringType>>(
                |s, sep, flag, output, ctx| {
                    let res = srfs::string::regexp_split_to_vec(s, sep, Some(flag), ctx);
                    for v in res {
                        output.builder.put_and_commit(v);
                    }
                    output.commit_row();
                },
            ),
        );

    registry
        .register_passthrough_nullable_3_arg::<StringType, StringType, NumberType<i64>, StringType, _, _>(
            "split_part",
            |_, _, _, _| FunctionDomain::Full,
            vectorize_with_builder_3_arg::<StringType, StringType, NumberType<i64>, StringType>(
                |s, sep, part, output, _| {
                    if sep.is_empty() {
                        if part == 0 || part == 1 || part == -1 {
                            output.put_str(s);
                        }
                    } else if s != sep {
                        if part < 0 {
                            let idx = (-part-1) as usize;
                            for (i, v) in s.rsplit(sep).enumerate() {
                                if i == idx {
                                    output.put_str(v);
                                    break;
                                }
                            }
                        } else {
                            let idx = if part == 0 {
                                0usize
                            } else {
                                (part - 1) as usize
                            };
                            for (i, v) in s.split(sep).enumerate() {
                                if i == idx {
                                    output.put_str(v);
                                    break;
                                }
                            }
                        }
                    }
                    output.commit_row();
                },
            ),
        );
}

pub(crate) mod soundex {
    use databend_common_expression::EvalContext;
    use databend_common_expression::types::string::StringColumnBuilder;

    pub fn soundex(val: &str, output: &mut StringColumnBuilder, _eval_context: &mut EvalContext) {
        let mut last = None;
        let mut count = 0;

        for ch in val.chars() {
            let score = number_map(ch);
            if last.is_none() {
                if !is_uni_alphabetic(ch) {
                    continue;
                }
                last = score;
                output.put_char(ch.to_ascii_uppercase());
            } else {
                if !ch.is_ascii_alphabetic() || is_drop(ch) || score.is_none() || score == last {
                    continue;
                }
                last = score;
                output.put_char(score.unwrap() as char);
            }

            count += 1;
        }
        // add '0'
        for _ in count..4 {
            output.put_char('0');
        }

        output.commit_row();
    }

    #[inline(always)]
    fn number_map(i: char) -> Option<u8> {
        match i.to_ascii_lowercase() {
            'b' | 'f' | 'p' | 'v' => Some(b'1'),
            'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => Some(b'2'),
            'd' | 't' => Some(b'3'),
            'l' => Some(b'4'),
            'm' | 'n' => Some(b'5'),
            'r' => Some(b'6'),
            _ => Some(b'0'),
        }
    }

    #[inline(always)]
    fn is_drop(c: char) -> bool {
        matches!(
            c.to_ascii_lowercase(),
            'a' | 'e' | 'i' | 'o' | 'u' | 'y' | 'h' | 'w'
        )
    }

    // https://github.com/mysql/mysql-server/blob/3290a66c89eb1625a7058e0ef732432b6952b435/sql/item_strfunc.cc#L1919
    #[inline(always)]
    fn is_uni_alphabetic(c: char) -> bool {
        c.is_ascii_lowercase() || c.is_ascii_uppercase() || c as i32 >= 0xC0
    }
}

#[inline]
fn substr(builder: &mut StringColumnBuilder, str: &str, pos: i64, len: u64) {
    if pos == 0 || len == 0 {
        builder.commit_row();
        return;
    }

    let char_len = str.chars().count();
    let start = if pos > 0 {
        (pos - 1).min(char_len as i64) as usize
    } else {
        char_len
            .checked_sub(pos.unsigned_abs() as usize)
            .unwrap_or(char_len)
    };

    builder.put_char_iter(str.chars().skip(start).take(len as usize));
    builder.commit_row();
}
