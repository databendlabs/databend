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

use databend_common_base::base::uuid::Uuid;
use databend_common_expression::types::decimal::Decimal128Type;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::string::StringColumn;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::unify_string;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_expression::vectorize_with_builder_4_arg;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;
use stringslice::StringSlice;

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

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "upper",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| col.data().len(),
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
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "lower",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| col.data().len(),
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
        ),
    );

    registry.register_1_arg::<StringType, NumberType<u64>, _, _>(
        "bit_length",
        |_, _| FunctionDomain::Full,
        |val, _| 8 * val.len() as u64,
    );

    registry.register_passthrough_nullable_1_arg::<StringType, NumberType<u64>, _, _>(
        "octet_length",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            ValueRef::Scalar(s) => Value::Scalar(s.len() as u64),
            ValueRef::Column(c) => {
                let diffs = c
                    .offsets()
                    .iter()
                    .zip(c.offsets().iter().skip(1))
                    .map(|(a, b)| b - a)
                    .collect::<Vec<_>>();

                Value::Column(diffs.into())
            }
        },
    );

    registry.register_1_arg::<StringType, NumberType<u64>, _, _>(
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

    registry.register_passthrough_nullable_1_arg::<Decimal128Type, StringType, _, _>(
        "to_uuid",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<Decimal128Type, StringType>(|arg, output, _| {
            let uuid = Uuid::from_u128(arg as u128);
            let str = uuid.as_simple().to_string();
            output.put_str(str.as_str());
            output.commit_row();
        }),
    );

    registry.register_2_arg::<StringType, StringType, NumberType<i8>, _, _>(
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

    registry.register_2_arg::<StringType, StringType, NumberType<u64>, _, _>(
        "instr",
        |_, _, _| FunctionDomain::Full,
        move |s: &str, substr: &str, _| find_at(s, substr, 1),
    );

    registry.register_2_arg::<StringType, StringType, NumberType<u64>, _, _>(
        "position",
        |_, _, _| FunctionDomain::Full,
        move |substr: &str, s: &str, _| find_at(s, substr, 1),
    );

    registry.register_2_arg::<StringType, StringType, NumberType<u64>, _, _>(
        "locate",
        |_, _, _| FunctionDomain::Full,
        move |substr: &str, s: &str, _| find_at(s, substr, 1),
    );

    registry.register_3_arg::<StringType, StringType, NumberType<u64>, NumberType<u64>, _, _>(
        "locate",
        |_, _, _, _| FunctionDomain::Full,
        move |substr: &str, s: &str, pos: u64, _| find_at(s, substr, pos),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "quote",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| col.data().len() * 2,
            |val, output, _| {
                for ch in val.chars() {
                    match ch {
                        '\0' => output.put_str("\\0"),
                        '\'' => output.put_str("\\\'"),
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
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "reverse",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| col.data().len(),
            |val, output, _| {
                for char in val.chars().rev() {
                    output.put_char(char);
                }
                output.commit_row();
            },
        ),
    );

    registry.register_1_arg::<StringType, NumberType<u8>, _, _>(
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

    // Trim functions
    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "ltrim",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| col.data().len(),
            |val, output, _| {
                output.put_str(val.trim_start());
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "rtrim",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| col.data().len(),
            |val, output, _| {
                output.put_str(val.trim_end());
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "trim",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| col.data().len(),
            |val, output, _| {
                output.put_str(val.trim());
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "trim_leading",
        |_, _, _| FunctionDomain::Full,
        vectorize_string_to_string_2_arg(
            |col, _| col.data().len(),
            |val, trim_str, _, output| {
                if trim_str.is_empty() {
                    output.put_str(val);
                    output.commit_row();
                    return;
                }

                output.put_str(val.trim_start_matches(trim_str));
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "trim_trailing",
        |_, _, _| FunctionDomain::Full,
        vectorize_string_to_string_2_arg(
            |col, _| col.data().len(),
            |val, trim_str, _, output| {
                if trim_str.is_empty() {
                    output.put_str(val);
                    output.commit_row();
                    return;
                }

                output.put_str(val.trim_end_matches(trim_str));
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "trim_both",
        |_, _, _| FunctionDomain::Full,
        vectorize_string_to_string_2_arg(
            |col, _| col.data().len(),
            |val, trim_str, _, output| {
                if trim_str.is_empty() {
                    output.put_str(val);
                    output.commit_row();
                    return;
                }

                let mut res = val;

                while res.starts_with(trim_str) {
                    res = &res[trim_str.len()..];
                }
                while res.ends_with(trim_str) {
                    res = &res[..res.len() - trim_str.len()];
                }

                output.put_str(res);
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "to_hex",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| col.data().len() * 2,
            |val, output, _| {
                let old_len = output.data.len();
                let extra_len = val.len() * 2;
                output.data.resize(old_len + extra_len, 0);
                hex::encode_to_slice(val, &mut output.data[old_len..]).unwrap();
                output.commit_row();
            },
        ),
    );

    // TODO: generalize them to be alias of [CONV](https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_conv)
    // Tracking issue: https://github.com/datafuselabs/databend/issues/7242
    registry.register_passthrough_nullable_1_arg::<NumberType<i64>, StringType, _, _>(
        "bin",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<NumberType<i64>, StringType>(|val, output, _| {
            write!(output.data, "{val:b}").unwrap();
            output.commit_row();
        }),
    );
    registry.register_passthrough_nullable_1_arg::<NumberType<i64>, StringType, _, _>(
        "oct",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<NumberType<i64>, StringType>(|val, output, _| {
            write!(output.data, "{val:o}").unwrap();
            output.commit_row();
        }),
    );
    registry.register_passthrough_nullable_1_arg::<NumberType<i64>, StringType, _, _>(
        "to_hex",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<NumberType<i64>, StringType>(|val, output, _| {
            write!(output.data, "{val:x}").unwrap();
            output.commit_row();
        }),
    );

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

    registry.register_1_arg::<StringType, UInt64Type, _, _>(
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

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "soundex",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| usize::max(col.data().len(), 4 * col.len()),
            soundex::soundex,
        ),
    );

    const MAX_SPACE_LENGTH: u64 = 1000000;
    registry.register_passthrough_nullable_1_arg::<NumberType<u64>, StringType, _, _>(
        "space",
        |_, _| FunctionDomain::MayThrow,
        |times, ctx| match times {
            ValueRef::Scalar(times) => {
                if times > MAX_SPACE_LENGTH {
                    ctx.set_error(
                        0,
                        format!("space length is too big, max is: {}", MAX_SPACE_LENGTH),
                    );
                    Value::Scalar("".to_string())
                } else {
                    Value::Scalar(" ".repeat(times as usize))
                }
            }
            ValueRef::Column(col) => {
                let mut total_space: u64 = 0;
                let mut offsets: Vec<u64> = Vec::with_capacity(col.len() + 1);
                offsets.push(0);
                for (i, times) in col.iter().enumerate() {
                    if times > &MAX_SPACE_LENGTH {
                        ctx.set_error(
                            i,
                            format!("space length is too big, max is: {}", MAX_SPACE_LENGTH),
                        );
                        break;
                    }
                    total_space += times;
                    offsets.push(total_space);
                }
                if ctx.errors.is_some() {
                    offsets.truncate(1);
                    total_space = 0;
                }
                let col = StringColumnBuilder {
                    data: " ".repeat(total_space as usize).into_bytes(),
                    offsets,
                    need_estimated: false,
                }
                .build();
                Value::Column(col)
            }
        },
    );

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
                    output.put_str(s.slice(0..n));
                } else {
                    output.put_str(s);
                }
                output.commit_row();
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
                    output.put_str(s.slice(s_len - n..));
                } else {
                    output.put_str(s);
                }
                output.commit_row();
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
                        output.builder.put_str(s);
                        output.builder.commit_row();
                    } else {
                        for v in s.split(sep) {
                            output.builder.put_str(v);
                            output.builder.commit_row();
                        }
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
        )
}

pub(crate) mod soundex {
    use databend_common_expression::types::string::StringColumnBuilder;
    use databend_common_expression::EvalContext;

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

/// String to String scalar function with estimated output column capacity.
pub fn vectorize_string_to_string(
    estimate_bytes: impl Fn(&StringColumn) -> usize + Copy,
    func: impl Fn(&str, &mut StringColumnBuilder, &mut EvalContext) + Copy,
) -> impl Fn(ValueRef<StringType>, &mut EvalContext) -> Value<StringType> + Copy {
    move |arg1, ctx| match arg1 {
        ValueRef::Scalar(val) => {
            let mut builder = StringColumnBuilder::with_capacity(1, 0);
            func(val, &mut builder, ctx);
            Value::Scalar(builder.build_scalar())
        }
        ValueRef::Column(col) => {
            let data_capacity = estimate_bytes(&col);
            let mut builder = StringColumnBuilder::with_capacity(col.len(), data_capacity);
            for val in col.iter() {
                func(val, &mut builder, ctx);
            }

            Value::Column(builder.build())
        }
    }
}

/// (String, String) to String scalar function with estimated output column capacity.
fn vectorize_string_to_string_2_arg(
    estimate_bytes: impl Fn(&StringColumn, &StringColumn) -> usize + Copy,
    func: impl Fn(&str, &str, &mut EvalContext, &mut StringColumnBuilder) + Copy,
) -> impl Fn(ValueRef<StringType>, ValueRef<StringType>, &mut EvalContext) -> Value<StringType> + Copy
{
    move |arg1, arg2, ctx| match (arg1, arg2) {
        (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2)) => {
            let mut builder = StringColumnBuilder::with_capacity(1, 0);
            func(arg1, arg2, ctx, &mut builder);
            Value::Scalar(builder.build_scalar())
        }
        (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
            let data_capacity =
                estimate_bytes(&StringColumnBuilder::repeat(arg1, 1).build(), &arg2);
            let mut builder = StringColumnBuilder::with_capacity(arg2.len(), data_capacity);
            for val in arg2.iter() {
                func(arg1, val, ctx, &mut builder);
            }
            Value::Column(builder.build())
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(arg2)) => {
            let data_capacity =
                estimate_bytes(&arg1, &StringColumnBuilder::repeat(arg2, 1).build());
            let mut builder = StringColumnBuilder::with_capacity(arg1.len(), data_capacity);
            for val in arg1.iter() {
                func(val, arg2, ctx, &mut builder);
            }
            Value::Column(builder.build())
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
            let data_capacity = estimate_bytes(&arg1, &arg2);
            let mut builder = StringColumnBuilder::with_capacity(arg1.len(), data_capacity);
            let iter = arg1.iter().zip(arg2.iter());
            for (val1, val2) in iter {
                func(val1, val2, ctx, &mut builder);
            }
            Value::Column(builder.build())
        }
    }
}
