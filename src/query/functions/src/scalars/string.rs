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

use base64::engine::general_purpose;
use base64::prelude::*;
use bstr::ByteSlice;
use common_expression::types::number::SimpleDomain;
use common_expression::types::number::UInt64Type;
use common_expression::types::string::StringColumn;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::ArrayType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::vectorize_with_builder_2_arg;
use common_expression::vectorize_with_builder_3_arg;
use common_expression::vectorize_with_builder_4_arg;
use common_expression::EvalContext;
use common_expression::FunctionDomain;
use common_expression::FunctionRegistry;
use common_expression::Value;
use common_expression::ValueRef;
use itertools::izip;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("to_string", &["to_varchar", "to_text"]);
    registry.register_aliases("upper", &["ucase"]);
    registry.register_aliases("lower", &["lcase"]);
    registry.register_aliases("length", &["octet_length"]);
    registry.register_aliases("char_length", &["character_length", "length_utf8"]);
    registry.register_aliases("substr", &["substring", "mid"]);
    registry.register_aliases("substr_utf8", &["substring_utf8"]);

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "upper",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| col.data().len(),
            |val, output, _| {
                for (start, end, ch) in val.char_indices() {
                    if ch == '\u{FFFD}' {
                        // If char is invalid, just copy it.
                        output.put_slice(&val.as_bytes()[start..end]);
                    } else if ch.is_ascii() {
                        output.put_u8(ch.to_ascii_uppercase() as u8);
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
                for (start, end, ch) in val.char_indices() {
                    if ch == '\u{FFFD}' {
                        // If char is invalid, just copy it.
                        output.put_slice(&val.as_bytes()[start..end]);
                    } else if ch.is_ascii() {
                        output.put_u8(ch.to_ascii_lowercase() as u8);
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
        "length",
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

    registry.register_passthrough_nullable_1_arg::<StringType, NumberType<u64>, _, _>(
        "char_length",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, NumberType<u64>>(|s, output, ctx| {
            match std::str::from_utf8(s) {
                Ok(s) => {
                    output.push(s.chars().count() as u64);
                }
                Err(err) => {
                    ctx.set_error(output.len(), err.to_string());
                    output.push(0);
                }
            }
        }),
    );

    const MAX_PADDING_LENGTH: usize = 1000000;
    registry.register_passthrough_nullable_3_arg::<StringType, NumberType<u64>, StringType, StringType, _, _>(
        "lpad",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<StringType, NumberType<u64>, StringType, StringType>(
            |s, pad_len, pad, output, ctx| {
                let pad_len = pad_len as usize;
                if pad_len > MAX_PADDING_LENGTH {
                    ctx.set_error(output.len(), format!("padding length '{}' is too big, max is: '{}'", pad_len, MAX_PADDING_LENGTH));
                } else if pad_len <= s.len() {
                    output.put_slice(&s[..pad_len]);
                } else if pad.is_empty() {
                    ctx.set_error(output.len(), format!("can't fill the '{}' length to '{}' with an empty pad string", String::from_utf8_lossy(s), pad_len));
                } else {
                    let mut remain_pad_len = pad_len - s.len();
                    while remain_pad_len > 0 {
                        if remain_pad_len < pad.len() {
                            output.put_slice(&pad[..remain_pad_len]);
                            remain_pad_len = 0;
                        } else {
                            output.put_slice(pad);
                            remain_pad_len -= pad.len();
                        }
                    }
                    output.put_slice(s);
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
                if pos < 1 || pos > srcstr.len() {
                    output.put_slice(srcstr);
                } else {
                    let pos = pos - 1;
                    output.put_slice(&srcstr[0..pos]);
                    output.put_slice(substr);
                    if pos + len < srcstr.len() {
                        output.put_slice(&srcstr[(pos + len)..]);
                    }
                }
                output.commit_row();
            }),
    );

    registry.register_passthrough_nullable_3_arg::<StringType, NumberType<u64>, StringType, StringType, _, _>(
        "rpad",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<StringType, NumberType<u64>, StringType, StringType>(
        |s: &[u8], pad_len: u64, pad: &[u8], output, ctx| {
            let pad_len = pad_len as usize;
            if pad_len > MAX_PADDING_LENGTH {
                ctx.set_error(output.len(), format!("padding length '{}' is too big, max is: '{}'", pad_len, MAX_PADDING_LENGTH));
            } else if pad_len <= s.len() {
                output.put_slice(&s[..pad_len])
            } else if pad.is_empty() {
                ctx.set_error(output.len(), format!("can't fill the '{}' length to '{}' with an empty pad string", String::from_utf8_lossy(s), pad_len));
            } else {
                output.put_slice(s);
                let mut remain_pad_len = pad_len - s.len();
                while remain_pad_len > 0 {
                    if remain_pad_len < pad.len() {
                        output.put_slice(&pad[..remain_pad_len]);
                        remain_pad_len = 0;
                    } else {
                        output.put_slice(pad);
                        remain_pad_len -= pad.len();
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
                output.put_slice(str);
                output.commit_row();
                return;
            }
            let mut skip = 0;
            for (p, w) in str.windows(from.len()).enumerate() {
                if w == from {
                    output.put_slice(to);
                    skip = from.len();
                } else if p + w.len() == str.len() {
                    output.put_slice(w);
                } else if skip > 1 {
                    skip -= 1;
                } else {
                    output.put_slice(&w[0..1]);
                }
            }
            output.commit_row();
        }),
    );

    registry.register_2_arg::<StringType, StringType, NumberType<i8>, _, _>(
        "strcmp",
        |_, _, _| FunctionDomain::Full,
        |s1, s2, _| {
            let res = match s1.len().cmp(&s2.len()) {
                Ordering::Equal => {
                    let mut res = Ordering::Equal;
                    for (s1i, s2i) in izip!(s1, s2) {
                        match s1i.cmp(s2i) {
                            Ordering::Equal => continue,
                            ord => {
                                res = ord;
                                break;
                            }
                        }
                    }
                    res
                }
                ord => ord,
            };
            match res {
                Ordering::Equal => 0,
                Ordering::Greater => 1,
                Ordering::Less => -1,
            }
        },
    );

    let find_at = |str: &[u8], substr: &[u8], pos: u64| {
        if substr.is_empty() {
            // the same behavior as MySQL, Postgres and Clickhouse
            return if pos == 0 { 1_u64 } else { pos };
        }

        let pos = pos as usize;
        if pos == 0 {
            return 0_u64;
        }
        let p = pos - 1;
        if p + substr.len() <= str.len() {
            str[p..]
                .windows(substr.len())
                .position(|w| w == substr)
                .map_or(0, |i| i + 1 + p) as u64
        } else {
            0_u64
        }
    };
    registry.register_2_arg::<StringType, StringType, NumberType<u64>, _, _>(
        "instr",
        |_, _, _| FunctionDomain::Full,
        move |str: &[u8], substr: &[u8], _| find_at(str, substr, 1),
    );

    registry.register_2_arg::<StringType, StringType, NumberType<u64>, _, _>(
        "position",
        |_, _, _| FunctionDomain::Full,
        move |substr: &[u8], str: &[u8], _| find_at(str, substr, 1),
    );

    registry.register_2_arg::<StringType, StringType, NumberType<u64>, _, _>(
        "locate",
        |_, _, _| FunctionDomain::Full,
        move |substr: &[u8], str: &[u8], _| find_at(str, substr, 1),
    );

    registry.register_3_arg::<StringType, StringType, NumberType<u64>, NumberType<u64>, _, _>(
        "locate",
        |_, _, _, _| FunctionDomain::Full,
        move |substr: &[u8], str: &[u8], pos: u64, _| find_at(str, substr, pos),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "to_base64",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| col.data().len() * 4 / 3 + col.len() * 4,
            |val, output, _| {
                base64::write::EncoderWriter::new(&mut output.data, &general_purpose::STANDARD)
                    .write_all(val)
                    .unwrap();
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "from_base64",
        |_, _| FunctionDomain::MayThrow,
        vectorize_string_to_string(
            |col| col.data().len() * 4 / 3 + col.len() * 4,
            |val, output, ctx| {
                if let Err(err) = general_purpose::STANDARD.decode_vec(val, &mut output.data) {
                    ctx.set_error(output.len(), err.to_string());
                }
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "quote",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| col.data().len() * 2,
            |val, output, _| {
                for ch in val {
                    match ch {
                        0 => output.put_slice(&[b'\\', b'0']),
                        b'\'' => output.put_slice(&[b'\\', b'\'']),
                        b'\"' => output.put_slice(&[b'\\', b'\"']),
                        8 => output.put_slice(&[b'\\', b'b']),
                        b'\n' => output.put_slice(&[b'\\', b'n']),
                        b'\r' => output.put_slice(&[b'\\', b'r']),
                        b'\t' => output.put_slice(&[b'\\', b't']),
                        b'\\' => output.put_slice(&[b'\\', b'\\']),
                        c => output.put_u8(*c),
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
                let start = output.data.len();
                output.put_slice(val);
                let buf = &mut output.data[start..];
                buf.reverse();
                output.commit_row();
            },
        ),
    );

    registry.register_1_arg::<StringType, NumberType<u8>, _, _>(
        "ascii",
        |_, domain| {
            FunctionDomain::Domain(SimpleDomain {
                min: domain.min.first().cloned().unwrap_or(0),
                max: domain
                    .max
                    .as_ref()
                    .map_or(u8::MAX, |v| v.first().cloned().unwrap_or_default()),
            })
        },
        |val, _| val.first().cloned().unwrap_or_default(),
    );

    // Trim functions
    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "ltrim",
        |_, _| FunctionDomain::Full,
        vectorize_string_to_string(
            |col| col.data().len(),
            |val, output, _| {
                let pos = val.iter().position(|ch| *ch != b' ' && *ch != b'\t');
                if let Some(idx) = pos {
                    output.put_slice(&val.as_bytes()[idx..]);
                }
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
                let pos = val.iter().rev().position(|ch| *ch != b' ' && *ch != b'\t');
                if let Some(idx) = pos {
                    output.put_slice(&val.as_bytes()[..val.len() - idx]);
                }
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
                let start_pos = val.iter().position(|ch| *ch != b' ' && *ch != b'\t');
                let end_pos = val.iter().rev().position(|ch| *ch != b' ' && *ch != b'\t');
                if let (Some(start_idx), Some(end_idx)) = (start_pos, end_pos) {
                    output.put_slice(&val.as_bytes()[start_idx..val.len() - end_idx]);
                }
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
                let chunk_size = trim_str.len();
                if chunk_size == 0 {
                    output.put_slice(val);
                    output.commit_row();
                    return;
                }

                let pos = val.chunks(chunk_size).position(|chunk| chunk != trim_str);
                if let Some(idx) = pos {
                    output.put_slice(&val.as_bytes()[idx * chunk_size..]);
                }
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
                let chunk_size = trim_str.len();
                if chunk_size == 0 {
                    output.put_slice(val);
                    output.commit_row();
                    return;
                }

                let pos = val.rchunks(chunk_size).position(|chunk| chunk != trim_str);
                if let Some(idx) = pos {
                    output.put_slice(&val.as_bytes()[..val.len() - idx * chunk_size]);
                }
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
                let chunk_size = trim_str.len();
                if chunk_size == 0 {
                    output.put_slice(val);
                    output.commit_row();
                    return;
                }

                let start_pos = val.chunks(chunk_size).position(|chunk| chunk != trim_str);

                // Trim all
                if start_pos.is_none() {
                    output.commit_row();
                    return;
                }

                let end_pos = val.rchunks(chunk_size).position(|chunk| chunk != trim_str);

                if let (Some(start_idx), Some(end_idx)) = (start_pos, end_pos) {
                    output.put_slice(
                        &val.as_bytes()[start_idx * chunk_size..val.len() - end_idx * chunk_size],
                    );
                }

                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "hex",
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
        "hex",
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
                    (0..times).for_each(|_| output.put_slice(a));
                }
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "unhex",
        |_, _| FunctionDomain::MayThrow,
        vectorize_string_to_string(
            |col| col.data().len() / 2,
            |val, output, ctx| {
                let old_len = output.data.len();
                let extra_len = val.len() / 2;
                output.data.resize(old_len + extra_len, 0);
                if let Err(err) = hex::decode_to_slice(val, &mut output.data[old_len..]) {
                    ctx.set_error(output.len(), err.to_string());
                }
                output.commit_row();
            },
        ),
    );

    registry.register_1_arg::<StringType, UInt64Type, _, _>(
        "ord",
        |_, _| FunctionDomain::Full,
        |str: &[u8], _| {
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

    const SPACE: u8 = 0x20;
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
                    Value::Scalar(vec![])
                } else {
                    Value::Scalar(vec![SPACE; times as usize])
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
                    data: vec![SPACE; total_space as usize],
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
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, NumberType<u64>, StringType>(
            |s, n, output, _| {
                let n = n as usize;
                if n < s.len() {
                    output.put_slice(&s[0..n]);
                } else {
                    output.put_slice(s);
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
                if n < s.len() {
                    output.put_slice(&s[s.len() - n..]);
                } else {
                    output.put_slice(s);
                }
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, NumberType<i64>, StringType, _, _>(
        "substr",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, NumberType<i64>, StringType>(
            |s, pos, output, _| {
                output.put_slice(substr(s, pos, s.len() as u64));
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_3_arg::<StringType, NumberType<i64>, NumberType<u64>, StringType, _, _>(
        "substr",
        |_, _, _, _| FunctionDomain::Full,
        vectorize_with_builder_3_arg::<StringType, NumberType<i64>, NumberType<u64>, StringType>(|s, pos, len, output, _| {
            output.put_slice(substr(s, pos, len));
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, NumberType<i64>, StringType, _, _>(
        "substr_utf8",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, NumberType<i64>, StringType>(
            |s, pos, output, ctx| match std::str::from_utf8(s) {
                Ok(s) => substr_utf8(output, s, pos, s.len() as u64),
                Err(e) => {
                    ctx.set_error(output.len(), e.to_string());
                    output.commit_row();
                }
            },
        ),
    );

    registry.register_passthrough_nullable_3_arg::<StringType, NumberType<i64>, NumberType<u64>, StringType, _, _>(
        "substr_utf8",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<StringType, NumberType<i64>, NumberType<u64>, StringType>(|s, pos, len, output, ctx| {
            match std::str::from_utf8(s)  {
                Ok(s) => substr_utf8(output, s, pos, len),
                Err(e) =>  {
                    ctx.set_error(output.len(), e.to_string());
                    output.commit_row();
                },
            }
        }),
    );

    registry
        .register_passthrough_nullable_2_arg::<StringType, StringType, ArrayType<StringType>, _, _>(
            "split",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<StringType, StringType, ArrayType<StringType>>(
                |str, sep, output, ctx| match std::str::from_utf8(str) {
                    Ok(s) => match std::str::from_utf8(sep) {
                        Ok(sep) => {
                            if s == sep {
                                output.builder.put_slice(&[]);
                                output.builder.commit_row();
                            } else if sep.is_empty() {
                                output.builder.put_slice(str);
                                output.builder.commit_row();
                            } else {
                                let split = s.split(&sep);
                                for i in split {
                                    output.builder.put_slice(i.as_bytes());
                                    output.builder.commit_row();
                                }
                            }
                            output.commit_row()
                        }
                        Err(e) => {
                            ctx.set_error(output.len(), e.to_string());
                            output.commit_row();
                        }
                    },
                    Err(e) => {
                        ctx.set_error(output.len(), e.to_string());
                        output.commit_row();
                    }
                },
            ),
        );

    registry
        .register_passthrough_nullable_3_arg::<StringType, StringType, NumberType<i64>, StringType, _, _>(
            "split_part",
            |_, _, _, _| FunctionDomain::Full,
            vectorize_with_builder_3_arg::<StringType, StringType, NumberType<i64>, StringType>(
                |str, sep, part, output, ctx| match std::str::from_utf8(str) {
                    Ok(s) => match std::str::from_utf8(sep) {
                        Ok(sep) => {
                            if s == sep {
                                output.commit_row()
                            } else if sep.is_empty() {
                                if part == 0 || part == 1 || part == -1 {
                                    output.put_slice(str);
                                }
                                output.commit_row()
                            } else {
                                if part < 0 {
                                    let split = s.rsplit(&sep);
                                    let idx = (-part-1) as usize;
                                    for (count, i) in split.enumerate() {
                                        if idx == count {
                                            output.put_slice(i.as_bytes());
                                            break
                                        }
                                    }
                                } else {
                                    let split = s.split(&sep);
                                    let idx = if part == 0 {
                                        0usize
                                    } else {
                                        (part - 1) as usize
                                    };
                                    for (count, i) in split.enumerate() {
                                        if idx == count {
                                            output.put_slice(i.as_bytes());
                                            break
                                        }
                                    }
                                }
                                output.commit_row();
                            }
                        }
                        Err(e) => {
                            ctx.set_error(output.len(), e.to_string());
                            output.commit_row();
                        }
                    },
                    Err(e) => {
                        ctx.set_error(output.len(), e.to_string());
                        output.commit_row();
                    }
                },
            ),
        )
}

pub(crate) mod soundex {
    use common_expression::types::string::StringColumnBuilder;
    use common_expression::EvalContext;

    pub fn soundex(val: &[u8], output: &mut StringColumnBuilder, _eval_context: &mut EvalContext) {
        let mut last = None;
        let mut count = 0;

        for ch in String::from_utf8_lossy(val).chars() {
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
fn substr(str: &[u8], pos: i64, len: u64) -> &[u8] {
    if pos > 0 && pos <= str.len() as i64 {
        let l = str.len();
        let s = (pos - 1) as usize;
        let mut e = len as usize + s;
        if e > l {
            e = l;
        }
        return &str[s..e];
    }
    if pos < 0 && -(pos) <= str.len() as i64 {
        let l = str.len();
        let s = l - -pos as usize;
        let mut e = len as usize + s;
        if e > l {
            e = l;
        }
        return &str[s..e];
    }
    &str[0..0]
}

#[inline]
fn substr_utf8(builder: &mut StringColumnBuilder, str: &str, pos: i64, len: u64) {
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
    func: impl Fn(&[u8], &mut StringColumnBuilder, &mut EvalContext) + Copy,
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
    func: impl Fn(&[u8], &[u8], &mut EvalContext, &mut StringColumnBuilder) + Copy,
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
