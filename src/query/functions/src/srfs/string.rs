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

use std::borrow::Cow;
use std::sync::Arc;

use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ValueType;
use databend_common_expression::Column;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionKind;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use regex::RegexBuilder;

pub fn register(registry: &mut FunctionRegistry) {
    registry.properties.insert(
        "regexp_split_to_table".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );

    let regexp_split_to_table = FunctionFactory::Closure(Box::new(|_, arg_types: &[DataType]| {
        match arg_types {
            [ty1, ty2, ty3]
                if is_string_like(ty1) && is_string_like(ty2) && is_string_like(ty3) =>
            {
                Some(build_regexp_split_to_table(ty1, ty2, Some(ty3)))
            }
            [ty1, ty2] if is_string_like(ty1) && is_string_like(ty2) => {
                Some(build_regexp_split_to_table(ty1, ty2, None))
            }
            _ => {
                // Generate a fake function with signature `unset(Array(T0 NULL))` to have a better error message.
                Some(build_regexp_split_to_table(
                    &DataType::Array(Box::new(DataType::Boolean)),
                    &DataType::Array(Box::new(DataType::Boolean)),
                    None,
                ))
            }
        }
    }));
    registry.register_function_factory("regexp_split_to_table", regexp_split_to_table);
}

fn is_string_like(dt: &DataType) -> bool {
    matches!(dt, DataType::String | DataType::Nullable(box DataType::String))
}

pub fn regexp_split_to_table(
    text: &str,
    mut pattern: &str,
    flags_str: Option<&str>,
    ctx: &mut EvalContext,
) -> Vec<String> {
    let mut literal_mode_from_prefix = false;
    if pattern.starts_with("***=") {
        pattern = &pattern[4..];
        literal_mode_from_prefix = true;
    } else if pattern.starts_with("***:") {
        pattern = &pattern[4..];
    }
    let mut literal_mode_from_flags = false;
    let mut builder_config_fns: Vec<Box<dyn FnOnce(&mut RegexBuilder) -> &mut RegexBuilder>> =
        Vec::new();

    if let Some(flags) = flags_str {
        for flag_char in flags.chars() {
            match flag_char {
                'i' => builder_config_fns.push(Box::new(|b| b.case_insensitive(true))),
                'c' => builder_config_fns.push(Box::new(|b| b.case_insensitive(false))),
                'n' | 'm' => {
                    builder_config_fns.push(Box::new(|b| b.dot_matches_new_line(false)));
                    builder_config_fns.push(Box::new(|b| b.multi_line(true)));
                }
                's' => {
                    builder_config_fns.push(Box::new(|b| b.dot_matches_new_line(true)));
                    builder_config_fns.push(Box::new(|b| b.multi_line(false)));
                }
                'x' => builder_config_fns.push(Box::new(|b| b.ignore_whitespace(true))),
                'q' => {
                    literal_mode_from_flags = true;
                } // The 'q' flag indicates that the pattern is a literal string.
                _ => {
                    ctx.set_error(
                        0,
                        format!("Unsupported or unrecognized flag: '{}'", flag_char),
                    );
                    return vec![];
                }
            }
        }
    }

    let final_literal_mode = literal_mode_from_prefix || literal_mode_from_flags;

    let final_pattern: Cow<str> = if final_literal_mode {
        Cow::Owned(regex::escape(pattern))
    } else {
        Cow::Borrowed(pattern)
    };

    let mut builder = RegexBuilder::new(&final_pattern);
    for config_fn in builder_config_fns {
        config_fn(&mut builder);
    }

    match builder.build() {
        Ok(re) => {
            let mut result = Vec::new();
            let mut current_segment_start_idx = 0;
            let mut last_delimiter_end_idx = 0;

            for m in re.find_iter(text) {
                let match_start = m.start();
                let match_end = m.end();
                let is_zero_length_match = match_start == match_end;

                // PostgreSQL rule: Ignore zero-length matches occurring at the start/end of the string
                // or immediately after a previous match. This means these zero-length matches do not
                // act as delimiters.
                if is_zero_length_match
                    && (match_start == 0
                        || match_start == text.len()
                        || match_start == last_delimiter_end_idx)
                {
                    continue;
                }

                if match_start > current_segment_start_idx
                    || (match_start == current_segment_start_idx && is_zero_length_match)
                {
                    result.push(text[current_segment_start_idx..match_start].to_string());
                }

                current_segment_start_idx = match_end;
                last_delimiter_end_idx = match_end;
            }

            if current_segment_start_idx <= text.len() {
                result.push(text[current_segment_start_idx..].to_string());
            }
            if result.is_empty() && !text.is_empty() {
                return vec![text.to_string()];
            }

            result
        }
        Err(e) => {
            ctx.set_error(0, format!("Failed to compile regex: {}", e));
            vec![]
        }
    }
}

fn build_regexp_split_to_table(
    arg_type: &DataType,
    arg2_type: &DataType,
    arg3_type: Option<&DataType>,
) -> Arc<Function> {
    match (arg_type, arg2_type, arg3_type) {
        (a1, a2, Some(a3)) if is_string_like(a1) && is_string_like(a2) && is_string_like(a3) => {
            Arc::new(Function {
                signature: FunctionSignature {
                    name: "regexp_split_to_table".to_string(),
                    args_type: vec![
                        arg_type.clone(),
                        arg2_type.clone(),
                        arg3_type.unwrap().clone(),
                    ],
                    return_type: DataType::Tuple(vec![DataType::String]),
                },
                eval: FunctionEval::SRF {
                    eval: Box::new(|args, ctx, max_nums_per_row| {
                        let arg = args[0].clone().to_owned();
                        let delimiter = args[1].clone().to_owned();
                        let flag = args[2].clone().to_owned();
                        let res = (0..ctx.num_rows)
                            .map(|row| {
                                match (
                                    arg.index(row).unwrap(),
                                    delimiter.index(row).unwrap(),
                                    flag.index(row).unwrap(),
                                ) {
                                    (
                                        ScalarRef::String(text),
                                        ScalarRef::String(pattern),
                                        ScalarRef::String(flag),
                                    ) => {
                                        let res =
                                            regexp_split_to_table(text, pattern, Some(flag), ctx);
                                        let mut builder =
                                            StringColumnBuilder::with_capacity(res.len());
                                        for v in res {
                                            builder.put_and_commit(v.as_str());
                                        }
                                        let col = builder.build();
                                        let col = StringType::upcast_column(col);
                                        let len = col.len();
                                        max_nums_per_row[row] =
                                            std::cmp::max(max_nums_per_row[row], len);
                                        (Value::Column(Column::Tuple(vec![col])), len)
                                    }
                                    _ => unreachable!(),
                                }
                            })
                            .collect();
                        res
                    }),
                },
            })
        }
        (a1, a2, None) if is_string_like(a1) && is_string_like(a2) => Arc::new(Function {
            signature: FunctionSignature {
                name: "regexp_split_to_table".to_string(),
                args_type: vec![arg_type.clone(), arg2_type.clone()],
                return_type: DataType::Tuple(vec![DataType::String]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(|args, ctx, max_nums_per_row| {
                    let arg = args[0].clone().to_owned();
                    let delimiter = args[1].clone().to_owned();
                    let res = (0..ctx.num_rows)
                        .map(
                            |row| match (arg.index(row).unwrap(), delimiter.index(row).unwrap()) {
                                (ScalarRef::String(text), ScalarRef::String(pattern)) => {
                                    let res = regexp_split_to_table(text, pattern, None, ctx);
                                    let mut builder = StringColumnBuilder::with_capacity(res.len());
                                    for v in res {
                                        builder.put_and_commit(v.as_str());
                                    }

                                    let col = builder.build();
                                    let col = StringType::upcast_column(col);
                                    let len = col.len();
                                    max_nums_per_row[row] =
                                        std::cmp::max(max_nums_per_row[row], len);
                                    (Value::Column(Column::Tuple(vec![col])), len)
                                }
                                _ => unreachable!(),
                            },
                        )
                        .collect();
                    res
                }),
            },
        }),
        _ => unreachable!(),
    }
}
