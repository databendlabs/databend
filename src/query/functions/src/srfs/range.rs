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

use std::sync::Arc;

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::Function;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionKind;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NormalizedSeries {
    pub start: i64,
    pub end: i64,
    pub step: i64,
}

fn infer_series_type(start: &DataType, end: &DataType) -> Option<DataType> {
    match (start.remove_nullable(), end.remove_nullable()) {
        (DataType::Number(_), DataType::Number(_)) => Some(DataType::Number(NumberDataType::Int64)),
        (DataType::Timestamp, DataType::Timestamp) => Some(DataType::Timestamp),
        (DataType::Date, DataType::Date) => Some(DataType::Date),
        _ => None,
    }
}

pub fn series_type_from_data_type(data_type: &DataType) -> Option<DataType> {
    match data_type.remove_nullable() {
        DataType::Number(_) => Some(DataType::Number(NumberDataType::Int64)),
        DataType::Timestamp => Some(DataType::Timestamp),
        DataType::Date => Some(DataType::Date),
        _ => None,
    }
}

pub fn normalize_series_params(
    series_type: &DataType,
    start: i64,
    mut end: i64,
    mut step: i64,
    inclusive: bool,
) -> Result<NormalizedSeries> {
    if matches!(series_type.remove_nullable(), DataType::Timestamp) {
        let abs_step = step.checked_abs().unwrap_or(i64::MAX);
        if abs_step < 1000 {
            step *= 1_000_000;
        } else if abs_step < 1_000_000 {
            step *= 1000;
        }
    }

    if step == 0 {
        return Err(ErrorCode::BadArguments("step must not be zero".to_string()));
    }

    if inclusive {
        if step > 0 {
            end += 1;
        } else {
            end -= 1;
        }
    }

    if step > 0 && start > end {
        return Err(ErrorCode::BadArguments(
            "start must be less than or equal to end when step is positive".to_string(),
        ));
    }
    if step < 0 && start < end {
        return Err(ErrorCode::BadArguments(
            "start must be greater than or equal to end when step is negative".to_string(),
        ));
    }

    Ok(NormalizedSeries { start, end, step })
}

pub fn register(registry: &mut FunctionRegistry) {
    registry.properties.insert(
        "generate_series".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );

    let generate_series = FunctionFactory::Closure(Box::new(|_, arg_types: &[DataType]| {
        build_series_function("generate_series", true, arg_types)
    }));
    registry.register_function_factory("generate_series", generate_series);
}

fn build_series_function(
    name: &str,
    inclusive: bool,
    arg_types: &[DataType],
) -> Option<Arc<Function>> {
    if !(2..=3).contains(&arg_types.len()) {
        return None;
    }

    let start_type = arg_types[0].remove_nullable();
    let end_type = arg_types[1].remove_nullable();
    let series_type = infer_series_type(&start_type, &end_type)?;

    if arg_types.len() == 3 {
        let step_type = arg_types[2].remove_nullable();
        if !matches!(step_type, DataType::Number(_)) {
            return None;
        }
    }

    let return_type = DataType::Tuple(vec![DataType::Nullable(Box::new(series_type.clone()))]);

    let args_type = arg_types.to_vec();
    Some(Arc::new(Function {
        signature: FunctionSignature {
            name: name.to_string(),
            args_type,
            return_type,
        },
        eval: FunctionEval::SRF {
            eval: Box::new(move |args, ctx, max_nums_per_row| {
                let start = args[0].clone().to_owned();
                let end = args[1].clone().to_owned();
                let step = args.get(2).map(|val| val.clone().to_owned());
                (0..ctx.num_rows)
                    .map(|row| {
                        build_series_row(
                            &series_type,
                            inclusive,
                            start.index(row).unwrap(),
                            end.index(row).unwrap(),
                            step.as_ref().map(|val| val.index(row).unwrap()),
                            row,
                            ctx,
                            max_nums_per_row,
                        )
                    })
                    .collect()
            }),
        },
    }))
}

fn build_series_row(
    series_type: &DataType,
    inclusive: bool,
    start: ScalarRef,
    end: ScalarRef,
    step: Option<ScalarRef>,
    row: usize,
    ctx: &mut databend_common_expression::EvalContext,
    max_nums_per_row: &mut [usize],
) -> (Value<AnyType>, usize) {
    if start.is_null() || end.is_null() || step.as_ref().is_some_and(|val| val.is_null()) {
        return (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0);
    }

    let step_i64 = match step {
        Some(ScalarRef::Number(num)) => match number_scalar_to_i64(num) {
            Some(val) => val,
            None => {
                ctx.set_error(row, "generate_series step must be an integer".to_string());
                return (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0);
            }
        },
        None => 1,
        _ => {
            ctx.set_error(row, "generate_series step must be a number".to_string());
            return (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0);
        }
    };

    let (start_i64, end_i64) = match series_type.remove_nullable() {
        DataType::Number(_) => match (start, end) {
            (ScalarRef::Number(start), ScalarRef::Number(end)) => {
                let start_i64 = match number_scalar_to_i64(start) {
                    Some(val) => val,
                    None => {
                        ctx.set_error(row, "generate_series start must be an integer".to_string());
                        return (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0);
                    }
                };
                let end_i64 = match number_scalar_to_i64(end) {
                    Some(val) => val,
                    None => {
                        ctx.set_error(row, "generate_series end must be an integer".to_string());
                        return (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0);
                    }
                };
                (start_i64, end_i64)
            }
            _ => {
                ctx.set_error(
                    row,
                    "generate_series expects numeric start and end".to_string(),
                );
                return (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0);
            }
        },
        DataType::Timestamp => match (start, end) {
            (ScalarRef::Timestamp(start), ScalarRef::Timestamp(end)) => (start, end),
            _ => {
                ctx.set_error(
                    row,
                    "generate_series expects timestamp start and end".to_string(),
                );
                return (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0);
            }
        },
        DataType::Date => match (start, end) {
            (ScalarRef::Date(start), ScalarRef::Date(end)) => (start as i64, end as i64),
            _ => {
                ctx.set_error(
                    row,
                    "generate_series expects date start and end".to_string(),
                );
                return (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0);
            }
        },
        _ => {
            ctx.set_error(
                row,
                "generate_series expects numeric/timestamp/date start and end".to_string(),
            );
            return (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0);
        }
    };

    let NormalizedSeries { start, end, step } =
        match normalize_series_params(series_type, start_i64, end_i64, step_i64, inclusive) {
            Ok(series) => series,
            Err(err) => {
                ctx.set_error(row, err.message().to_string());
                return (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0);
            }
        };

    let mut values = Vec::new();
    let mut current = start;
    if step > 0 {
        while current < end {
            values.push(current);
            current += step;
        }
    } else {
        while current > end {
            values.push(current);
            current += step;
        }
    }

    let len = values.len();
    max_nums_per_row[row] = std::cmp::max(max_nums_per_row[row], len);

    let column = match series_type.remove_nullable() {
        DataType::Number(_) => Int64Type::from_data(values),
        DataType::Timestamp => TimestampType::from_data(values),
        DataType::Date => {
            let values = values.into_iter().map(|val| val as i32).collect();
            DateType::from_data(values)
        }
        _ => unreachable!(),
    };
    let column = NullableColumn::new_column(column, Bitmap::new_constant(true, len));
    (Value::Column(Column::Tuple(vec![column])), len)
}

fn number_scalar_to_i64(num: NumberScalar) -> Option<i64> {
    match num {
        NumberScalar::Int8(val) => Some(val as i64),
        NumberScalar::Int16(val) => Some(val as i64),
        NumberScalar::Int32(val) => Some(val as i64),
        NumberScalar::Int64(val) => Some(val),
        NumberScalar::UInt8(val) => Some(val as i64),
        NumberScalar::UInt16(val) => Some(val as i64),
        NumberScalar::UInt32(val) => Some(val as i64),
        NumberScalar::UInt64(val) => i64::try_from(val).ok(),
        NumberScalar::Float32(val) => Some(val.into_inner() as i64),
        NumberScalar::Float64(val) => Some(val.into_inner() as i64),
    }
}
