/*// Copyright 2021 Datafuse Labs
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

use common_arrow::arrow::buffer::Buffer;
use common_expression::types::timestamptz::TimestampTzColumn;
use common_expression::types::timestamptz::TimestampTzDataType;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::types::Number;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::EvalContext;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionEval;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::Value;
use common_expression::ValueRef;
use num_traits::AsPrimitive;

// int float to decimal
pub fn register(registry: &mut FunctionRegistry) {
    let factory = |params: &[usize], args_type: &[DataType]| {
        if args_type.len() != 1 {
            return None;
        }
        if params.len() != 1 {
            return None;
        }
        if !matches!(
            args_type[0].remove_nullable(),
            DataType::Number(NumberDataType::Int64)
                | DataType::Date
                | DataType::String
                | DataType::Timestamp
                | DataType::TimestampTz(_)
        ) {
            return None;
        }

        let tz = params[0].clone() as String;
        let from_type = args_type[0].remove_nullable();
        let return_type = DataType::TimestampTz(TimestampTzDataType(tz));

        Some(Function {
            signature: FunctionSignature {
                name: "to_timestamptz".to_string(),
                args_type: vec![from_type.clone()],
                return_type: return_type.clone(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(move |args, ctx| {
                    convert_to_timestamptz(&args[0], ctx, from_type.clone(), return_type.clone())
                }),
            },
        })
    };

    registry.register_function_factory("to_timestamptz", move |params, args_type| {
        Some(Arc::new(factory(params, args_type)?))
    });
    registry.register_function_factory("to_timestamptz", move |params, args_type| {
        let f = factory(params, args_type)?;
        Some(Arc::new(f.wrap_nullable()))
    });
    registry.register_function_factory("try_to_timestamptz", move |params, args_type| {
        let mut f = factory(params, args_type)?;
        f.signature.name = "try_to_timestamptz".to_string();
        Some(Arc::new(f.error_to_null()))
    });
    registry.register_function_factory("try_to_timestamptz", move |params, args_type| {
        let mut f = factory(params, args_type)?;
        f.signature.name = "try_to_timestamptz".to_string();
        Some(Arc::new(f.error_to_null().wrap_nullable()))
    });
}

fn convert_to_timestamptz(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: DataType,
    dest_type: DataType,
) -> Value<AnyType> {
    match from_type {
        DataType::Number(NumberDataType::Int64) => {
            int64_to_timestamptz(arg, ctx, from_type, dest_type)
        }
        // DataType::Date => date_to_timestamptz(arg, ctx, from_type, dest_type),
        // DataType::Timestamp => timestamp_to_timestamptz(arg, ctx, from_type, dest_type),
        // DataType::String => string_to_timestamptz(arg, ctx, dest_type),
        _ => unreachable!("convert_to_timestamptz not support this DataType"),
    }
}

fn int64_to_timestamptz(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: DataType,
    dest_type: DataType,
) -> Value<AnyType> {
    let dest_type = dest_type.as_timestamptz().unwrap();
    let tz = dest_type.0;
    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &from_type);
            builder.build()
        }
    };

    let from_type = from_type.as_number().unwrap();
    let result = match from_type {
        NumberDataType::Int64 => {
            let column = NumberType::<i64>::try_downcast_column(&column).unwrap();
            TimestampTzColumn(column, tz)
        }
        _ => unreachable!(),
    };

    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(Scalar::TimestampTz(scalar, tz))
    } else {
        Value::Column(Column::TimestampTz(result))
    }
}
*/