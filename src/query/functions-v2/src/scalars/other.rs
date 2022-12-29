// Copyright 2022 Datafuse Labs.
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
use std::time::Duration;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_base::base::convert_byte_size;
use common_base::base::convert_number_size;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::number::Float64Type;
use common_expression::types::number::UInt8Type;
use common_expression::types::DataType;
use common_expression::types::StringType;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::Column;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::Value;
use common_expression::ValueRef;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<Float64Type, StringType, _, _>(
        "humanize_size",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<Float64Type, StringType>(move |val, output, _| {
            let new_val = convert_byte_size(val.into());
            output.put_str(&new_val);
            output.commit_row();
            Ok(())
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Float64Type, StringType, _, _>(
        "humanize_number",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<Float64Type, StringType>(move |val, output, _| {
            let new_val = convert_number_size(val.into());
            output.put_str(&new_val);
            output.commit_row();
            Ok(())
        }),
    );

    registry.register_1_arg_core::<Float64Type, UInt8Type, _, _>(
        "sleep",
        FunctionProperty::default(),
        |_| FunctionDomain::MayThrow,
        |x, _| match x {
            common_expression::ValueRef::Scalar(x) => {
                let duration = Duration::try_from_secs_f64(x.into()).map_err(|x| x.to_string())?;
                if duration.gt(&Duration::from_secs(300)) {
                    return Err(format!(
                        "The maximum sleep time is 300 seconds. Requested: {:?}",
                        duration
                    ));
                };
                std::thread::sleep(duration);
                Ok(Value::Scalar(1u8))
            }
            common_expression::ValueRef::Column(_) => Err("Must be constant argument".to_string()),
        },
    );

    registry.register_function_factory("ignore", |_, _arg_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "ignore".to_string(),
                args_type: vec![DataType::Generic(0)],
                return_type: DataType::Boolean,
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_args_domain| FunctionDomain::Full),
            eval: Box::new(|_args, _ctx| Ok(Value::Scalar(Scalar::Boolean(false)))),
        }))
    });

    registry.register_function_factory("assume_not_null", |_, _arg_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "assume_not_null".to_string(),
                args_type: vec![DataType::Generic(0)],
                return_type: DataType::Generic(0),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain| FunctionDomain::Domain(args_domain[0].clone())),
            eval: Box::new(|args, _ctx| match &args[0] {
                ValueRef::Column(Column::Null { .. }) | ValueRef::Column(Column::Nullable(_)) => {
                    Err("assume_not_null got null argument".to_string())
                }
                ValueRef::Scalar(x) if x.is_null() => {
                    Err("assume_not_null got null argument".to_string())
                }
                other => Ok(other.clone().to_owned()),
            }),
        }))
    });

    registry.register_function_factory("to_nullable", |_, _arg_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "to_nullable".to_string(),
                args_type: vec![DataType::Generic(0)],
                return_type: DataType::Generic(0).wrap_nullable(),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain| FunctionDomain::Domain(args_domain[0].clone())),
            eval: Box::new(|args, _ctx| match &args[0] {
                x @ ValueRef::Scalar(_) => Ok(x.clone().to_owned()),
                x @ ValueRef::Column(Column::Null { .. })
                | x @ ValueRef::Column(Column::Nullable(_)) => Ok(x.clone().to_owned()),
                _x @ ValueRef::Column(c) => {
                    let mut m = MutableBitmap::with_capacity(c.len());
                    m.extend_constant(c.len(), true);
                    let ret = Column::Nullable(Box::new(NullableColumn {
                        column: c.clone(),
                        validity: m.into(),
                    }));

                    Ok(Value::Column(ret))
                }
            }),
        }))
    });
}
