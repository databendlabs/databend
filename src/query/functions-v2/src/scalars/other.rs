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

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_base::base::convert_byte_size;
use common_base::base::convert_number_size;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::number::Float64Type;
use common_expression::types::number::UInt32Type;
use common_expression::types::number::UInt8Type;
use common_expression::types::DataType;
use common_expression::types::NullableType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::ALL_NUMERICS_TYPES;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::with_number_mapped_type;
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
    registry.register_aliases("inet_aton", &["ipv4_string_to_num"]);
    registry.register_aliases("try_inet_aton", &["try_ipv4_string_to_num"]);
    registry.register_aliases("inet_ntoa", &["ipv4_num_to_string"]);
    registry.register_aliases("try_inet_ntoa", &["try_ipv4_num_to_string"]);

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

    registry.register_function_factory("typeof", |_, _arg_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "typeof".to_string(),
                args_type: vec![DataType::Generic(0)],
                return_type: DataType::String,
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_args_domain| FunctionDomain::Full),
            eval: Box::new(|_args, ctx| {
                Ok(Value::Scalar(Scalar::String(
                    ctx.generics[0].sql_name().into_bytes(),
                )))
            }),
        }))
    });

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

    registry.register_aliases("assume_not_null", &["remove_nullable"]);
    registry.register_function_factory("assume_not_null", |_, _arg_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "assume_not_null".to_string(),
                args_type: vec![DataType::Nullable(Box::new(DataType::Generic(0)))],
                return_type: DataType::Generic(0),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain| {
                let domain = match &args_domain[0] {
                    common_expression::Domain::Nullable(c) => match &c.value {
                        Some(d) => *d.clone(),
                        None => return FunctionDomain::Full,
                    },
                    other => other.clone(),
                };
                FunctionDomain::Domain(domain)
            }),
            eval: Box::new(|args, ctx| match &args[0] {
                ValueRef::Scalar(x) if x.is_null() => {
                    Ok(Value::Scalar(ctx.generics[0].default_value()))
                }
                ValueRef::Column(Column::Nullable(c)) => Ok(Value::Column(c.column.clone())),
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

    registry.register_passthrough_nullable_1_arg::<StringType, UInt32Type, _, _>(
        "inet_aton",
        FunctionProperty::default(),
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, UInt32Type>(|v, output, _| {
            let addr_str = String::from_utf8_lossy(v);
            let addr = addr_str.parse::<Ipv4Addr>().map_err(|err| {
                format!(
                    "Failed to parse '{}' into a IPV4 address, {}",
                    addr_str, err
                )
            })?;
            let addr_binary = u32::from(addr);
            output.push(addr_binary);
            Ok(())
        }),
    );

    registry.register_combine_nullable_1_arg::<StringType, UInt32Type, _, _>(
        "try_inet_aton",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, NullableType<UInt32Type>>(|v, output, _| {
            let addr_str = String::from_utf8_lossy(v);
            match addr_str.parse::<Ipv4Addr>() {
                Ok(addr) => {
                    let addr_binary = u32::from(addr);
                    output.push(addr_binary);
                }
                Err(_) => output.push_null(),
            }
            Ok(())
        }),
    );

    for num_ty in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match num_ty {
            NumberDataType::NUM_TYPE => {
                registry
                    .register_passthrough_nullable_1_arg::<NumberType<NUM_TYPE>, StringType, _, _>(
                        "inet_ntoa",
                        FunctionProperty::default(),
                        |_| FunctionDomain::MayThrow,
                        vectorize_with_builder_1_arg::<NumberType<NUM_TYPE>, StringType>(
                            |v, output, _| {
                                match num_traits::cast::cast::<NUM_TYPE, u32>(v) {
                                    Some(val) => {
                                        let addr_str =
                                            Ipv4Addr::from(val.to_be_bytes()).to_string();
                                        output.put_str(&addr_str);
                                        output.commit_row();
                                    }
                                    None => {
                                        return Err(format!(
                                            "Failed to parse '{}' into a IPV4 address",
                                            v
                                        ));
                                    }
                                }
                                Ok(())
                            },
                        ),
                    );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match num_ty {
            NumberDataType::NUM_TYPE => {
                registry.register_combine_nullable_1_arg::<NumberType<NUM_TYPE>, StringType, _, _>(
                    "try_inet_ntoa",
                    FunctionProperty::default(),
                    |_| FunctionDomain::Full,
                    vectorize_with_builder_1_arg::<NumberType<NUM_TYPE>, NullableType<StringType>>(
                        |v, output, _| {
                            match num_traits::cast::cast::<NUM_TYPE, u32>(v) {
                                Some(val) => {
                                    let addr_str = Ipv4Addr::from(val.to_be_bytes()).to_string();
                                    output.validity.push(true);
                                    output.builder.put_str(&addr_str);
                                    output.builder.commit_row();
                                }
                                None => output.push_null(),
                            }
                            Ok(())
                        },
                    ),
                );
            }
        });
    }
}
