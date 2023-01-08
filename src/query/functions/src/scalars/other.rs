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

use common_base::base::convert_byte_size;
use common_base::base::convert_number_size;
use common_expression::types::boolean::BooleanDomain;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::number::Float64Type;
use common_expression::types::number::UInt32Type;
use common_expression::types::number::UInt8Type;
use common_expression::types::number::F64;
use common_expression::types::ArgType;
use common_expression::types::DataType;
use common_expression::types::DateType;
use common_expression::types::GenericType;
use common_expression::types::NullType;
use common_expression::types::NullableType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::types::ValueType;
use common_expression::types::ALL_NUMERICS_TYPES;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::with_number_mapped_type;
use common_expression::Domain;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::Value;
use common_expression::ValueRef;
use ordered_float::OrderedFloat;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("inet_aton", &["ipv4_string_to_num"]);
    registry.register_aliases("try_inet_aton", &["try_ipv4_string_to_num"]);
    registry.register_aliases("inet_ntoa", &["ipv4_num_to_string"]);
    registry.register_aliases("try_inet_ntoa", &["try_ipv4_num_to_string"]);
    registry.register_aliases("assume_not_null", &["remove_nullable"]);

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

    registry.register_1_arg_core::<GenericType<0>, StringType, _, _>(
        "typeof",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        |_, ctx| Ok(Value::Scalar(ctx.generics[0].sql_name().into_bytes())),
    );

    registry.register_function_factory("ignore", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "ignore".to_string(),
                args_type: vec![DataType::Generic(0); args_type.len()],
                return_type: DataType::Boolean,
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| {
                FunctionDomain::Domain(Domain::Boolean(BooleanDomain {
                    has_true: false,
                    has_false: true,
                }))
            }),
            eval: Box::new(|_, _| Ok(Value::Scalar(Scalar::Boolean(false)))),
        }))
    });

    registry.register_1_arg_core::<NullableType<GenericType<0>>, GenericType<0>, _, _>(
        "assume_not_null",
        FunctionProperty::default(),
        |domain| {
            domain
                .value
                .as_ref()
                .map(|domain| FunctionDomain::Domain((**domain).clone()))
                .unwrap_or(FunctionDomain::Full)
        },
        |val, ctx| match val {
            ValueRef::Scalar(None) => Ok(Value::Scalar(ctx.generics[0].default_value())),
            ValueRef::Scalar(Some(scalar)) => Ok(Value::Scalar(scalar.to_owned())),
            ValueRef::Column(NullableColumn { column, .. }) => Ok(Value::Column(column)),
        },
    );

    registry.register_1_arg_core::<NullType, NullType, _, _>(
        "to_nullable",
        FunctionProperty::default(),
        |_| FunctionDomain::Domain(()),
        |val, _| Ok(val.to_owned()),
    );

    registry
        .register_1_arg_core::<NullableType<GenericType<0>>, NullableType<GenericType<0>>, _, _>(
            "to_nullable",
            FunctionProperty::default(),
            |domain| FunctionDomain::Domain(domain.clone()),
            |val, _| Ok(val.to_owned()),
        );

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

    register_run_diff(registry);
}

macro_rules! register_simple_domain_type_run_diff {
    ($registry:ident, $T:ty, $O:ty, $source_primitive_type:ty, $zero:expr) => {
        $registry.register_passthrough_nullable_1_arg::<$T, $O, _, _>(
            "running_difference",
            FunctionProperty::default(),
            |_| FunctionDomain::MayThrow,
            move |arg1, ctx| match arg1 {
                ValueRef::Scalar(_val) => {
                    let mut builder =
                        NumberType::<$source_primitive_type>::create_builder(1, ctx.generics);
                    builder.push($zero);
                    Ok(Value::Scalar(
                        NumberType::<$source_primitive_type>::build_scalar(builder),
                    ))
                }
                ValueRef::Column(col) => {
                    let a_iter = NumberType::<$source_primitive_type>::iter_column(&col);
                    let b_iter = NumberType::<$source_primitive_type>::iter_column(&col);
                    let size = col.len();
                    let mut builder = NumberType::<$source_primitive_type>::create_builder(
                        a_iter.size_hint().0,
                        ctx.generics,
                    );
                    builder.push($zero);
                    for (a, b) in a_iter.skip(1).zip(b_iter.take(size - 1)) {
                        let diff = a - b;
                        builder.push(diff);
                    }
                    Ok(Value::Column(
                        NumberType::<$source_primitive_type>::build_column(builder),
                    ))
                }
            },
        );
    };
}

fn register_run_diff(registry: &mut FunctionRegistry) {
    register_simple_domain_type_run_diff!(registry, NumberType<i64>, NumberType<i64>, i64, 0);
    register_simple_domain_type_run_diff!(registry, DateType, NumberType<i32>, i32, 0);
    register_simple_domain_type_run_diff!(registry, TimestampType, NumberType<i64>, i64, 0);
    register_simple_domain_type_run_diff!(
        registry,
        NumberType<F64>,
        NumberType<F64>,
        F64,
        OrderedFloat(0.0)
    );
}
