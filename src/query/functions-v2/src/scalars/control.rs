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

use common_expression::types::boolean::BooleanDomain;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::nullable::NullableDomain;
use common_expression::types::number::SimpleDomain;
use common_expression::types::ArgType;
use common_expression::types::ArrayType;
use common_expression::types::BooleanType;
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
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Domain;
use common_expression::Function;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::ScalarRef;
use common_expression::Value;
use common_expression::ValueRef;
use common_hashtable::HashtableKeyable;
use common_hashtable::KeysRef;
use common_hashtable::StackHashSet;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_function_factory("multi_if", |_, args_type| {
        if args_type.len() < 3 || args_type.len() % 2 == 0 {
            return None;
        }
        let sig_args_type = (0..(args_type.len() - 1) / 2)
            .flat_map(|_| {
                [
                    DataType::Nullable(Box::new(DataType::Boolean)),
                    DataType::Generic(0),
                ]
            })
            .chain([DataType::Generic(0)])
            .collect();

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "multi_if".to_string(),
                args_type: sig_args_type,
                return_type: DataType::Generic(0),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain| {
                let mut domain = None;
                for cond_idx in (0..args_domain.len() - 1).step_by(2) {
                    let (has_true, has_null_or_false) = match &args_domain[cond_idx] {
                        Domain::Nullable(NullableDomain {
                            has_null,
                            value:
                                Some(box Domain::Boolean(BooleanDomain {
                                    has_true,
                                    has_false,
                                })),
                        }) => (*has_true, *has_null || *has_false),
                        Domain::Nullable(NullableDomain { value: None, .. }) => (false, true),
                        _ => unreachable!(),
                    };
                    match (&mut domain, has_true, has_null_or_false) {
                        (None, true, false) => {
                            return Some(args_domain[cond_idx + 1].clone());
                        }
                        (None, false, true) => {
                            continue;
                        }
                        (None, true, true) => {
                            domain = Some(args_domain[cond_idx + 1].clone());
                        }
                        (Some(prev_domain), true, false) => {
                            return Some(prev_domain.merge(&args_domain[cond_idx + 1]));
                        }
                        (Some(_), false, true) => {
                            continue;
                        }
                        (Some(prev_domain), true, true) => {
                            domain = Some(prev_domain.merge(&args_domain[cond_idx + 1]));
                        }
                        (_, false, false) => unreachable!(),
                    }
                }

                Some(match domain {
                    Some(domain) => domain.merge(args_domain.last().unwrap()),
                    None => args_domain.last().unwrap().clone(),
                })
            }),
            eval: Box::new(|args, ctx| {
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });

                let mut output_builder =
                    ColumnBuilder::with_capacity(&ctx.generics[0], len.unwrap_or(1));
                for row_idx in 0..(len.unwrap_or(1)) {
                    let result_idx = (0..args.len() - 1)
                        .step_by(2)
                        .find(|&cond_idx| match &args[cond_idx] {
                            ValueRef::Scalar(ScalarRef::Null) => false,
                            ValueRef::Scalar(ScalarRef::Boolean(cond)) => *cond,
                            ValueRef::Column(Column::Nullable(box NullableColumn {
                                column: Column::Boolean(cond_col),
                                validity,
                            })) => validity.get_bit(row_idx) && cond_col.get_bit(row_idx),
                            _ => unreachable!(),
                        })
                        .map(|idx| {
                            // The next argument of true condition is the value to return.
                            idx + 1
                        })
                        .unwrap_or_else(|| {
                            // If no true condition is found, the last argument is the value to return.
                            args.len() - 1
                        });

                    match &args[result_idx] {
                        ValueRef::Scalar(scalar) => {
                            output_builder.push(scalar.clone());
                        }
                        ValueRef::Column(col) => {
                            output_builder.push(col.index(row_idx).unwrap());
                        }
                    }
                }

                match len {
                    Some(_) => Ok(Value::Column(output_builder.build())),
                    None => Ok(Value::Scalar(output_builder.build_scalar())),
                }
            }),
        }))
    });
    registry.register_1_arg_core::<NullType, BooleanType, _, _>(
        "is_not_null",
        FunctionProperty::default(),
        |_| {
            Some(BooleanDomain {
                has_true: false,
                has_false: true,
            })
        },
        |_, _| Ok(Value::Scalar(false)),
    );
    registry.register_1_arg_core::<NullableType<GenericType<0>>, BooleanType, _, _>(
        "is_not_null",
        FunctionProperty::default(),
        |NullableDomain { has_null, value }| {
            Some(BooleanDomain {
                has_true: value.is_some(),
                has_false: *has_null,
            })
        },
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, .. }) => {
                let bitmap = validity.clone();
                Ok(Value::Column(bitmap))
            }
            ValueRef::Scalar(None) => Ok(Value::Scalar(false)),
            ValueRef::Scalar(Some(_)) => Ok(Value::Scalar(true)),
        },
    );

    const IN_ERROR_MSG: &str = "Incorrect element of set. Must be literal or constant expression";

    fn cal_in<T: ArgType>(
        lhs: ValueRef<T>,
        rhs: ValueRef<ArrayType<T>>,
    ) -> Result<Value<BooleanType>, String>
    where
        T::Scalar: HashtableKeyable,
    {
        // must be scalar in rhs, if rhs contains column, it'll be rewritted to `a = b1 or a = b2 or a = b3` in type_checker
        let array = rhs.as_scalar().ok_or_else(|| IN_ERROR_MSG.to_string())?;
        let mut set = StackHashSet::<_, 128>::with_capacity(T::column_len(array));
        for val in T::iter_column(array) {
            let _ = set.set_insert(T::to_owned_scalar(val));
        }
        match lhs {
            ValueRef::Scalar(c) => Ok(Value::Scalar(set.contains(&T::to_owned_scalar(c)))),
            ValueRef::Column(col) => {
                let result = BooleanType::column_from_iter(
                    T::iter_column(&col).map(|c| set.contains(&T::to_owned_scalar(c))),
                    &[],
                );
                Ok(Value::Column(result))
            }
        }
    }

    for left in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match left {
            NumberDataType::NUM_TYPE => {
                registry.register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, ArrayType<NumberType<NUM_TYPE>>, BooleanType, _, _>(
                    "in",
                    FunctionProperty::default(),
                    |lhs, rhs| {
                        Some(BooleanDomain {
                            has_false: rhs.max < lhs.max || rhs.min > rhs.max,
                            has_true: (rhs.min..=rhs.max).contains(&lhs.min)
                                || (rhs.min..=rhs.max).contains(&lhs.max),
                        })
                    },
                    |lhs, rhs, _| cal_in(lhs, rhs)
                );
            }
        });
    }

    registry
        .register_passthrough_nullable_2_arg::<DateType, ArrayType<DateType>, BooleanType, _, _>(
            "in",
            FunctionProperty::default(),
            |lhs, rhs| {
                Some(BooleanDomain {
                    has_false: rhs.max < lhs.max || rhs.min > rhs.max,
                    has_true: (rhs.min..=rhs.max).contains(&lhs.min)
                        || (rhs.min..=rhs.max).contains(&lhs.max),
                })
            },
            |lhs, rhs, _| cal_in(lhs, rhs),
        );

    registry.register_passthrough_nullable_2_arg::<TimestampType, ArrayType<TimestampType>, BooleanType, _, _>(
            "in",
            FunctionProperty::default(),
            |lhs, rhs| {
                Some(BooleanDomain {
                    has_false: rhs.max < lhs.max || rhs.min > rhs.max,
                    has_true: (rhs.min..=rhs.max).contains(&lhs.min)
                        || (rhs.min..=rhs.max).contains(&lhs.max),
                })
            },
            |lhs, rhs, _| cal_in(lhs, rhs)
        );

    registry.register_passthrough_nullable_2_arg::<BooleanType, ArrayType<BooleanType>, BooleanType, _, _>(
        "in",
        FunctionProperty::default(),
        |lhs, rhs| {
            Some(BooleanDomain {
                has_false: true,
                has_true: (rhs.has_false && lhs.has_false) || (rhs.has_true && lhs.has_true),
            })
        },
        |lhs, rhs, _| {
            let array = rhs.as_scalar().ok_or_else(|| IN_ERROR_MSG.to_string())?;
            let mut set = StackHashSet::<_, 128>::with_capacity(BooleanType::column_len(array));
            for val in array.iter() {
                let _ = set.set_insert(val as u8);
            }
            match lhs {
                ValueRef::Scalar(val) =>  {
                    Ok(Value::Scalar(set.contains( &(val as u8))))
                },
                ValueRef::Column(col) => {
                    let result = BooleanType::column_from_iter(BooleanType::iter_column(&col).map(|val| {
                        set.contains(&(val as u8))
                    }), &[]);
                    Ok(Value::Column(result))
                }
            }
        }
    );

    registry.register_passthrough_nullable_2_arg::<StringType, ArrayType<StringType>, BooleanType, _, _>(
        "in",
        FunctionProperty::default(),
        |_, _| None,
        |lhs, rhs, _| {
            let array = rhs.as_scalar().ok_or_else(|| IN_ERROR_MSG.to_string())?;
            let mut set = StackHashSet::<_, 128>::with_capacity(StringType::column_len(array));
            for val in array.iter() {
                let key_ref = KeysRef::create(val.as_ptr() as usize, val.len());
                let _ = set.set_insert(key_ref);
            }
            match lhs {
                ValueRef::Scalar(val) =>  {
                    let key_ref = KeysRef::create(val.as_ptr() as usize, val.len());
                    Ok(Value::Scalar(set.contains( &key_ref)))
                },
                ValueRef::Column(col) => {
                    let result = BooleanType::column_from_iter(StringType::iter_column(&col).map(|val| {
                        let key_ref = KeysRef::create(val.as_ptr() as usize, val.len());
                        set.contains(&key_ref)
                    }), &[]);
                    Ok(Value::Column(result))
                }
            }
        }
    );
}
