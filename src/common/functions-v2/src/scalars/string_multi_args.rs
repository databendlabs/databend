// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::ArgType;
use common_expression::types::DataType;
use common_expression::types::NullableType;
use common_expression::types::StringType;
use common_expression::types::ValueType;
use common_expression::util::constant_bitmap;
use common_expression::Column;
use common_expression::Domain;
use common_expression::Function;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::NullableDomain;
use common_expression::Scalar;
use common_expression::Value;
use common_expression::ValueRef;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_function_factory("concat", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "concat",
                args_type: vec![DataType::String; args_type.len()],
                return_type: DataType::String,
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(|args, _generics| {
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });
                let args = args
                    .iter()
                    .map(|arg| arg.try_downcast::<StringType>().unwrap())
                    .collect::<Vec<_>>();

                let size = len.unwrap_or(1);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for idx in 0..size {
                    for arg in &args {
                        unsafe { builder.put_slice(arg.index_unchecked(idx)) }
                    }
                    builder.commit_row();
                }

                match len {
                    Some(_) => Ok(Value::Column(Column::String(builder.build()))),
                    _ => Ok(Value::Scalar(Scalar::String(builder.build_scalar()))),
                }
            }),
        }))
    });

    // nullable concat
    registry.register_function_factory("concat", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "concat",
                args_type: vec![DataType::Nullable(Box::new(DataType::String)); args_type.len()],
                return_type: DataType::Nullable(Box::new(DataType::String)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain, _| {
                Some(args_domain.iter().fold(
                    Domain::Nullable(NullableDomain {
                        has_null: false,
                        value: None,
                    }),
                    |acc, x| {
                        let x = x.as_nullable().unwrap();
                        let acc = acc.as_nullable().unwrap();

                        Domain::Nullable(NullableDomain {
                            has_null: acc.has_null | x.has_null,
                            value: None,
                        })
                    },
                ))
            }),
            eval: Box::new(|args, _generics| {
                type T = NullableType<StringType>;
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });
                let mut bitmap: Option<Bitmap> = None;
                let mut inner_args: Vec<ValueRef<StringType>> = Vec::with_capacity(args.len());
                for arg in args {
                    let col = arg.try_downcast::<T>().unwrap();
                    match col {
                        ValueRef::Scalar(None) => return Ok(Value::Scalar(T::upcast_scalar(None))),
                        ValueRef::Column(c) => {
                            bitmap
                                .as_mut()
                                .map(|m| *m = common_arrow::arrow::bitmap::and(m, &c.validity));
                            inner_args.push(ValueRef::Column(c.column.clone()));
                        }
                        ValueRef::Scalar(Some(s)) => inner_args.push(ValueRef::Scalar(s)),
                    }
                }
                let size = len.unwrap_or(1);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for idx in 0..size {
                    for arg in &inner_args {
                        unsafe { builder.put_slice(arg.index_unchecked(idx)) }
                    }
                    builder.commit_row();
                }

                match len {
                    Some(len) => {
                        let n = NullableColumn::<StringType> {
                            column: builder.build(),
                            validity: bitmap.unwrap_or(constant_bitmap(true, len).into()),
                        };
                        let c = T::upcast_column(n);
                        Ok(Value::Column(c))
                    }
                    _ => Ok(Value::Scalar(T::upcast_scalar(Some(
                        builder.build_scalar(),
                    )))),
                }
            }),
        }))
    });

    registry.register_function_factory("concat_ws", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "concat_ws",
                args_type: vec![DataType::String; args_type.len()],
                return_type: DataType::String,
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(|args, _generics| {
                if args.len() < 2 {
                    return Err(format!("concat_ws requires at least two arguments"));
                }
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });
                let args = args
                    .iter()
                    .map(|arg| arg.try_downcast::<StringType>().unwrap())
                    .collect::<Vec<_>>();

                let size = len.unwrap_or(1);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);

                match &args[0] {
                    ValueRef::Scalar(v) => {
                        for idx in 0..size {
                            for arg in &args {
                                if idx != 0 {
                                    builder.put_slice(v);
                                }
                                unsafe { builder.put_slice(arg.index_unchecked(idx)) }
                            }
                            builder.commit_row();
                        }
                    }
                    ValueRef::Column(c) => {
                        for idx in 0..size {
                            for arg in &args {
                                if idx != 0 {
                                    unsafe {
                                        builder.put_slice(c.index_unchecked(idx));
                                    }
                                }
                                unsafe { builder.put_slice(arg.index_unchecked(idx)) }
                            }
                            builder.commit_row();
                        }
                    }
                }

                match len {
                    Some(_) => Ok(Value::Column(Column::String(builder.build()))),
                    _ => Ok(Value::Scalar(Scalar::String(builder.build_scalar()))),
                }
            }),
        }))
    });

    // nullable concat ws
    registry.register_function_factory("concat_ws", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "concat_ws",
                args_type: vec![DataType::Nullable(Box::new(DataType::String)); args_type.len()],
                return_type: DataType::Nullable(Box::new(DataType::String)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain, _| {
                Some(args_domain.iter().fold(
                    Domain::Nullable(NullableDomain {
                        has_null: false,
                        value: None,
                    }),
                    |acc, x| {
                        let x = x.as_nullable().unwrap();
                        let acc = acc.as_nullable().unwrap();

                        Domain::Nullable(NullableDomain {
                            has_null: acc.has_null | x.has_null,
                            value: None,
                        })
                    },
                ))
            }),
            eval: Box::new(|args, _generics| {
                if args.len() < 2 {
                    return Err(format!("concat_ws requires at least two arguments"));
                }

                type T = NullableType<StringType>;
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });

                let size = len.unwrap_or(1);
                let new_args = args
                    .iter()
                    .map(|arg| arg.try_downcast::<T>().unwrap())
                    .collect::<Vec<_>>();

                let mut nullable_builder = T::create_builder(size, &[]);
                match &new_args[0] {
                    ValueRef::Scalar(None) => {
                        return Ok(Value::Scalar(T::upcast_scalar(None)));
                    }
                    ValueRef::Scalar(Some(v)) => {
                        let builder = &mut nullable_builder.builder;
                        nullable_builder.validity.extend_constant(size, true);

                        for idx in 0..size {
                            for arg in &new_args[1..] {
                                unsafe {
                                    match arg.index_unchecked(idx) {
                                        Some(s) if idx != 0 => {
                                            builder.put_slice(v);
                                            builder.put_slice(s);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            builder.commit_row();
                        }
                    }
                    ValueRef::Column(_) => {
                        let mut nullable_builder = T::create_builder(size, &[]);
                        let builder = &mut nullable_builder.builder;
                        let validity = &mut nullable_builder.validity;

                        for idx in 0..size {
                            unsafe {
                                match new_args[0].index_unchecked(idx) {
                                    Some(v) => {
                                        for arg in &new_args[1..] {
                                            match arg.index_unchecked(idx) {
                                                Some(s) if idx != 0 => {
                                                    builder.put_slice(v);
                                                    builder.put_slice(s);
                                                }
                                                _ => {}
                                            }
                                        }
                                        builder.commit_row();
                                        validity.push(true);
                                    }
                                    None => {
                                        builder.commit_row();
                                        validity.push(false);
                                    }
                                }
                            }
                        }
                    }
                }
                match len {
                    Some(_) => {
                        let n = nullable_builder.build();
                        let c = T::upcast_column(n);
                        Ok(Value::Column(c))
                    }
                    _ => Ok(Value::Scalar(T::upcast_scalar(
                        nullable_builder.build_scalar(),
                    ))),
                }
            }),
        }))
    });
}
