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

use std::collections::HashMap;
use std::sync::Arc;

use chrono_tz::Tz;

use crate::property::FunctionProperty;
use crate::types::*;
use crate::values::Value;
use crate::values::ValueRef;

#[derive(Debug, Clone)]
pub struct FunctionSignature {
    pub name: &'static str,
    pub args_type: Vec<DataType>,
    pub return_type: DataType,
    pub property: FunctionProperty,
}

#[derive(Clone)]
pub struct FunctionContext {
    pub tz: Tz,
}

/// `FunctionID` is a unique identifier for a function. It's used to construct
/// the exactly same function from the remote execution nodes.
#[derive(Debug, Clone)]
pub enum FunctionID {
    Builtin {
        name: &'static str,
        id: usize,
    },
    Factory {
        name: &'static str,
        id: usize,
        params: Vec<usize>,
        args_type: Vec<DataType>,
    },
}

pub struct Function {
    pub signature: FunctionSignature,
    #[allow(clippy::type_complexity)]
    pub eval: Box<dyn Fn(&[ValueRef<AnyType>], &GenericMap) -> Value<AnyType>>,
    // #[allow(clippy::type_complexity)]
    // pub domain_to_range: Option<Box<dyn Fn(&[ValueRange<AnyType>]) -> Option<ValueRange<AnyType>>>>,
}

#[derive(Default)]
pub struct FunctionRegistry {
    pub funcs: HashMap<&'static str, Vec<Arc<Function>>>,
    /// A function to build function depending on the const parameters and the type of arguments (before coersion).
    ///
    /// The first argument is the const parameters and the second argument is the number of arguments.
    #[allow(clippy::type_complexity)]
    pub factories: HashMap<
        &'static str,
        Vec<Box<dyn Fn(&[usize], &[DataType]) -> Option<Arc<Function>> + 'static>>,
    >,
}

impl FunctionRegistry {
    pub fn search_candidates(
        &self,
        name: &str,
        params: &[usize],
        args_type: &[DataType],
    ) -> Vec<(FunctionID, Arc<Function>)> {
        if params.is_empty() {
            let builtin_funcs = self
                .funcs
                .get_key_value(name)
                .map(|(name, funcs)| {
                    funcs
                        .iter()
                        .enumerate()
                        .filter_map(|(id, func)| {
                            if func.signature.name == *name
                                && func.signature.args_type.len() == args_type.len()
                            {
                                Some((FunctionID::Builtin { name, id }, func.clone()))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            if !builtin_funcs.is_empty() {
                return builtin_funcs;
            }
        }

        self.factories
            .get_key_value(name)
            .map(|(name, factories)| {
                factories
                    .iter()
                    .enumerate()
                    .filter_map(|(id, factory)| {
                        factory(params, args_type).map(|func| {
                            (
                                FunctionID::Factory {
                                    name,
                                    id,
                                    params: params.to_vec(),
                                    args_type: args_type.to_vec(),
                                },
                                func,
                            )
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    pub fn register_0_arg_core<O: ArgType, F>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        func: F,
    ) where
        F: Fn(&GenericMap) -> Value<O> + 'static + Clone + Copy,
    {
        self.funcs
            .entry(name)
            .or_insert_with(Vec::new)
            .push(Arc::new(Function {
                signature: FunctionSignature {
                    name,
                    args_type: vec![],
                    return_type: O::data_type(),
                    property,
                },
                eval: Box::new(erase_function_generic_0_arg(func)),
            }));
    }

    pub fn register_1_arg<I1: ArgType, O: ArgType, F>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        func: F,
    ) where
        F: for<'a> Fn(I1::ScalarRef<'a>) -> O::Scalar + 'static + Clone + Copy,
    {
        let has_nullable = &[I1::data_type(), O::data_type()]
            .iter()
            .any(|ty| ty.as_nullable().is_some());

        assert!(
            !has_nullable,
            "Function {} has nullable argument or output, please use register_1_arg_core instead",
            name
        );

        let property = property.preserve_not_null(true);

        self.register_1_arg_core::<NullType, NullType, _>(name, property.clone(), move |_, _| {
            Value::Scalar(())
        });

        self.register_1_arg_core::<I1, O, _>(name, property.clone(), move |val, generics| {
            vectorize_1_arg(val, generics, func)
        });

        self.register_1_arg_core::<NullableType<I1>, NullableType<O>, _>(
            name,
            property,
            move |val, generics| vectorize_passthrough_nullable_1_arg(val, generics, func),
        );
    }

    pub fn register_with_writer_1_arg<I1: ArgType, O: ArgType, F>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        func: F,
    ) where
        F: for<'a> Fn(I1::ScalarRef<'a>, &mut O::ColumnBuilder) + 'static + Clone + Copy,
    {
        let has_nullable = &[I1::data_type(), O::data_type()]
            .iter()
            .any(|ty| ty.as_nullable().is_some());

        assert!(
            !has_nullable,
            "Function {} has nullable argument or output, please use register_1_arg_core instead",
            name
        );

        let property = property.preserve_not_null(true);

        self.register_1_arg_core::<NullType, NullType, _>(name, property.clone(), move |_, _| {
            Value::Scalar(())
        });

        self.register_1_arg_core::<I1, O, _>(name, property.clone(), move |val, generics| {
            vectorize_with_writer_1_arg(val, generics, func)
        });

        self.register_1_arg_core::<NullableType<I1>, NullableType<O>, _>(
            name,
            property,
            move |val, generics| {
                vectorize_with_writer_passthrough_nullable_1_arg(val, generics, func)
            },
        );
    }

    pub fn register_1_arg_core<I1: ArgType, O: ArgType, F>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        func: F,
    ) where
        F: Fn(ValueRef<I1>, &GenericMap) -> Value<O> + 'static + Clone + Copy,
    {
        self.funcs
            .entry(name)
            .or_insert_with(Vec::new)
            .push(Arc::new(Function {
                signature: FunctionSignature {
                    name,
                    args_type: vec![I1::data_type()],
                    return_type: O::data_type(),
                    property,
                },
                eval: Box::new(erase_function_generic_1_arg(func)),
            }));
    }

    pub fn register_2_arg<I1: ArgType, I2: ArgType, O: ArgType, F>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        func: F,
    ) where
        F: for<'a, 'b> Fn(I1::ScalarRef<'a>, I2::ScalarRef<'b>) -> O::Scalar
            + Sized
            + 'static
            + Clone
            + Copy,
    {
        let has_nullable = &[I1::data_type(), I2::data_type(), O::data_type()]
            .iter()
            .any(|ty| ty.as_nullable().is_some());

        assert!(
            !has_nullable,
            "Function {} has nullable argument or output, please use register_2_arg_core instead",
            name
        );

        let property = property.preserve_not_null(true);

        self.register_2_arg_core::<NullType, I2, NullType, _>(
            name,
            property.clone(),
            move |_, _, _| Value::Scalar(()),
        );
        self.register_2_arg_core::<I1, NullType, NullType, _>(
            name,
            property.clone(),
            move |_, _, _| Value::Scalar(()),
        );
        self.register_2_arg_core::<NullType, NullType, NullType, _>(
            name,
            property.clone(),
            move |_, _, _| Value::Scalar(()),
        );

        self.register_2_arg_core::<I1, I2, O, _>(
            name,
            property.clone(),
            move |lhs, rhs, generics| vectorize_2_arg(lhs, rhs, generics, func),
        );

        self.register_2_arg_core::<NullableType<I1>, NullableType<I2>, NullableType<O>, _>(
            name,
            property,
            move |lhs, rhs, generics| {
                vectorize_passthrough_nullable_2_arg(lhs, rhs, generics, func)
            },
        );
    }

    pub fn register_with_writer_2_arg<I1: ArgType, I2: ArgType, O: ArgType, F>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        func: F,
    ) where
        F: for<'a, 'b> Fn(I1::ScalarRef<'a>, I2::ScalarRef<'b>, &mut O::ColumnBuilder)
            + Sized
            + 'static
            + Clone
            + Copy,
    {
        let has_nullable = &[I1::data_type(), I2::data_type(), O::data_type()]
            .iter()
            .any(|ty| ty.as_nullable().is_some());

        assert!(
            !has_nullable,
            "Function {} has nullable argument or output, please use register_2_arg_core instead",
            name
        );

        let property = property.preserve_not_null(true);

        self.register_2_arg_core::<NullType, I2, NullType, _>(
            name,
            property.clone(),
            move |_, _, _| Value::Scalar(()),
        );
        self.register_2_arg_core::<I1, NullType, NullType, _>(
            name,
            property.clone(),
            move |_, _, _| Value::Scalar(()),
        );
        self.register_2_arg_core::<NullType, NullType, NullType, _>(
            name,
            property.clone(),
            move |_, _, _| Value::Scalar(()),
        );

        self.register_2_arg_core::<I1, I2, O, _>(
            name,
            property.clone(),
            move |lhs, rhs, generics| vectorize_with_writer_2_arg(lhs, rhs, generics, func),
        );

        self.register_2_arg_core::<NullableType<I1>, NullableType<I2>, NullableType<O>, _>(
            name,
            property,
            move |lhs, rhs, generics| {
                vectorize_with_writer_passthrough_nullable_2_arg(lhs, rhs, generics, func)
            },
        );
    }

    pub fn register_2_arg_core<I1: ArgType, I2: ArgType, O: ArgType, F>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        func: F,
    ) where
        F: for<'a> Fn(ValueRef<'a, I1>, ValueRef<'a, I2>, &GenericMap) -> Value<O>
            + Sized
            + 'static
            + Clone
            + Copy,
    {
        self.funcs
            .entry(name)
            .or_insert_with(Vec::new)
            .push(Arc::new(Function {
                signature: FunctionSignature {
                    name,
                    args_type: vec![I1::data_type(), I2::data_type()],
                    return_type: O::data_type(),
                    property,
                },
                eval: Box::new(erase_function_generic_2_arg(func)),
            }));
    }

    pub fn register_function_factory(
        &mut self,
        name: &'static str,
        factory: impl Fn(&[usize], &[DataType]) -> Option<Arc<Function>> + 'static,
    ) {
        self.factories
            .entry(name)
            .or_insert_with(Vec::new)
            .push(Box::new(factory));
    }
}

fn erase_function_generic_0_arg<O: ArgType>(
    func: impl for<'a> Fn(&GenericMap) -> Value<O>,
) -> impl Fn(&[ValueRef<AnyType>], &GenericMap) -> Value<AnyType> {
    move |_args, generics| {
        let result = func(generics);

        match result {
            Value::Scalar(scalar) => Value::Scalar(O::upcast_scalar(scalar)),
            Value::Column(col) => Value::Column(O::upcast_column(col)),
        }
    }
}

fn erase_function_generic_1_arg<I1: ArgType, O: ArgType>(
    func: impl for<'a> Fn(ValueRef<'a, I1>, &GenericMap) -> Value<O>,
) -> impl Fn(&[ValueRef<AnyType>], &GenericMap) -> Value<AnyType> {
    move |args, generics| {
        let arg1 = match &args[0] {
            ValueRef::Scalar(scalar) => ValueRef::Scalar(I1::try_downcast_scalar(scalar).unwrap()),
            ValueRef::Column(col) => ValueRef::Column(I1::try_downcast_column(col).unwrap()),
        };

        let result = func(arg1, generics);

        match result {
            Value::Scalar(scalar) => Value::Scalar(O::upcast_scalar(scalar)),
            Value::Column(col) => Value::Column(O::upcast_column(col)),
        }
    }
}

fn erase_function_generic_2_arg<I1: ArgType, I2: ArgType, O: ArgType>(
    func: impl for<'a> Fn(ValueRef<'a, I1>, ValueRef<'a, I2>, &GenericMap) -> Value<O>,
) -> impl Fn(&[ValueRef<AnyType>], &GenericMap) -> Value<AnyType> {
    move |args, generics| {
        let arg1 = match &args[0] {
            ValueRef::Scalar(scalar) => ValueRef::Scalar(I1::try_downcast_scalar(scalar).unwrap()),
            ValueRef::Column(col) => ValueRef::Column(I1::try_downcast_column(col).unwrap()),
        };
        let arg2 = match &args[1] {
            ValueRef::Scalar(scalar) => ValueRef::Scalar(I2::try_downcast_scalar(scalar).unwrap()),
            ValueRef::Column(col) => ValueRef::Column(I2::try_downcast_column(col).unwrap()),
        };

        let result = func(arg1, arg2, generics);

        match result {
            Value::Scalar(scalar) => Value::Scalar(O::upcast_scalar(scalar)),
            Value::Column(col) => Value::Column(O::upcast_column(col)),
        }
    }
}

pub fn vectorize_1_arg<'a, I1: ArgType, O: ArgType>(
    val: ValueRef<'a, I1>,
    generics: &GenericMap,
    func: impl Fn(I1::ScalarRef<'_>) -> O::Scalar,
) -> Value<O> {
    match val {
        ValueRef::Scalar(val) => Value::Scalar(func(val)),
        ValueRef::Column(col) => {
            let iter = I1::iter_column(&col).map(func);
            let col = O::column_from_iter(iter, generics);
            Value::Column(col)
        }
    }
}

pub fn vectorize_with_writer_1_arg<'a, I1: ArgType, O: ArgType>(
    val: ValueRef<'a, I1>,
    generics: &GenericMap,
    func: impl Fn(I1::ScalarRef<'_>, &mut O::ColumnBuilder),
) -> Value<O> {
    match val {
        ValueRef::Scalar(val) => {
            let mut builder = O::create_builder(1, generics);
            func(val, &mut builder);
            Value::Scalar(O::build_scalar(builder))
        }
        ValueRef::Column(col) => {
            let iter = I1::iter_column(&col);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for val in I1::iter_column(&col) {
                func(val, &mut builder);
            }
            Value::Column(O::build_column(builder))
        }
    }
}

pub fn vectorize_passthrough_nullable_1_arg<'a, I1: ArgType, O: ArgType>(
    val: ValueRef<'a, NullableType<I1>>,
    generics: &GenericMap,
    func: impl for<'for_a> Fn(I1::ScalarRef<'for_a>) -> O::Scalar,
) -> Value<NullableType<O>> {
    match val {
        ValueRef::Scalar(None) => Value::Scalar(None),
        ValueRef::Scalar(Some(val)) => Value::Scalar(Some(func(val))),
        ValueRef::Column((col, validity)) => {
            let iter = I1::iter_column(&col).map(func);
            let col = O::column_from_iter(iter, generics);
            Value::Column((col, validity))
        }
    }
}

pub fn vectorize_with_writer_passthrough_nullable_1_arg<'a, I1: ArgType, O: ArgType>(
    val: ValueRef<'a, NullableType<I1>>,
    generics: &GenericMap,
    func: impl Fn(I1::ScalarRef<'_>, &mut O::ColumnBuilder),
) -> Value<NullableType<O>> {
    match val {
        ValueRef::Scalar(None) => Value::Scalar(None),
        ValueRef::Scalar(Some(val)) => {
            let mut builder = O::create_builder(1, generics);
            func(val, &mut builder);
            Value::Scalar(Some(O::build_scalar(builder)))
        }
        ValueRef::Column((col, validity)) => {
            let iter = I1::iter_column(&col);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for val in I1::iter_column(&col) {
                func(val, &mut builder);
            }
            Value::Column((O::build_column(builder), validity))
        }
    }
}

pub fn vectorize_2_arg<'a, 'b, I1: ArgType, I2: ArgType, O: ArgType>(
    lhs: ValueRef<'a, I1>,
    rhs: ValueRef<'b, I2>,
    generics: &GenericMap,
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>) -> O::Scalar,
) -> Value<O> {
    match (lhs, rhs) {
        (ValueRef::Scalar(lhs), ValueRef::Scalar(rhs)) => Value::Scalar(func(lhs, rhs)),
        (ValueRef::Scalar(lhs), ValueRef::Column(rhs)) => {
            let iter = I2::iter_column(&rhs).map(|rhs| func(lhs.clone(), rhs));
            let col = O::column_from_iter(iter, generics);
            Value::Column(col)
        }
        (ValueRef::Column(lhs), ValueRef::Scalar(rhs)) => {
            let iter = I1::iter_column(&lhs).map(|lhs| func(lhs, rhs.clone()));
            let col = O::column_from_iter(iter, generics);
            Value::Column(col)
        }
        (ValueRef::Column(lhs), ValueRef::Column(rhs)) => {
            let iter = I1::iter_column(&lhs)
                .zip(I2::iter_column(&rhs))
                .map(|(lhs, rhs)| func(lhs, rhs));
            let col = O::column_from_iter(iter, generics);
            Value::Column(col)
        }
    }
}

pub fn vectorize_with_writer_2_arg<'a, 'b, I1: ArgType, I2: ArgType, O: ArgType>(
    lhs: ValueRef<'a, I1>,
    rhs: ValueRef<'b, I2>,
    generics: &GenericMap,
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut O::ColumnBuilder),
) -> Value<O> {
    match (lhs, rhs) {
        (ValueRef::Scalar(lhs), ValueRef::Scalar(rhs)) => {
            let mut builder = O::create_builder(1, generics);
            func(lhs, rhs, &mut builder);
            Value::Scalar(O::build_scalar(builder))
        }
        (ValueRef::Scalar(lhs), ValueRef::Column(rhs)) => {
            let iter = I2::iter_column(&rhs);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for rhs in iter {
                func(lhs.clone(), rhs, &mut builder);
            }
            Value::Column(O::build_column(builder))
        }
        (ValueRef::Column(lhs), ValueRef::Scalar(rhs)) => {
            let iter = I1::iter_column(&lhs);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for lhs in iter {
                func(lhs, rhs.clone(), &mut builder);
            }
            Value::Column(O::build_column(builder))
        }
        (ValueRef::Column(lhs), ValueRef::Column(rhs)) => {
            let iter = I1::iter_column(&lhs).zip(I2::iter_column(&rhs));
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for (lhs, rhs) in iter {
                func(lhs, rhs, &mut builder);
            }
            Value::Column(O::build_column(builder))
        }
    }
}

pub fn vectorize_passthrough_nullable_2_arg<'a, 'b, I1: ArgType, I2: ArgType, O: ArgType>(
    lhs: ValueRef<'a, NullableType<I1>>,
    rhs: ValueRef<'b, NullableType<I2>>,
    generics: &GenericMap,
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>) -> O::Scalar,
) -> Value<NullableType<O>> {
    match (lhs, rhs) {
        (ValueRef::Scalar(None), _) | (_, ValueRef::Scalar(None)) => Value::Scalar(None),
        (ValueRef::Scalar(Some(lhs)), ValueRef::Scalar(Some(rhs))) => {
            Value::Scalar(Some(func(lhs, rhs)))
        }
        (ValueRef::Scalar(Some(lhs)), ValueRef::Column((rhs, rhs_validity))) => {
            let iter = I2::iter_column(&rhs).map(|rhs| func(lhs.clone(), rhs));
            let col = O::column_from_iter(iter, generics);
            Value::Column((col, rhs_validity))
        }
        (ValueRef::Column((lhs, lhs_validity)), ValueRef::Scalar(Some(rhs))) => {
            let iter = I1::iter_column(&lhs).map(|lhs| func(lhs, rhs.clone()));
            let col = O::column_from_iter(iter, generics);
            Value::Column((col, lhs_validity))
        }
        (ValueRef::Column((lhs, lhs_validity)), ValueRef::Column((rhs, rhs_validity))) => {
            let iter = I1::iter_column(&lhs)
                .zip(I2::iter_column(&rhs))
                .map(|(lhs, rhs)| func(lhs, rhs));
            let col = O::column_from_iter(iter, generics);
            let validity = common_arrow::arrow::bitmap::or(&lhs_validity, &rhs_validity);
            Value::Column((col, validity))
        }
    }
}

pub fn vectorize_with_writer_passthrough_nullable_2_arg<
    'a,
    'b,
    I1: ArgType,
    I2: ArgType,
    O: ArgType,
>(
    lhs: ValueRef<'a, NullableType<I1>>,
    rhs: ValueRef<'b, NullableType<I2>>,
    generics: &GenericMap,
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut O::ColumnBuilder),
) -> Value<NullableType<O>> {
    match (lhs, rhs) {
        (ValueRef::Scalar(None), _) | (_, ValueRef::Scalar(None)) => Value::Scalar(None),
        (ValueRef::Scalar(Some(lhs)), ValueRef::Scalar(Some(rhs))) => {
            let mut builder = O::create_builder(1, generics);
            func(lhs, rhs, &mut builder);
            Value::Scalar(Some(O::build_scalar(builder)))
        }
        (ValueRef::Scalar(Some(lhs)), ValueRef::Column((rhs, rhs_validity))) => {
            let iter = I2::iter_column(&rhs).zip(&rhs_validity);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for (rhs, rhs_validity) in iter {
                if rhs_validity {
                    func(lhs.clone(), rhs, &mut builder);
                } else {
                    O::push_default(&mut builder);
                }
            }
            Value::Column((O::build_column(builder), rhs_validity))
        }
        (ValueRef::Column((lhs, lhs_validity)), ValueRef::Scalar(Some(rhs))) => {
            let iter = I1::iter_column(&lhs).zip(&lhs_validity);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for (lhs, lhs_validity) in iter {
                if lhs_validity {
                    func(lhs, rhs.clone(), &mut builder);
                } else {
                    O::push_default(&mut builder);
                }
            }
            Value::Column((O::build_column(builder), lhs_validity))
        }
        (ValueRef::Column((lhs, lhs_validity)), ValueRef::Column((rhs, rhs_validity))) => {
            let iter = I1::iter_column(&lhs)
                .zip(&lhs_validity)
                .zip(I2::iter_column(&rhs))
                .zip(&rhs_validity);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for (((lhs, lhs_validity), rhs), rhs_validity) in iter {
                if lhs_validity && rhs_validity {
                    func(lhs, rhs, &mut builder);
                } else {
                    O::push_default(&mut builder);
                }
            }
            Value::Column((O::build_column(builder), lhs_validity))
        }
    }
}

impl Default for FunctionContext {
    fn default() -> Self {
        Self {
            tz: "UTC".parse::<Tz>().unwrap(),
        }
    }
}
