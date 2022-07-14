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
        F: Fn(I1::ScalarRef<'_>) -> O::Scalar + 'static + Clone + Copy,
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

        self.register_1_arg_core::<NullType, NullType, _>(
            name,
            property.clone(),
            vectorize_1_arg::<NullType, NullType>(|_| ()),
        );

        self.register_1_arg_core::<I1, O, _>(name, property.clone(), vectorize_1_arg(func));

        self.register_1_arg_core::<NullableType<I1>, NullableType<O>, _>(
            name,
            property,
            vectorize_passthrough_nullable_1_arg(func),
        );
    }

    pub fn register_with_writer_1_arg<I1: ArgType, O: ArgType, F>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        func: F,
    ) where
        F: Fn(I1::ScalarRef<'_>, &mut O::ColumnBuilder) + 'static + Clone + Copy,
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

        self.register_1_arg_core::<NullType, NullType, _>(
            name,
            property.clone(),
            vectorize_1_arg::<NullType, NullType>(|_| ()),
        );

        self.register_1_arg_core::<I1, O, _>(
            name,
            property.clone(),
            vectorize_with_writer_1_arg(func),
        );

        self.register_1_arg_core::<NullableType<I1>, NullableType<O>, _>(
            name,
            property,
            vectorize_with_writer_passthrough_nullable_1_arg(func),
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
        F: Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>) -> O::Scalar + Sized + 'static + Clone + Copy,
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

        self.register_2_arg_core::<NullableType<I1>, NullType, NullType, _>(
            name,
            property.clone(),
            vectorize_2_arg::<NullableType<I1>, NullType, NullType>(|_, _| ()),
        );
        self.register_2_arg_core::<NullType, NullableType<I2>, NullType, _>(
            name,
            property.clone(),
            vectorize_2_arg::<NullType, NullableType<I2>, NullType>(|_, _| ()),
        );
        self.register_2_arg_core::<NullType, NullType, NullType, _>(
            name,
            property.clone(),
            vectorize_2_arg::<NullType, NullType, NullType>(|_, _| ()),
        );

        self.register_2_arg_core::<I1, I2, O, _>(name, property.clone(), vectorize_2_arg(func));

        self.register_2_arg_core::<NullableType<I1>, NullableType<I2>, NullableType<O>, _>(
            name,
            property,
            vectorize_passthrough_nullable_2_arg(func),
        );
    }

    pub fn register_with_writer_2_arg<I1: ArgType, I2: ArgType, O: ArgType, F>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        func: F,
    ) where
        F: Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut O::ColumnBuilder)
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

        self.register_2_arg_core::<NullableType<I1>, NullType, NullType, _>(
            name,
            property.clone(),
            vectorize_2_arg::<NullableType<I1>, NullType, NullType>(|_, _| ()),
        );
        self.register_2_arg_core::<NullType, NullableType<I2>, NullType, _>(
            name,
            property.clone(),
            vectorize_2_arg::<NullType, NullableType<I2>, NullType>(|_, _| ()),
        );
        self.register_2_arg_core::<NullType, NullType, NullType, _>(
            name,
            property.clone(),
            vectorize_2_arg::<NullType, NullType, NullType>(|_, _| ()),
        );

        self.register_2_arg_core::<I1, I2, O, _>(
            name,
            property.clone(),
            vectorize_with_writer_2_arg(func),
        );

        self.register_2_arg_core::<NullableType<I1>, NullableType<I2>, NullableType<O>, _>(
            name,
            property,
            vectorize_with_writer_passthrough_nullable_2_arg(func),
        );
    }

    pub fn register_2_arg_core<I1: ArgType, I2: ArgType, O: ArgType, F>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        func: F,
    ) where
        F: Fn(ValueRef<I1>, ValueRef<I2>, &GenericMap) -> Value<O> + Sized + 'static + Clone + Copy,
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
    func: impl Fn(&GenericMap) -> Value<O>,
) -> impl Fn(&[ValueRef<AnyType>], &GenericMap) -> Value<AnyType> {
    move |_args, generics| func(generics).upcast()
}

fn erase_function_generic_1_arg<I1: ArgType, O: ArgType>(
    func: impl Fn(ValueRef<I1>, &GenericMap) -> Value<O>,
) -> impl Fn(&[ValueRef<AnyType>], &GenericMap) -> Value<AnyType> {
    move |args, generics| {
        let arg1 = args[0].try_downcast().unwrap();

        func(arg1, generics).upcast()
    }
}

fn erase_function_generic_2_arg<I1: ArgType, I2: ArgType, O: ArgType>(
    func: impl Fn(ValueRef<I1>, ValueRef<I2>, &GenericMap) -> Value<O>,
) -> impl Fn(&[ValueRef<AnyType>], &GenericMap) -> Value<AnyType> {
    move |args, generics| {
        let arg1 = args[0].try_downcast().unwrap();
        let arg2 = args[1].try_downcast().unwrap();

        func(arg1, arg2, generics).upcast()
    }
}

pub fn vectorize_1_arg<I1: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>) -> O::Scalar + Copy,
) -> impl Fn(ValueRef<I1>, &GenericMap) -> Value<O> + Copy {
    move |arg1, generics| match arg1 {
        ValueRef::Scalar(val) => Value::Scalar(func(val)),
        ValueRef::Column(col) => {
            let iter = I1::iter_column(&col).map(func);
            let col = O::column_from_iter(iter, generics);
            Value::Column(col)
        }
    }
}

pub fn vectorize_with_writer_1_arg<I1: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>, &mut O::ColumnBuilder) + Copy,
) -> impl Fn(ValueRef<I1>, &GenericMap) -> Value<O> + Copy {
    move |arg1, generics| match arg1 {
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

pub fn vectorize_passthrough_nullable_1_arg<I1: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>) -> O::Scalar + Copy,
) -> impl Fn(ValueRef<NullableType<I1>>, &GenericMap) -> Value<NullableType<O>> + Copy {
    move |arg1, generics| match arg1 {
        ValueRef::Scalar(None) => Value::Scalar(None),
        ValueRef::Scalar(Some(val)) => Value::Scalar(Some(func(val))),
        ValueRef::Column((col, validity)) => {
            let iter = I1::iter_column(&col).map(func);
            let col = O::column_from_iter(iter, generics);
            Value::Column((col, validity))
        }
    }
}

pub fn vectorize_with_writer_passthrough_nullable_1_arg<I1: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>, &mut O::ColumnBuilder) + Copy,
) -> impl Fn(ValueRef<NullableType<I1>>, &GenericMap) -> Value<NullableType<O>> + Copy {
    move |arg1, generics| match arg1 {
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

pub fn vectorize_2_arg<I1: ArgType, I2: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>) -> O::Scalar + Copy,
) -> impl Fn(ValueRef<I1>, ValueRef<I2>, &GenericMap) -> Value<O> + Copy {
    move |arg1, arg2, generics| match (arg1, arg2) {
        (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2)) => Value::Scalar(func(arg1, arg2)),
        (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
            let iter = I2::iter_column(&arg2).map(|arg2| func(arg1.clone(), arg2));
            let col = O::column_from_iter(iter, generics);
            Value::Column(col)
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(arg2)) => {
            let iter = I1::iter_column(&arg1).map(|arg1| func(arg1, arg2.clone()));
            let col = O::column_from_iter(iter, generics);
            Value::Column(col)
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
            let iter = I1::iter_column(&arg1)
                .zip(I2::iter_column(&arg2))
                .map(|(arg1, arg2)| func(arg1, arg2));
            let col = O::column_from_iter(iter, generics);
            Value::Column(col)
        }
    }
}

pub fn vectorize_with_writer_2_arg<I1: ArgType, I2: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut O::ColumnBuilder) + Copy,
) -> impl Fn(ValueRef<I1>, ValueRef<I2>, &GenericMap) -> Value<O> + Copy {
    move |arg1, arg2, generics| match (arg1, arg2) {
        (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2)) => {
            let mut builder = O::create_builder(1, generics);
            func(arg1, arg2, &mut builder);
            Value::Scalar(O::build_scalar(builder))
        }
        (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
            let iter = I2::iter_column(&arg2);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for arg2 in iter {
                func(arg1.clone(), arg2, &mut builder);
            }
            Value::Column(O::build_column(builder))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(arg2)) => {
            let iter = I1::iter_column(&arg1);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for arg1 in iter {
                func(arg1, arg2.clone(), &mut builder);
            }
            Value::Column(O::build_column(builder))
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
            let iter = I1::iter_column(&arg1).zip(I2::iter_column(&arg2));
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for (arg1, arg2) in iter {
                func(arg1, arg2, &mut builder);
            }
            Value::Column(O::build_column(builder))
        }
    }
}

pub fn vectorize_passthrough_nullable_2_arg<I1: ArgType, I2: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>) -> O::Scalar + Copy,
) -> impl Fn(
    ValueRef<NullableType<I1>>,
    ValueRef<NullableType<I2>>,
    &GenericMap,
) -> Value<NullableType<O>>
+ Copy {
    move |arg1, arg2, generics| match (arg1, arg2) {
        (ValueRef::Scalar(None), _) | (_, ValueRef::Scalar(None)) => Value::Scalar(None),
        (ValueRef::Scalar(Some(arg1)), ValueRef::Scalar(Some(arg2))) => {
            Value::Scalar(Some(func(arg1, arg2)))
        }
        (ValueRef::Scalar(Some(arg1)), ValueRef::Column((arg2, arg2_validity))) => {
            let iter = I2::iter_column(&arg2).map(|arg2| func(arg1.clone(), arg2));
            let col = O::column_from_iter(iter, generics);
            Value::Column((col, arg2_validity))
        }
        (ValueRef::Column((arg1, arg1_validity)), ValueRef::Scalar(Some(arg2))) => {
            let iter = I1::iter_column(&arg1).map(|arg1| func(arg1, arg2.clone()));
            let col = O::column_from_iter(iter, generics);
            Value::Column((col, arg1_validity))
        }
        (ValueRef::Column((arg1, arg1_validity)), ValueRef::Column((arg2, arg2_validity))) => {
            let iter = I1::iter_column(&arg1)
                .zip(I2::iter_column(&arg2))
                .map(|(arg1, arg2)| func(arg1, arg2));
            let col = O::column_from_iter(iter, generics);
            let validity = common_arrow::arrow::bitmap::and(&arg1_validity, &arg2_validity);
            Value::Column((col, validity))
        }
    }
}

pub fn vectorize_with_writer_passthrough_nullable_2_arg<I1: ArgType, I2: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut O::ColumnBuilder) + Copy,
) -> impl Fn(
    ValueRef<NullableType<I1>>,
    ValueRef<NullableType<I2>>,
    &GenericMap,
) -> Value<NullableType<O>>
+ Copy {
    move |arg1, arg2, generics| match (arg1, arg2) {
        (ValueRef::Scalar(None), _) | (_, ValueRef::Scalar(None)) => Value::Scalar(None),
        (ValueRef::Scalar(Some(arg1)), ValueRef::Scalar(Some(arg2))) => {
            let mut builder = O::create_builder(1, generics);
            func(arg1, arg2, &mut builder);
            Value::Scalar(Some(O::build_scalar(builder)))
        }
        (ValueRef::Scalar(Some(arg1)), ValueRef::Column((arg2, arg2_validity))) => {
            let iter = I2::iter_column(&arg2);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for arg2 in iter {
                func(arg1.clone(), arg2, &mut builder);
            }
            Value::Column((O::build_column(builder), arg2_validity))
        }
        (ValueRef::Column((arg1, arg1_validity)), ValueRef::Scalar(Some(arg2))) => {
            let iter = I1::iter_column(&arg1);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for arg1 in iter {
                func(arg1, arg2.clone(), &mut builder);
            }
            Value::Column((O::build_column(builder), arg1_validity))
        }
        (ValueRef::Column((arg1, arg1_validity)), ValueRef::Column((arg2, arg2_validity))) => {
            let iter = I1::iter_column(&arg1).zip(I2::iter_column(&arg2));
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for (arg1, arg2) in iter {
                func(arg1, arg2, &mut builder);
            }
            let validity = common_arrow::arrow::bitmap::and(&arg1_validity, &arg2_validity);
            Value::Column((O::build_column(builder), validity))
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
