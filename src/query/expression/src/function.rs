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
use serde::Deserialize;
use serde::Serialize;

use crate::property::Domain;
use crate::property::FunctionProperty;
use crate::property::NullableDomain;
use crate::types::nullable::NullableColumn;
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

impl Default for FunctionContext {
    fn default() -> Self {
        Self {
            tz: "UTC".parse::<Tz>().unwrap(),
        }
    }
}

/// `FunctionID` is a unique identifier for a function. It's used to construct
/// the exactly same function from the remote execution nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionID {
    Builtin {
        name: String,
        id: usize,
    },
    Factory {
        name: String,
        id: usize,
        params: Vec<usize>,
        args_type: Vec<DataType>,
    },
}

pub struct Function {
    pub signature: FunctionSignature,
    #[allow(clippy::type_complexity)]
    pub calc_domain: Box<dyn Fn(&[Domain], &GenericMap) -> Option<Domain>>,
    #[allow(clippy::type_complexity)]
    pub eval: Box<dyn Fn(&[ValueRef<AnyType>], &GenericMap) -> Result<Value<AnyType>, String>>,
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
    /// Aliases map from alias function name to concrete function name.
    pub aliases: HashMap<&'static str, &'static str>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, id: &FunctionID) -> Option<Arc<Function>> {
        match id {
            FunctionID::Builtin { name, id } => self.funcs.get(name.as_str())?.get(*id).cloned(),
            FunctionID::Factory {
                name,
                id,
                params,
                args_type,
            } => {
                let factory = self.factories.get(name.as_str())?.get(*id)?;
                factory(params, args_type)
            }
        }
    }

    pub fn search_candidates(
        &self,
        name: &str,
        params: &[usize],
        args_type: &[DataType],
    ) -> Vec<(FunctionID, Arc<Function>)> {
        let name = self.aliases.get(name).cloned().unwrap_or(name);
        if params.is_empty() {
            let builtin_funcs = self
                .funcs
                .get(name)
                .map(|funcs| {
                    funcs
                        .iter()
                        .enumerate()
                        .filter_map(|(id, func)| {
                            if func.signature.name == name
                                && func.signature.args_type.len() == args_type.len()
                            {
                                Some((
                                    FunctionID::Builtin {
                                        name: name.to_string(),
                                        id,
                                    },
                                    func.clone(),
                                ))
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
            .get(name)
            .map(|factories| {
                factories
                    .iter()
                    .enumerate()
                    .filter_map(|(id, factory)| {
                        factory(params, args_type).map(|func| {
                            (
                                FunctionID::Factory {
                                    name: name.to_string(),
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

    pub fn register_0_arg_core<O: ArgType, F, G>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        calc_domain: F,
        func: G,
    ) where
        F: Fn() -> Option<O::Domain> + 'static + Clone + Copy,
        G: Fn(&GenericMap) -> Result<Value<O>, String> + 'static + Clone + Copy,
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
                calc_domain: Box::new(erase_calc_domain_generic_0_arg::<O>(calc_domain)),
                eval: Box::new(erase_function_generic_0_arg(func)),
            }));
    }

    pub fn register_1_arg<I1: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&I1::Domain) -> Option<O::Domain> + 'static + Clone + Copy,
        G: Fn(I1::ScalarRef<'_>) -> O::Scalar + 'static + Clone + Copy,
    {
        self.register_passthrough_nullable_1_arg::<I1, O, _, _>(
            name,
            property,
            calc_domain,
            vectorize_1_arg(func),
        )
    }

    pub fn register_passthrough_nullable_1_arg<I1: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&I1::Domain) -> Option<O::Domain> + 'static + Clone + Copy,
        G: Fn(ValueRef<I1>, &GenericMap) -> Result<Value<O>, String> + 'static + Clone + Copy,
    {
        let has_nullable = &[I1::data_type(), O::data_type()]
            .iter()
            .any(|ty| ty.as_nullable().is_some() || ty.is_null());

        assert!(
            !has_nullable,
            "Function {} has nullable argument or output, please use register_1_arg_core instead",
            name
        );

        self.register_1_arg_core::<NullType, NullType, _, _>(
            name,
            property.clone(),
            |_| Some(()),
            |_, _| Ok(Value::Scalar(())),
        );

        self.register_1_arg_core::<I1, O, _, _>(name, property.clone(), calc_domain, func);

        self.register_1_arg_core::<NullableType<I1>, NullableType<O>, _, _>(
            name,
            property,
            move |arg1| {
                let value = match &arg1.value {
                    Some(value) => Some(calc_domain(value)?),
                    None => None,
                };
                Some(NullableDomain {
                    has_null: arg1.has_null,
                    value: value.map(Box::new),
                })
            },
            passthrough_nullable_1_arg(func),
        );
    }

    pub fn register_1_arg_core<I1: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&I1::Domain) -> Option<O::Domain> + 'static + Clone + Copy,
        G: Fn(ValueRef<I1>, &GenericMap) -> Result<Value<O>, String> + 'static + Clone + Copy,
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
                calc_domain: Box::new(erase_calc_domain_generic_1_arg::<I1, O>(calc_domain)),
                eval: Box::new(erase_function_generic_1_arg(func)),
            }));
    }

    pub fn register_2_arg<I1: ArgType, I2: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&I1::Domain, &I2::Domain) -> Option<O::Domain> + 'static + Clone + Copy,
        G: Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>) -> O::Scalar + 'static + Clone + Copy,
    {
        self.register_passthrough_nullable_2_arg::<I1, I2, O, _, _>(
            name,
            property,
            calc_domain,
            vectorize_2_arg(func),
        )
    }

    pub fn register_passthrough_nullable_2_arg<I1: ArgType, I2: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&I1::Domain, &I2::Domain) -> Option<O::Domain> + 'static + Clone + Copy,
        G: Fn(ValueRef<I1>, ValueRef<I2>, &GenericMap) -> Result<Value<O>, String>
            + 'static
            + Clone
            + Copy,
    {
        let has_nullable = &[I1::data_type(), I2::data_type(), O::data_type()]
            .iter()
            .any(|ty| ty.as_nullable().is_some() || ty.is_null());

        assert!(
            !has_nullable,
            "Function {} has nullable argument or output, please use register_2_arg_core instead",
            name
        );

        self.register_2_arg_core::<NullType, NullableType<I2>, NullType, _, _>(
            name,
            property.clone(),
            |_, _| Some(()),
            |_, _, _| Ok(Value::Scalar(())),
        );
        self.register_2_arg_core::<NullableType<I1>, NullType, NullType, _, _>(
            name,
            property.clone(),
            |_, _| Some(()),
            |_, _, _| Ok(Value::Scalar(())),
        );

        self.register_2_arg_core::<I1, I2, O, _, _>(name, property.clone(), calc_domain, func);

        self.register_2_arg_core::<NullableType<I1>, NullableType<I2>, NullableType<O>, _, _>(
            name,
            property,
            move |arg1, arg2| {
                let value = match (&arg1.value, &arg2.value) {
                    (Some(value1), Some(value2)) => Some(calc_domain(value1, value2)?),
                    _ => None,
                };
                Some(NullableDomain {
                    has_null: arg1.has_null || arg2.has_null,
                    value: value.map(Box::new),
                })
            },
            passthrough_nullable_2_arg(func),
        );
    }

    pub fn register_2_arg_core<I1: ArgType, I2: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&I1::Domain, &I2::Domain) -> Option<O::Domain> + 'static + Clone + Copy,
        G: Fn(ValueRef<I1>, ValueRef<I2>, &GenericMap) -> Result<Value<O>, String>
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
                calc_domain: Box::new(erase_calc_domain_generic_2_arg::<I1, I2, O>(calc_domain)),
                eval: Box::new(erase_function_generic_2_arg(func)),
            }));
    }

    pub fn register_3_arg<I1: ArgType, I2: ArgType, I3: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&I1::Domain, &I2::Domain, &I3::Domain) -> Option<O::Domain> + 'static + Clone + Copy,
        G: Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, I3::ScalarRef<'_>) -> O::Scalar
            + 'static
            + Clone
            + Copy,
    {
        self.register_passthrough_nullable_3_arg::<I1, I2, I3, O, _, _>(
            name,
            property,
            calc_domain,
            vectorize_3_arg(func),
        )
    }

    pub fn register_passthrough_nullable_3_arg<
        I1: ArgType,
        I2: ArgType,
        I3: ArgType,
        O: ArgType,
        F,
        G,
    >(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&I1::Domain, &I2::Domain, &I3::Domain) -> Option<O::Domain> + 'static + Clone + Copy,
        G: Fn(ValueRef<I1>, ValueRef<I2>, ValueRef<I3>, &GenericMap) -> Result<Value<O>, String>
            + 'static
            + Clone
            + Copy,
    {
        let has_nullable = &[
            I1::data_type(),
            I2::data_type(),
            I3::data_type(),
            O::data_type(),
        ]
        .iter()
        .any(|ty| ty.as_nullable().is_some() || ty.is_null());

        assert!(
            !has_nullable,
            "Function {} has nullable argument or output, please use register_2_arg_core instead",
            name
        );

        self.register_3_arg_core::<NullType, NullableType<I2>, NullableType<I3>, NullType, _, _>(
            name,
            property.clone(),
            |_, _, _| Some(()),
            |_, _, _, _| Ok(Value::Scalar(())),
        );
        self.register_3_arg_core::<NullableType<I1>, NullType, NullableType<I3>, NullType, _, _>(
            name,
            property.clone(),
            |_, _, _| Some(()),
            |_, _, _, _| Ok(Value::Scalar(())),
        );
        self.register_3_arg_core::<NullableType<I1>, NullableType<I2>, NullType, NullType, _, _>(
            name,
            property.clone(),
            |_, _, _| Some(()),
            |_, _, _, _| Ok(Value::Scalar(())),
        );

        self.register_3_arg_core::<I1, I2, I3, O, _, _>(name, property.clone(), calc_domain, func);

        self.register_3_arg_core::<NullableType<I1>, NullableType<I2>, NullableType<I3>, NullableType<O>, _, _>(
            name,
            property,
            move |arg1, arg2, arg3| {
                let value = match (&arg1.value, &arg2.value, &arg3.value) {
                    (Some(value1), Some(value2), Some(value3)) => Some(calc_domain(value1, value2, value3)?),
                    _ => None,
                };
                Some(NullableDomain {
                    has_null: arg1.has_null || arg2.has_null || arg3.has_null,
                    value: value.map(Box::new),
                })
            },
            passthrough_nullable_3_arg(func),
        );
    }

    pub fn register_3_arg_core<I1: ArgType, I2: ArgType, I3: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &'static str,
        property: FunctionProperty,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&I1::Domain, &I2::Domain, &I3::Domain) -> Option<O::Domain> + 'static + Clone + Copy,
        G: Fn(ValueRef<I1>, ValueRef<I2>, ValueRef<I3>, &GenericMap) -> Result<Value<O>, String>
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
                    args_type: vec![I1::data_type(), I2::data_type(), I3::data_type()],
                    return_type: O::data_type(),
                    property,
                },
                calc_domain: Box::new(erase_calc_domain_generic_3_arg::<I1, I2, I3, O>(
                    calc_domain,
                )),
                eval: Box::new(erase_function_generic_3_arg(func)),
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

    pub fn register_aliases(&mut self, fn_name: &'static str, aliases: &[&'static str]) {
        for alias in aliases {
            self.aliases.insert(alias, fn_name);
        }
    }
}

fn erase_calc_domain_generic_0_arg<O: ArgType>(
    func: impl Fn() -> Option<O::Domain>,
) -> impl Fn(&[Domain], &GenericMap) -> Option<Domain> {
    move |_args, _generics| func().map(O::upcast_domain)
}

fn erase_calc_domain_generic_1_arg<I1: ArgType, O: ArgType>(
    func: impl Fn(&I1::Domain) -> Option<O::Domain>,
) -> impl Fn(&[Domain], &GenericMap) -> Option<Domain> {
    move |args, _generics| {
        let arg1 = I1::try_downcast_domain(&args[0]).unwrap();
        func(&arg1).map(O::upcast_domain)
    }
}

fn erase_calc_domain_generic_2_arg<I1: ArgType, I2: ArgType, O: ArgType>(
    func: impl Fn(&I1::Domain, &I2::Domain) -> Option<O::Domain>,
) -> impl Fn(&[Domain], &GenericMap) -> Option<Domain> {
    move |args, _generics| {
        let arg1 = I1::try_downcast_domain(&args[0]).unwrap();
        let arg2 = I2::try_downcast_domain(&args[1]).unwrap();
        func(&arg1, &arg2).map(O::upcast_domain)
    }
}

fn erase_calc_domain_generic_3_arg<I1: ArgType, I2: ArgType, I3: ArgType, O: ArgType>(
    func: impl Fn(&I1::Domain, &I2::Domain, &I3::Domain) -> Option<O::Domain>,
) -> impl Fn(&[Domain], &GenericMap) -> Option<Domain> {
    move |args, _generics| {
        let arg1 = I1::try_downcast_domain(&args[0]).unwrap();
        let arg2 = I2::try_downcast_domain(&args[1]).unwrap();
        let arg3 = I3::try_downcast_domain(&args[2]).unwrap();
        func(&arg1, &arg2, &arg3).map(O::upcast_domain)
    }
}

fn erase_function_generic_0_arg<O: ArgType>(
    func: impl Fn(&GenericMap) -> Result<Value<O>, String>,
) -> impl Fn(&[ValueRef<AnyType>], &GenericMap) -> Result<Value<AnyType>, String> {
    move |_args, generics| func(generics).map(Value::upcast)
}

fn erase_function_generic_1_arg<I1: ArgType, O: ArgType>(
    func: impl Fn(ValueRef<I1>, &GenericMap) -> Result<Value<O>, String>,
) -> impl Fn(&[ValueRef<AnyType>], &GenericMap) -> Result<Value<AnyType>, String> {
    move |args, generics| {
        let arg1 = args[0].try_downcast().unwrap();
        func(arg1, generics).map(Value::upcast)
    }
}

fn erase_function_generic_2_arg<I1: ArgType, I2: ArgType, O: ArgType>(
    func: impl Fn(ValueRef<I1>, ValueRef<I2>, &GenericMap) -> Result<Value<O>, String>,
) -> impl Fn(&[ValueRef<AnyType>], &GenericMap) -> Result<Value<AnyType>, String> {
    move |args, generics| {
        let arg1 = args[0].try_downcast().unwrap();
        let arg2 = args[1].try_downcast().unwrap();
        func(arg1, arg2, generics).map(Value::upcast)
    }
}

fn erase_function_generic_3_arg<I1: ArgType, I2: ArgType, I3: ArgType, O: ArgType>(
    func: impl Fn(ValueRef<I1>, ValueRef<I2>, ValueRef<I3>, &GenericMap) -> Result<Value<O>, String>,
) -> impl Fn(&[ValueRef<AnyType>], &GenericMap) -> Result<Value<AnyType>, String> {
    move |args, generics| {
        let arg1 = args[0].try_downcast().unwrap();
        let arg2 = args[1].try_downcast().unwrap();
        let arg3 = args[2].try_downcast().unwrap();
        func(arg1, arg2, arg3, generics).map(Value::upcast)
    }
}

pub fn vectorize_1_arg<I1: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>) -> O::Scalar + Copy,
) -> impl Fn(ValueRef<I1>, &GenericMap) -> Result<Value<O>, String> + Copy {
    move |arg1, generics| match arg1 {
        ValueRef::Scalar(val) => Ok(Value::Scalar(func(val))),
        ValueRef::Column(col) => {
            let iter = I1::iter_column(&col).map(func);
            let col = O::column_from_iter(iter, generics);
            Ok(Value::Column(col))
        }
    }
}

pub fn vectorize_2_arg<I1: ArgType, I2: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>) -> O::Scalar + Copy,
) -> impl Fn(ValueRef<I1>, ValueRef<I2>, &GenericMap) -> Result<Value<O>, String> + Copy {
    move |arg1, arg2, generics| match (arg1, arg2) {
        (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2)) => Ok(Value::Scalar(func(arg1, arg2))),
        (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
            let iter = I2::iter_column(&arg2).map(|arg2| func(arg1.clone(), arg2));
            let col = O::column_from_iter(iter, generics);
            Ok(Value::Column(col))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(arg2)) => {
            let iter = I1::iter_column(&arg1).map(|arg1| func(arg1, arg2.clone()));
            let col = O::column_from_iter(iter, generics);
            Ok(Value::Column(col))
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
            let iter = I1::iter_column(&arg1)
                .zip(I2::iter_column(&arg2))
                .map(|(arg1, arg2)| func(arg1, arg2));
            let col = O::column_from_iter(iter, generics);
            Ok(Value::Column(col))
        }
    }
}

pub fn vectorize_3_arg<I1: ArgType, I2: ArgType, I3: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, I3::ScalarRef<'_>) -> O::Scalar + Copy,
) -> impl Fn(ValueRef<I1>, ValueRef<I2>, ValueRef<I3>, &GenericMap) -> Result<Value<O>, String> + Copy
{
    move |arg1, arg2, arg3, generics| match (arg1, arg2, arg3) {
        (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2), ValueRef::Scalar(arg3)) => {
            Ok(Value::Scalar(func(arg1, arg2, arg3)))
        }
        (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2), ValueRef::Column(arg3)) => {
            let arg3_iter = I3::iter_column(&arg3);
            let iter = arg3_iter.map(|arg3| func(arg1.clone(), arg2.clone(), arg3));
            let col = O::column_from_iter(iter, generics);
            Ok(Value::Column(col))
        }
        (ValueRef::Scalar(arg1), ValueRef::Column(arg2), ValueRef::Scalar(arg3)) => {
            let arg2_iter = I2::iter_column(&arg2);
            let iter = arg2_iter.map(|arg2| func(arg1.clone(), arg2, arg3.clone()));
            let col = O::column_from_iter(iter, generics);
            Ok(Value::Column(col))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(arg2), ValueRef::Scalar(arg3)) => {
            let arg1_iter = I1::iter_column(&arg1);
            let iter = arg1_iter.map(|arg1| func(arg1, arg2.clone(), arg3.clone()));
            let col = O::column_from_iter(iter, generics);
            Ok(Value::Column(col))
        }
        (ValueRef::Scalar(arg1), ValueRef::Column(arg2), ValueRef::Column(arg3)) => {
            let arg2_iter = I2::iter_column(&arg2);
            let arg3_iter = I3::iter_column(&arg3);
            let iter = arg2_iter
                .zip(arg3_iter)
                .map(|(arg2, arg3)| func(arg1.clone(), arg2, arg3));
            let col = O::column_from_iter(iter, generics);
            Ok(Value::Column(col))
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2), ValueRef::Scalar(arg3)) => {
            let arg1_iter = I1::iter_column(&arg1);
            let arg2_iter = I2::iter_column(&arg2);
            let iter = arg1_iter
                .zip(arg2_iter)
                .map(|(arg1, arg2)| func(arg1, arg2, arg3.clone()));
            let col = O::column_from_iter(iter, generics);
            Ok(Value::Column(col))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(arg2), ValueRef::Column(arg3)) => {
            let arg1_iter = I1::iter_column(&arg1);
            let arg3_iter = I3::iter_column(&arg3);
            let iter = arg1_iter
                .zip(arg3_iter)
                .map(|(arg1, arg3)| func(arg1, arg2.clone(), arg3));
            let col = O::column_from_iter(iter, generics);
            Ok(Value::Column(col))
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2), ValueRef::Column(arg3)) => {
            let arg1_iter = I1::iter_column(&arg1);
            let arg2_iter = I2::iter_column(&arg2);
            let arg3_iter = I3::iter_column(&arg3);
            let iter = arg1_iter
                .zip(arg2_iter)
                .zip(arg3_iter)
                .map(|((arg1, arg2), arg3)| func(arg1, arg2, arg3));
            let col = O::column_from_iter(iter, generics);
            Ok(Value::Column(col))
        }
    }
}

pub fn vectorize_with_builder_1_arg<I1: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>, &mut O::ColumnBuilder) -> Result<(), String> + Copy,
) -> impl Fn(ValueRef<I1>, &GenericMap) -> Result<Value<O>, String> + Copy {
    move |arg1, generics| match arg1 {
        ValueRef::Scalar(val) => {
            let mut builder = O::create_builder(1, generics);
            func(val, &mut builder)?;
            Ok(Value::Scalar(O::build_scalar(builder)))
        }
        ValueRef::Column(col) => {
            let iter = I1::iter_column(&col);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for val in I1::iter_column(&col) {
                func(val, &mut builder)?;
            }
            Ok(Value::Column(O::build_column(builder)))
        }
    }
}

pub fn vectorize_with_builder_2_arg<I1: ArgType, I2: ArgType, O: ArgType>(
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut O::ColumnBuilder) -> Result<(), String>
    + Copy,
) -> impl Fn(ValueRef<I1>, ValueRef<I2>, &GenericMap) -> Result<Value<O>, String> + Copy {
    move |arg1, arg2, generics| match (arg1, arg2) {
        (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2)) => {
            let mut builder = O::create_builder(1, generics);
            func(arg1, arg2, &mut builder)?;
            Ok(Value::Scalar(O::build_scalar(builder)))
        }
        (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
            let iter = I2::iter_column(&arg2);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for arg2 in iter {
                func(arg1.clone(), arg2, &mut builder)?;
            }
            Ok(Value::Column(O::build_column(builder)))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(arg2)) => {
            let iter = I1::iter_column(&arg1);
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for arg1 in iter {
                func(arg1, arg2.clone(), &mut builder)?;
            }
            Ok(Value::Column(O::build_column(builder)))
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
            let iter = I1::iter_column(&arg1).zip(I2::iter_column(&arg2));
            let mut builder = O::create_builder(iter.size_hint().0, generics);
            for (arg1, arg2) in iter {
                func(arg1, arg2, &mut builder)?;
            }
            Ok(Value::Column(O::build_column(builder)))
        }
    }
}

pub fn vectorize_with_builder_3_arg<I1: ArgType, I2: ArgType, I3: ArgType, O: ArgType>(
    func: impl Fn(
        I1::ScalarRef<'_>,
        I2::ScalarRef<'_>,
        I3::ScalarRef<'_>,
        &mut O::ColumnBuilder,
    ) -> Result<(), String>
    + Copy,
) -> impl Fn(ValueRef<I1>, ValueRef<I2>, ValueRef<I3>, &GenericMap) -> Result<Value<O>, String> + Copy
{
    move |arg1, arg2, arg3, generics| match (arg1, arg2, arg3) {
        (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2), ValueRef::Scalar(arg3)) => {
            let mut builder = O::create_builder(1, generics);
            func(arg1, arg2, arg3, &mut builder)?;
            Ok(Value::Scalar(O::build_scalar(builder)))
        }
        (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2), ValueRef::Column(arg3)) => {
            let arg3_iter = I3::iter_column(&arg3);
            let mut builder = O::create_builder(arg3_iter.size_hint().0, generics);
            for arg3 in arg3_iter {
                func(arg1.clone(), arg2.clone(), arg3, &mut builder)?;
            }
            Ok(Value::Column(O::build_column(builder)))
        }
        (ValueRef::Scalar(arg1), ValueRef::Column(arg2), ValueRef::Scalar(arg3)) => {
            let arg2_iter = I2::iter_column(&arg2);
            let mut builder = O::create_builder(arg2_iter.size_hint().0, generics);
            for arg2 in arg2_iter {
                func(arg1.clone(), arg2, arg3.clone(), &mut builder)?;
            }
            Ok(Value::Column(O::build_column(builder)))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(arg2), ValueRef::Scalar(arg3)) => {
            let arg1_iter = I1::iter_column(&arg1);
            let mut builder = O::create_builder(arg1_iter.size_hint().0, generics);
            for arg1 in arg1_iter {
                func(arg1, arg2.clone(), arg3.clone(), &mut builder)?;
            }
            Ok(Value::Column(O::build_column(builder)))
        }
        (ValueRef::Scalar(arg1), ValueRef::Column(arg2), ValueRef::Column(arg3)) => {
            let arg2_iter = I2::iter_column(&arg2);
            let arg3_iter = I3::iter_column(&arg3);
            let mut builder = O::create_builder(arg2_iter.size_hint().0, generics);
            for (arg2, arg3) in arg2_iter.zip(arg3_iter) {
                func(arg1.clone(), arg2, arg3, &mut builder)?;
            }
            Ok(Value::Column(O::build_column(builder)))
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2), ValueRef::Scalar(arg3)) => {
            let arg1_iter = I1::iter_column(&arg1);
            let arg2_iter = I2::iter_column(&arg2);
            let mut builder = O::create_builder(arg1_iter.size_hint().0, generics);
            for (arg1, arg2) in arg1_iter.zip(arg2_iter) {
                func(arg1, arg2, arg3.clone(), &mut builder)?;
            }
            Ok(Value::Column(O::build_column(builder)))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(arg2), ValueRef::Column(arg3)) => {
            let arg1_iter = I1::iter_column(&arg1);
            let arg3_iter = I3::iter_column(&arg3);
            let mut builder = O::create_builder(arg1_iter.size_hint().0, generics);
            for (arg1, arg3) in arg1_iter.zip(arg3_iter) {
                func(arg1, arg2.clone(), arg3, &mut builder)?;
            }
            Ok(Value::Column(O::build_column(builder)))
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2), ValueRef::Column(arg3)) => {
            let arg1_iter = I1::iter_column(&arg1);
            let arg2_iter = I2::iter_column(&arg2);
            let arg3_iter = I3::iter_column(&arg3);
            let mut builder = O::create_builder(arg1_iter.size_hint().0, generics);
            for ((arg1, arg2), arg3) in arg1_iter.zip(arg2_iter).zip(arg3_iter) {
                func(arg1, arg2, arg3, &mut builder)?;
            }
            Ok(Value::Column(O::build_column(builder)))
        }
    }
}

pub fn passthrough_nullable_1_arg<I1: ArgType, O: ArgType>(
    func: impl Fn(ValueRef<I1>, &GenericMap) -> Result<Value<O>, String> + Copy,
) -> impl Fn(ValueRef<NullableType<I1>>, &GenericMap) -> Result<Value<NullableType<O>>, String> + Copy
{
    move |arg1, generics| match arg1 {
        ValueRef::Scalar(None) => Ok(Value::Scalar(None)),
        ValueRef::Scalar(Some(val)) => Ok(Value::Scalar(Some(
            func(ValueRef::Scalar(val), generics)?
                .into_scalar()
                .unwrap(),
        ))),
        ValueRef::Column(col) => {
            let inner_col = func(ValueRef::Column(col.column), generics)?
                .into_column()
                .unwrap();
            Ok(Value::Column(NullableColumn {
                column: inner_col,
                validity: col.validity,
            }))
        }
    }
}

pub fn passthrough_nullable_2_arg<I1: ArgType, I2: ArgType, O: ArgType>(
    func: impl Fn(ValueRef<I1>, ValueRef<I2>, &GenericMap) -> Result<Value<O>, String> + Copy,
) -> impl Fn(
    ValueRef<NullableType<I1>>,
    ValueRef<NullableType<I2>>,
    &GenericMap,
) -> Result<Value<NullableType<O>>, String>
+ Copy {
    move |arg1, arg2, generics| match (arg1, arg2) {
        (ValueRef::Scalar(None), _) | (_, ValueRef::Scalar(None)) => Ok(Value::Scalar(None)),
        (ValueRef::Scalar(Some(arg1)), ValueRef::Scalar(Some(arg2))) => Ok(Value::Scalar(Some(
            func(ValueRef::Scalar(arg1), ValueRef::Scalar(arg2), generics)?
                .into_scalar()
                .unwrap(),
        ))),
        (ValueRef::Scalar(Some(arg1)), ValueRef::Column(arg2)) => {
            let column = func(
                ValueRef::Scalar(arg1),
                ValueRef::Column(arg2.column),
                generics,
            )?
            .into_column()
            .unwrap();
            Ok(Value::Column(NullableColumn {
                column,
                validity: arg2.validity,
            }))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(Some(arg2))) => {
            let column = func(
                ValueRef::Column(arg1.column),
                ValueRef::Scalar(arg2),
                generics,
            )?
            .into_column()
            .unwrap();
            Ok(Value::Column(NullableColumn {
                column,
                validity: arg1.validity,
            }))
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
            let column = func(
                ValueRef::Column(arg1.column),
                ValueRef::Column(arg2.column),
                generics,
            )?
            .into_column()
            .unwrap();
            let validity = common_arrow::arrow::bitmap::and(&arg1.validity, &arg2.validity);
            Ok(Value::Column(NullableColumn { column, validity }))
        }
    }
}

pub fn passthrough_nullable_3_arg<I1: ArgType, I2: ArgType, I3: ArgType, O: ArgType>(
    func: impl Fn(ValueRef<I1>, ValueRef<I2>, ValueRef<I3>, &GenericMap) -> Result<Value<O>, String>
    + Copy,
) -> impl Fn(
    ValueRef<NullableType<I1>>,
    ValueRef<NullableType<I2>>,
    ValueRef<NullableType<I3>>,
    &GenericMap,
) -> Result<Value<NullableType<O>>, String>
+ Copy {
    move |arg1, arg2, arg3, generics| match (arg1, arg2, arg3) {
        (ValueRef::Scalar(None), _, _)
        | (_, ValueRef::Scalar(None), _)
        | (_, _, ValueRef::Scalar(None)) => Ok(Value::Scalar(None)),
        (
            ValueRef::Scalar(Some(arg1)),
            ValueRef::Scalar(Some(arg2)),
            ValueRef::Scalar(Some(arg3)),
        ) => Ok(Value::Scalar(Some(
            func(
                ValueRef::Scalar(arg1),
                ValueRef::Scalar(arg2),
                ValueRef::Scalar(arg3),
                generics,
            )?
            .into_scalar()
            .unwrap(),
        ))),
        (ValueRef::Scalar(Some(arg1)), ValueRef::Scalar(Some(arg2)), ValueRef::Column(arg3)) => {
            let column = func(
                ValueRef::Scalar(arg1),
                ValueRef::Scalar(arg2),
                ValueRef::Column(arg3.column),
                generics,
            )?
            .into_column()
            .unwrap();
            Ok(Value::Column(NullableColumn {
                column,
                validity: arg3.validity,
            }))
        }
        (ValueRef::Scalar(Some(arg1)), ValueRef::Column(arg2), ValueRef::Scalar(Some(arg3))) => {
            let column = func(
                ValueRef::Scalar(arg1),
                ValueRef::Column(arg2.column),
                ValueRef::Scalar(arg3),
                generics,
            )?
            .into_column()
            .unwrap();
            Ok(Value::Column(NullableColumn {
                column,
                validity: arg2.validity,
            }))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(Some(arg2)), ValueRef::Scalar(Some(arg3))) => {
            let column = func(
                ValueRef::Column(arg1.column),
                ValueRef::Scalar(arg2),
                ValueRef::Scalar(arg3),
                generics,
            )?
            .into_column()
            .unwrap();
            Ok(Value::Column(NullableColumn {
                column,
                validity: arg1.validity,
            }))
        }
        (ValueRef::Scalar(Some(arg1)), ValueRef::Column(arg2), ValueRef::Column(arg3)) => {
            let column = func(
                ValueRef::Scalar(arg1),
                ValueRef::Column(arg2.column),
                ValueRef::Column(arg3.column),
                generics,
            )?
            .into_column()
            .unwrap();
            let validity = common_arrow::arrow::bitmap::and(&arg2.validity, &arg3.validity);
            Ok(Value::Column(NullableColumn { column, validity }))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(Some(arg2)), ValueRef::Column(arg3)) => {
            let column = func(
                ValueRef::Column(arg1.column),
                ValueRef::Scalar(arg2),
                ValueRef::Column(arg3.column),
                generics,
            )?
            .into_column()
            .unwrap();
            let validity = common_arrow::arrow::bitmap::and(&arg1.validity, &arg3.validity);
            Ok(Value::Column(NullableColumn { column, validity }))
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2), ValueRef::Scalar(Some(arg3))) => {
            let column = func(
                ValueRef::Column(arg1.column),
                ValueRef::Column(arg2.column),
                ValueRef::Scalar(arg3),
                generics,
            )?
            .into_column()
            .unwrap();
            let validity = common_arrow::arrow::bitmap::and(&arg1.validity, &arg2.validity);
            Ok(Value::Column(NullableColumn { column, validity }))
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2), ValueRef::Column(arg3)) => {
            let column = func(
                ValueRef::Column(arg1.column),
                ValueRef::Column(arg2.column),
                ValueRef::Column(arg3.column),
                generics,
            )?
            .into_column()
            .unwrap();
            let validity = common_arrow::arrow::bitmap::and(
                &common_arrow::arrow::bitmap::and(&arg1.validity, &arg2.validity),
                &arg3.validity,
            );
            Ok(Value::Column(NullableColumn { column, validity }))
        }
    }
}
