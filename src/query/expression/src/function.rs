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
use std::ops::BitAnd;
use std::sync::Arc;

use chrono_tz::Tz;
use common_arrow::arrow::bitmap::MutableBitmap;
use serde::Deserialize;
use serde::Serialize;

use crate::property::Domain;
use crate::property::FunctionProperty;
use crate::types::nullable::NullableColumn;
use crate::types::*;
use crate::utils::arrow::constant_bitmap;
use crate::values::Value;
use crate::values::ValueRef;
use crate::Column;
use crate::Expr;
use crate::FunctionDomain;
use crate::Scalar;

#[derive(Debug, Clone)]
pub struct FunctionSignature {
    pub name: String,
    pub args_type: Vec<DataType>,
    pub return_type: DataType,
    pub property: FunctionProperty,
}

#[derive(Clone, Copy)]
pub struct FunctionContext {
    pub tz: Tz,
}

#[derive(Clone, Copy)]
pub struct EvalContext<'a> {
    pub generics: &'a GenericMap,
    pub num_rows: usize,
    pub tz: Tz,
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
    pub calc_domain: Box<dyn Fn(&[Domain]) -> FunctionDomain<AnyType> + Send + Sync>,
    #[allow(clippy::type_complexity)]
    pub eval: Box<
        dyn Fn(&[ValueRef<AnyType>], EvalContext) -> Result<Value<AnyType>, String> + Send + Sync,
    >,
}

#[derive(Default)]
pub struct FunctionRegistry {
    pub funcs: HashMap<String, Vec<Arc<Function>>>,
    /// A function to build function depending on the const parameters and the type of arguments (before coersion).
    ///
    /// The first argument is the const parameters and the second argument is the number of arguments.
    #[allow(clippy::type_complexity)]
    pub factories: HashMap<
        String,
        Vec<Box<dyn Fn(&[usize], &[DataType]) -> Option<Arc<Function>> + Send + Sync + 'static>>,
    >,
    /// Aliases map from alias function name to concrete function name.
    pub aliases: HashMap<String, String>,
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
        args: &[Expr],
    ) -> Vec<(FunctionID, Arc<Function>)> {
        let name = name.to_lowercase();
        let name = self
            .aliases
            .get(name.as_str())
            .map(String::as_str)
            .unwrap_or(name.as_str());
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
                                && func.signature.args_type.len() == args.len()
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

        let args_type = args
            .iter()
            .map(Expr::data_type)
            .cloned()
            .collect::<Vec<_>>();
        self.factories
            .get(name)
            .map(|factories| {
                factories
                    .iter()
                    .enumerate()
                    .filter_map(|(id, factory)| {
                        factory(params, &args_type).map(|func| {
                            (
                                FunctionID::Factory {
                                    name: name.to_string(),
                                    id,
                                    params: params.to_vec(),
                                    args_type: args_type.clone(),
                                },
                                func,
                            )
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    pub fn register_function_factory(
        &mut self,
        name: &str,
        factory: impl Fn(&[usize], &[DataType]) -> Option<Arc<Function>> + 'static + Send + Sync,
    ) {
        self.factories
            .entry(name.to_string())
            .or_insert_with(Vec::new)
            .push(Box::new(factory));
    }

    pub fn register_aliases(&mut self, fn_name: &str, aliases: &[&str]) {
        for alias in aliases {
            self.aliases.insert(alias.to_string(), fn_name.to_string());
        }
    }
}

pub fn wrap_nullable<F>(
    f: F,
) -> impl Fn(&[ValueRef<AnyType>], EvalContext) -> Result<Value<AnyType>, String> + Copy
where F: Fn(&[ValueRef<AnyType>], EvalContext) -> Result<Value<AnyType>, String> + Copy {
    move |args, ctx| {
        type T = NullableType<AnyType>;
        type Result = AnyType;

        let mut bitmap: Option<MutableBitmap> = None;
        let mut nonull_args: Vec<ValueRef<Result>> = Vec::with_capacity(args.len());

        let mut len = 1;
        for arg in args {
            let arg = arg.try_downcast::<T>().unwrap();
            match arg {
                ValueRef::Scalar(None) => return Ok(Value::Scalar(Scalar::Null)),
                ValueRef::Scalar(Some(s)) => {
                    nonull_args.push(ValueRef::Scalar(s.clone()));
                }
                ValueRef::Column(v) => {
                    len = v.len();
                    nonull_args.push(ValueRef::Column(v.column.clone()));
                    bitmap = match bitmap {
                        Some(m) => Some(m.bitand(&v.validity)),
                        None => Some(v.validity.clone().make_mut()),
                    };
                }
            }
        }
        let results = f(&nonull_args, ctx)?;
        let bitmap = bitmap.unwrap_or_else(|| constant_bitmap(true, len));
        match results {
            Value::Scalar(s) => {
                if bitmap.get(0) {
                    Ok(Value::Scalar(Result::upcast_scalar(s)))
                } else {
                    Ok(Value::Scalar(Scalar::Null))
                }
            }
            Value::Column(column) => {
                let result = match column {
                    Column::Nullable(box nullable_column) => {
                        let validity = bitmap.into();
                        let validity =
                            common_arrow::arrow::bitmap::and(&nullable_column.validity, &validity);
                        Column::Nullable(Box::new(NullableColumn {
                            column: nullable_column.column,
                            validity,
                        }))
                    }
                    _ => Column::Nullable(Box::new(NullableColumn {
                        column,
                        validity: bitmap.into(),
                    })),
                };
                Ok(Value::Column(Result::upcast_column(result)))
            }
        }
    }
}
