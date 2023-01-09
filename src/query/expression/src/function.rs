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
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;

use crate::property::Domain;
use crate::property::FunctionProperty;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableDomain;
use crate::types::*;
use crate::utils::arrow::constant_bitmap;
use crate::values::Value;
use crate::values::ValueRef;
use crate::Column;
use crate::ColumnIndex;
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

pub type AutoCastSignature = Vec<(DataType, DataType)>;

#[derive(Clone, Copy)]
pub struct FunctionContext {
    pub tz: Tz,
}

impl Default for FunctionContext {
    fn default() -> Self {
        Self { tz: Tz::UTC }
    }
}

#[derive(Clone)]
pub struct EvalContext<'a> {
    pub generics: &'a GenericMap,
    pub num_rows: usize,

    // validity from the nullable column
    pub validity: Option<Bitmap>,
    pub errors: Option<(MutableBitmap, String)>,
    pub tz: Tz,
}

impl<'a> EvalContext<'a> {
    #[inline]
    pub fn set_error(&mut self, row: usize, error_msg: impl AsRef<str>) {
        // if the row is invalid, we don't need to set error
        if self
            .validity
            .as_ref()
            .map(|b| !b.get_bit(row))
            .unwrap_or(false)
        {
            return;
        }

        match self.errors.as_mut() {
            Some((valids, _)) => {
                valids.set(row, false);
            }
            None => {
                let mut valids = constant_bitmap(true, self.num_rows.max(1));
                valids.set(row, false);
                self.errors = Some((valids, error_msg.as_ref().to_string()));
            }
        }
    }

    pub fn render_error(&self, args: &[Value<AnyType>], name: &str) -> Result<(), String> {
        match &self.errors {
            Some((valids, error)) => {
                let first_error_row = valids
                    .iter()
                    .enumerate()
                    .filter(|(_, valid)| !valid)
                    .take(1)
                    .next()
                    .unwrap()
                    .0;
                let args = args
                    .iter()
                    .map(|arg| {
                        let arg_ref = arg.as_ref();
                        arg_ref.index(first_error_row).unwrap().to_string()
                    })
                    .join(", ");

                let error_msg = format!("{} during evaluate function: {}({})", error, name, args);
                Err(error_msg)
            }
            None => Ok(()),
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
    TryAdaptor(TryAdaptor),
}

pub struct Function {
    pub signature: FunctionSignature,
    #[allow(clippy::type_complexity)]
    pub calc_domain: Box<dyn Fn(&[Domain]) -> FunctionDomain<AnyType> + Send + Sync>,
    #[allow(clippy::type_complexity)]
    pub eval: Box<dyn Fn(&[ValueRef<AnyType>], &mut EvalContext) -> Value<AnyType> + Send + Sync>,
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
    /// Aliases map from alias function name to original function name.
    pub aliases: HashMap<String, String>,

    /// fn name to cast signatures
    pub auto_cast_signatures: HashMap<String, AutoCastSignature>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn registered_names(&self) -> Vec<String> {
        self.funcs
            .keys()
            .chain(self.factories.keys())
            .chain(self.aliases.keys())
            .unique()
            .cloned()
            .collect()
    }

    pub fn contains(&self, func_name: &str) -> bool {
        self.funcs.contains_key(func_name)
            || self.factories.contains_key(func_name)
            || self.aliases.contains_key(func_name)
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

            FunctionID::TryAdaptor(adaptor) => {
                let inner = self.get(&adaptor.function_id)?;
                Some(adaptor.get_function(inner))
            }
        }
    }

    pub fn get_casting_rules(&self, func_name: &str) -> Option<&AutoCastSignature> {
        self.auto_cast_signatures.get(func_name)
    }

    pub fn search_candidates<Index: ColumnIndex>(
        &self,
        name: &str,
        params: &[usize],
        args: &[Expr<Index>],
    ) -> Vec<(FunctionID, Arc<Function>)> {
        let name = name.to_lowercase();

        let mut candidates = Vec::new();

        if name.starts_with("try_") {
            let inner_name = &name[4..];
            for (inner_function_id, inner_function) in
                self.search_candidates(inner_name, params, args)
            {
                let adpator = TryAdaptor {
                    function_id: Box::new(inner_function_id),
                };
                let function = adpator.get_function(inner_function);
                candidates.push((FunctionID::TryAdaptor(adpator), function));
            }
        }

        if let Some(funcs) = self.funcs.get(&name) {
            candidates.extend(funcs.iter().enumerate().filter_map(|(id, func)| {
                if func.signature.name == name && func.signature.args_type.len() == args.len() {
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
            }));
        }

        if let Some(factories) = self.factories.get(&name) {
            let args_type = args
                .iter()
                .map(Expr::data_type)
                .cloned()
                .collect::<Vec<_>>();
            candidates.extend(factories.iter().enumerate().filter_map(|(id, factory)| {
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
            }));
        }

        candidates
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

    pub fn register_auto_cast_signatures(&mut self, fn_name: &str, signatures: AutoCastSignature) {
        self.auto_cast_signatures
            .entry(fn_name.to_string())
            .or_insert_with(Vec::new)
            .extend(signatures);
    }
}

pub fn wrap_nullable<F>(
    f: F,
) -> impl Fn(&[ValueRef<AnyType>], &mut EvalContext) -> Value<AnyType> + Copy
where F: Fn(&[ValueRef<AnyType>], &mut EvalContext) -> Value<AnyType> + Copy {
    move |args, ctx| {
        type T = NullableType<AnyType>;
        type Result = AnyType;

        let mut bitmap: Option<MutableBitmap> = None;
        let mut nonull_args: Vec<ValueRef<Result>> = Vec::with_capacity(args.len());

        let mut len = 1;
        for arg in args {
            let arg = arg.try_downcast::<T>().unwrap();
            match arg {
                ValueRef::Scalar(None) => return Value::Scalar(Scalar::Null),
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
        let results = f(&nonull_args, ctx);
        let bitmap = bitmap.unwrap_or_else(|| constant_bitmap(true, len));
        match results {
            Value::Scalar(s) => {
                if bitmap.get(0) {
                    Value::Scalar(s)
                } else {
                    Value::Scalar(Scalar::Null)
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
                Value::Column(result)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TryAdaptor {
    function_id: Box<FunctionID>,
}

impl TryAdaptor {
    pub fn get_function(&self, inner: Arc<Function>) -> Arc<Function> {
        let mut sig = inner.signature.clone();
        sig.name = format!("try_{}", sig.name);
        sig.return_type = sig.return_type.wrap_nullable();

        let inner_c = inner.clone();
        let calc_domain =
            Box::new(
                move |domains: &[Domain]| match (inner_c.calc_domain)(domains) {
                    FunctionDomain::Domain(d) => match d {
                        Domain::Nullable(other) => {
                            FunctionDomain::Domain(NullableType::<AnyType>::upcast_domain(other))
                        }
                        other => {
                            let d = NullableDomain {
                                has_null: false,
                                value: Some(Box::new(other)),
                            };
                            FunctionDomain::Domain(NullableType::<AnyType>::upcast_domain(d))
                        }
                    },
                    // Here we convert full to full, this may lose some internal information since it's runtime adpator
                    FunctionDomain::Full | FunctionDomain::MayThrow => FunctionDomain::Full,
                },
            );

        Arc::new(Function {
            signature: sig,
            calc_domain,
            eval: Box::new(move |args, ctx| {
                let res = (inner.eval)(args, ctx);
                let validity: Option<Bitmap> = ctx.errors.take().map(|v| v.0.into());
                match res {
                    Value::Scalar(s) => match validity {
                        Some(v) if v.get_bit(0) => Value::Scalar(s),
                        None => Value::Scalar(s),
                        _ => Value::Scalar(Scalar::Null),
                    },
                    Value::Column(c) => Value::Column(c.wrap_nullable(validity)),
                }
            }),
        })
    }
}
