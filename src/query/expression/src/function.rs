// Copyright 2021 Datafuse Labs
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

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::BitAnd;
use std::ops::BitOr;
use std::ops::Not;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;

use crate::date_helper::TzLUT;
use crate::property::Domain;
use crate::property::FunctionProperty;
use crate::type_check::try_unify_signature;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableDomain;
use crate::types::*;
use crate::values::Value;
use crate::values::ValueRef;
use crate::Column;
use crate::ColumnIndex;
use crate::Expr;
use crate::FunctionDomain;
use crate::Scalar;

pub type AutoCastRules<'a> = &'a [(DataType, DataType)];
/// A function to build function depending on the const parameters and the type of arguments (before coercion).
///
/// The first argument is the const parameters and the second argument is the types of arguments.
pub trait FunctionFactory =
    Fn(&[usize], &[DataType]) -> Option<Arc<Function>> + Send + Sync + 'static;

pub struct Function {
    pub signature: FunctionSignature,
    pub eval: FunctionEval,
}

#[derive(Debug, Clone)]
pub struct FunctionSignature {
    pub name: String,
    pub args_type: Vec<DataType>,
    pub return_type: DataType,
}

#[derive(EnumAsInner)]
#[allow(clippy::type_complexity)]
pub enum FunctionEval {
    /// Scalar function that returns a single value.
    Scalar {
        /// Given the domains of the arguments, return the domain of the output value.
        calc_domain:
            Box<dyn Fn(&FunctionContext, &[Domain]) -> FunctionDomain<AnyType> + Send + Sync>,
        /// Given a set of arguments, return a single value.
        /// The result must be in the same length as the input arguments if its a column.
        eval: Box<dyn Fn(&[ValueRef<AnyType>], &mut EvalContext) -> Value<AnyType> + Send + Sync>,
    },
    /// Set-returning-function that input a scalar and then return a set.
    SRF {
        /// Given multiple rows, return multiple sets of results
        /// for each input row, along with the number of rows in each set.
        eval: Box<
            dyn Fn(
                    &[ValueRef<AnyType>],
                    &mut EvalContext,
                    &mut [usize],
                ) -> Vec<(Value<AnyType>, usize)>
                + Send
                + Sync,
        >,
    },
}

#[derive(Clone, Default)]
pub struct FunctionContext {
    pub tz: TzLUT,

    pub openai_api_chat_base_url: String,
    pub openai_api_embedding_base_url: String,
    pub openai_api_key: String,
    pub openai_api_version: String,
    pub openai_api_embedding_model: String,
    pub openai_api_completion_model: String,
}

#[derive(Clone)]
pub struct EvalContext<'a> {
    pub generics: &'a GenericMap,
    pub num_rows: usize,

    pub func_ctx: &'a FunctionContext,
    /// Validity bitmap of outer nullable column. This is an optimization
    /// to avoid recording errors on the NULL value which has a corresponding
    /// default value in nullable's inner column.
    pub validity: Option<Bitmap>,
    pub errors: Option<(MutableBitmap, String)>,
}

/// `FunctionID` is a unique identifier for a function in the registry. It's used to
/// construct the exactly same function in the remote execution nodes.
#[derive(Debug, Clone, PartialEq, Hash, Eq, Serialize, Deserialize)]
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

#[derive(Default)]
pub struct FunctionRegistry {
    pub funcs: HashMap<String, Vec<(Arc<Function>, usize)>>,
    #[allow(clippy::type_complexity)]
    pub factories: HashMap<String, Vec<(Box<dyn FunctionFactory>, usize)>>,

    /// Aliases map from alias function name to original function name.
    pub aliases: HashMap<String, String>,

    /// Default cast rules for all functions.
    pub default_cast_rules: Vec<(DataType, DataType)>,
    /// Cast rules for specific functions, in addition to default cast rules.
    pub additional_cast_rules: HashMap<String, Vec<(DataType, DataType)>>,
    /// The auto rules that should use TRY_CAST instead of CAST.
    pub auto_try_cast_rules: Vec<(DataType, DataType)>,

    pub properties: HashMap<String, FunctionProperty>,
}

impl Function {
    pub fn passthrough_nullable(self) -> Self {
        debug_assert!(
            !self
                .signature
                .args_type
                .iter()
                .any(|ty| ty.is_nullable_or_null())
        );

        let (calc_domain, eval) = self.eval.into_scalar().unwrap();

        let signature = FunctionSignature {
            name: self.signature.name.clone(),
            args_type: self
                .signature
                .args_type
                .iter()
                .map(|ty| ty.wrap_nullable())
                .collect(),
            return_type: self.signature.return_type.wrap_nullable(),
        };

        let new_calc_domain = Box::new(move |ctx: &FunctionContext, domains: &[Domain]| {
            let mut args_has_null = false;
            let mut args_domain = Vec::with_capacity(domains.len());
            for domain in domains {
                match domain {
                    Domain::Nullable(NullableDomain {
                        has_null,
                        value: Some(value),
                    }) => {
                        args_has_null = args_has_null || *has_null;
                        args_domain.push(value.as_ref().clone());
                    }
                    Domain::Nullable(NullableDomain { value: None, .. }) => {
                        return FunctionDomain::Domain(Domain::Nullable(NullableDomain {
                            has_null: true,
                            value: None,
                        }));
                    }
                    _ => unreachable!(),
                }
            }

            calc_domain(ctx, &args_domain).map(|result_domain| {
                let (result_has_null, result_domain) = match result_domain {
                    Domain::Nullable(NullableDomain { has_null, value }) => (has_null, value),
                    domain => (false, Some(Box::new(domain))),
                };
                Domain::Nullable(NullableDomain {
                    has_null: args_has_null || result_has_null,
                    value: result_domain,
                })
            })
        });

        Function {
            signature,
            eval: FunctionEval::Scalar {
                calc_domain: new_calc_domain,
                eval: Box::new(passthrough_nullable(eval)),
            },
        }
    }

    pub fn error_to_null(self) -> Self {
        debug_assert!(!self.signature.return_type.is_nullable_or_null());

        let mut signature = self.signature;
        signature.return_type = signature.return_type.wrap_nullable();

        let (calc_domain, eval) = self.eval.into_scalar().unwrap();

        let new_calc_domain = Box::new(move |ctx: &FunctionContext, domains: &[Domain]| {
            let domain = calc_domain(ctx, domains);
            match domain {
                FunctionDomain::Domain(domain) => {
                    let new_domain = NullableDomain {
                        has_null: false,
                        value: Some(Box::new(domain)),
                    };
                    FunctionDomain::Domain(NullableType::<AnyType>::upcast_domain(new_domain))
                }
                FunctionDomain::Full | FunctionDomain::MayThrow => FunctionDomain::Full,
            }
        });
        let new_eval = Box::new(move |val: &[ValueRef<AnyType>], ctx: &mut EvalContext| {
            let num_rows = ctx.num_rows;
            let output = eval(val, ctx);
            if let Some((validity, _)) = ctx.errors.take() {
                match output {
                    Value::Scalar(_) => Value::Scalar(Scalar::Null),
                    Value::Column(column) => {
                        Value::Column(Column::Nullable(Box::new(NullableColumn {
                            column,
                            validity: validity.into(),
                        })))
                    }
                }
            } else {
                match output {
                    Value::Scalar(scalar) => Value::Scalar(scalar),
                    Value::Column(column) => {
                        Value::Column(Column::Nullable(Box::new(NullableColumn {
                            column,
                            validity: Bitmap::new_constant(true, num_rows),
                        })))
                    }
                }
            }
        });

        Function {
            signature,
            eval: FunctionEval::Scalar {
                calc_domain: new_calc_domain,
                eval: new_eval,
            },
        }
    }
}

impl FunctionRegistry {
    pub fn empty() -> Self {
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
            FunctionID::Builtin { name, id } => self
                .funcs
                .get(name.as_str())?
                .iter()
                .find(|(_, func_id)| func_id == id)
                .map(|(func, _)| func.clone()),
            FunctionID::Factory {
                name,
                id,
                params,
                args_type,
            } => {
                let factory = self
                    .factories
                    .get(name.as_str())?
                    .iter()
                    .find(|(_, func_id)| func_id == id)
                    .map(|(func, _)| func)?;
                factory(params, args_type)
            }
        }
    }

    pub fn search_candidates<Index: ColumnIndex>(
        &self,
        name: &str,
        params: &[usize],
        args: &[Expr<Index>],
    ) -> Vec<(FunctionID, Arc<Function>)> {
        let name = name.to_lowercase();

        let mut candidates = Vec::new();

        if let Some(funcs) = self.funcs.get(&name) {
            candidates.extend(funcs.iter().filter_map(|(func, id)| {
                if func.signature.name == name && func.signature.args_type.len() == args.len() {
                    Some((
                        FunctionID::Builtin {
                            name: name.to_string(),
                            id: *id,
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
            candidates.extend(factories.iter().filter_map(|(factory, id)| {
                factory(params, &args_type).map(|func| {
                    (
                        FunctionID::Factory {
                            name: name.to_string(),
                            id: *id,
                            params: params.to_vec(),
                            args_type: args_type.clone(),
                        },
                        func,
                    )
                })
            }));
        }

        candidates.sort_by_key(|(id, _)| id.id());

        candidates
    }

    pub fn get_auto_cast_rules(&self, func_name: &str) -> &[(DataType, DataType)] {
        self.additional_cast_rules
            .get(func_name)
            .unwrap_or(&self.default_cast_rules)
    }

    pub fn is_auto_try_cast_rule(&self, arg_type: &DataType, sig_type: &DataType) -> bool {
        self.auto_try_cast_rules
            .iter()
            .any(|(src_ty, dest_ty)| arg_type == src_ty && sig_type == dest_ty)
    }

    pub fn get_property(&self, func_name: &str) -> Option<FunctionProperty> {
        let func_name = func_name.to_lowercase();
        if self.contains(&func_name) {
            Some(
                self.properties
                    .get(&func_name.to_lowercase())
                    .cloned()
                    .unwrap_or_default(),
            )
        } else {
            None
        }
    }

    pub fn register_function(&mut self, func: Function) {
        let name = func.signature.name.clone();
        let id = self.next_function_id(&name);
        self.funcs
            .entry(name)
            .or_insert_with(Vec::new)
            .push((Arc::new(func), id));
    }

    pub fn register_function_factory(&mut self, name: &str, factory: impl FunctionFactory) {
        let id = self.next_function_id(name);
        self.factories
            .entry(name.to_string())
            .or_insert_with(Vec::new)
            .push((Box::new(factory), id));
    }

    pub fn register_aliases(&mut self, fn_name: &str, aliases: &[&str]) {
        for alias in aliases {
            self.aliases.insert(alias.to_string(), fn_name.to_string());
        }
    }

    pub fn register_default_cast_rules(
        &mut self,
        default_cast_rules: impl IntoIterator<Item = (DataType, DataType)>,
    ) {
        self.default_cast_rules
            .extend(default_cast_rules.into_iter());
    }

    pub fn register_additional_cast_rules(
        &mut self,
        fn_name: &str,
        additional_cast_rules: impl IntoIterator<Item = (DataType, DataType)>,
    ) {
        self.additional_cast_rules
            .entry(fn_name.to_string())
            .or_insert_with(Vec::new)
            .extend(additional_cast_rules.into_iter());
    }

    pub fn register_auto_try_cast_rules(
        &mut self,
        auto_try_cast_rules: impl IntoIterator<Item = (DataType, DataType)>,
    ) {
        self.auto_try_cast_rules.extend(auto_try_cast_rules);
    }

    pub fn next_function_id(&self, name: &str) -> usize {
        self.funcs.get(name).map(|funcs| funcs.len()).unwrap_or(0)
            + self.factories.get(name).map(|f| f.len()).unwrap_or(0)
    }

    pub fn all_function_names(&self) -> Vec<String> {
        self.aliases
            .keys()
            .chain(self.funcs.keys())
            .chain(self.factories.keys())
            .map(|s| s.to_string())
            .sorted()
            .dedup()
            .collect()
    }

    pub fn check_ambiguity(&self) {
        for (name, funcs) in &self.funcs {
            let auto_cast_rules = self.get_auto_cast_rules(name);
            for (former, former_id) in funcs {
                for latter in funcs
                    .iter()
                    .filter(|(_, id)| id > former_id)
                    .map(|(func, _)| func.clone())
                    .chain(
                        self.factories
                            .get(name)
                            .map(Vec::as_slice)
                            .unwrap_or(&[])
                            .iter()
                            .filter(|(_, id)| id > former_id)
                            .filter_map(|(factory, _)| factory(&[], &former.signature.args_type)),
                    )
                {
                    if former.signature.args_type.len() == latter.signature.args_type.len() {
                        if let Ok(subst) = try_unify_signature(
                            latter.signature.args_type.iter(),
                            former.signature.args_type.iter(),
                            auto_cast_rules,
                        ) {
                            if subst.apply(&former.signature.return_type).is_ok()
                                && former
                                    .signature
                                    .args_type
                                    .iter()
                                    .all(|sig_ty| subst.apply(sig_ty).is_ok())
                            {
                                panic!(
                                    "Ambiguous signatures for function:\n- {}\n- {}\n\
                                        Suggestion: swap the order of the overloads.",
                                    former.signature, latter.signature
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

impl FunctionID {
    pub fn id(&self) -> usize {
        match self {
            FunctionID::Builtin { id, .. } => *id,
            FunctionID::Factory { id, .. } => *id,
        }
    }

    pub fn name(&self) -> Cow<'_, str> {
        match self {
            FunctionID::Builtin { name, .. } => Cow::Borrowed(name),
            FunctionID::Factory { name, .. } => Cow::Borrowed(name),
        }
    }

    pub fn params(&self) -> &[usize] {
        match self {
            FunctionID::Builtin { .. } => &[],
            FunctionID::Factory { params, .. } => params.as_slice(),
        }
    }
}

impl<'a> EvalContext<'a> {
    #[inline]
    pub fn set_error(&mut self, row: usize, error_msg: impl Into<String>) {
        // If the row is NULL, we don't need to set error.
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
                let mut valids = Bitmap::new_constant(true, self.num_rows.max(1)).make_mut();
                valids.set(row, false);
                self.errors = Some((valids, error_msg.into()));
            }
        }
    }

    pub fn render_error(
        &self,
        span: Span,
        params: &[usize],
        args: &[Value<AnyType>],
        func_name: &str,
    ) -> Result<()> {
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

                let err_msg = if params.is_empty() {
                    format!("{error} while evaluating function `{func_name}({args})`")
                } else {
                    format!(
                        "{error} while evaluating function `{func_name}({params})({args})`",
                        params = params.iter().join(", ")
                    )
                };

                Err(ErrorCode::Internal(err_msg).set_span(span))
            }
            None => Ok(()),
        }
    }
}

pub fn passthrough_nullable<F>(
    f: F,
) -> impl Fn(&[ValueRef<AnyType>], &mut EvalContext) -> Value<AnyType>
where F: Fn(&[ValueRef<AnyType>], &mut EvalContext) -> Value<AnyType> {
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
        let bitmap = bitmap.unwrap_or_else(|| Bitmap::new_constant(true, len).make_mut());
        if let Some((error_bitmap, _)) = ctx.errors.as_mut() {
            // If the original value is NULL, we can ignore the error.
            let rhs: Bitmap = bitmap.clone().not().into();
            let res = error_bitmap.clone().bitor(&rhs);
            if res.unset_bits() == 0 {
                ctx.errors = None;
            } else {
                *error_bitmap = res;
            }
        }

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

pub fn error_to_null<I1: ArgType, O: ArgType>(
    func: impl for<'a> Fn(ValueRef<'a, I1>, &mut EvalContext) -> Value<O> + Copy + Send + Sync,
) -> impl for<'a> Fn(ValueRef<'a, I1>, &mut EvalContext) -> Value<NullableType<O>> + Copy + Send + Sync
{
    debug_assert!(!O::data_type().is_nullable_or_null());
    move |val, ctx| {
        let output = func(val, ctx);
        if let Some((validity, _)) = ctx.errors.take() {
            match output {
                Value::Scalar(_) => Value::Scalar(None),
                Value::Column(column) => Value::Column(NullableColumn {
                    column,
                    validity: validity.into(),
                }),
            }
        } else {
            match output {
                Value::Scalar(scalar) => Value::Scalar(Some(scalar)),
                Value::Column(column) => Value::Column(NullableColumn {
                    column,
                    validity: Bitmap::new_constant(true, ctx.num_rows),
                }),
            }
        }
    }
}
