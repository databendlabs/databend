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
use std::fmt::Debug;
use std::ops::BitAnd;
use std::ops::BitOr;
use std::ops::Not;
use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_io::GeometryDataType;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use jiff::tz::TimeZone;
use jiff::Zoned;
use serde::Deserialize;
use serde::Serialize;

use crate::property::Domain;
use crate::property::FunctionProperty;
use crate::type_check::try_unify_signature;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableDomain;
use crate::types::*;
use crate::values::Value;
use crate::Column;
use crate::ColumnIndex;
use crate::Expr;
use crate::FunctionDomain;
use crate::Scalar;

pub type AutoCastRules<'a> = &'a [(DataType, DataType)];

/// A function to build function depending on the const parameters and the type of arguments (before coercion).
///
/// The first argument is the const parameters and the second argument is the types of arguments.
pub trait FunctionFactoryClosure =
    Fn(&[Scalar], &[DataType]) -> Option<Arc<Function>> + Send + Sync + 'static;

pub struct FunctionFactoryHelper {
    fixed_arg_count: Option<usize>,
    passthrough_nullable: bool,
    create: Box<dyn FunctionFactoryClosure>,
}

impl Debug for FunctionFactoryHelper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionFactoryHelper")
            .field("fixed_arg_count", &self.fixed_arg_count)
            .field("passthrough_nullable", &self.passthrough_nullable)
            .finish()
    }
}

impl FunctionFactoryHelper {
    pub fn create_1_arg_core(
        create: fn(&[Scalar], &DataType) -> Option<Function>,
    ) -> FunctionFactory {
        FunctionFactory::Helper(Self {
            fixed_arg_count: Some(1),
            passthrough_nullable: false,
            create: Box::new(move |params, args: &[DataType]| match args {
                [arg0] => create(params, arg0).map(Arc::new),
                _ => None,
            }),
        })
    }

    pub fn create_1_arg_passthrough_nullable(
        create: fn(&[Scalar], &DataType) -> Option<Function>,
    ) -> FunctionFactory {
        FunctionFactory::Helper(Self {
            fixed_arg_count: Some(1),
            passthrough_nullable: true,
            create: Box::new(move |params, args: &[DataType]| match args {
                [DataType::Nullable(box arg0)] => {
                    create(params, arg0).map(|func| Arc::new(func.passthrough_nullable()))
                }
                [arg0] => create(params, arg0).map(Arc::new),
                _ => None,
            }),
        })
    }
}

pub enum FunctionFactory {
    Closure(Box<dyn FunctionFactoryClosure>),
    Helper(FunctionFactoryHelper),
}

impl FunctionFactory {
    fn create(&self, params: &[Scalar], args: &[DataType]) -> Option<Arc<Function>> {
        match self {
            FunctionFactory::Closure(closure) => closure(params, args),
            FunctionFactory::Helper(factory) => (factory.create)(params, args),
        }
    }
}

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
        eval: Box<dyn Fn(&[Value<AnyType>], &mut EvalContext) -> Value<AnyType> + Send + Sync>,
    },
    /// Set-returning-function that input a scalar and then return a set.
    SRF {
        /// Given multiple rows, return multiple sets of results
        /// for each input row, along with the number of rows in each set.
        eval: Box<
            dyn Fn(
                    &[Value<AnyType>],
                    &mut EvalContext,
                    &mut [usize],
                ) -> Vec<(Value<AnyType>, usize)>
                + Send
                + Sync,
        >,
    },
}

#[derive(Clone)]
pub struct FunctionContext {
    pub tz: TimeZone,
    pub now: Zoned,
    pub rounding_mode: bool,
    pub disable_variant_check: bool,

    pub openai_api_chat_base_url: String,
    pub openai_api_embedding_base_url: String,
    pub openai_api_key: String,
    pub openai_api_version: String,
    pub openai_api_embedding_model: String,
    pub openai_api_completion_model: String,

    pub geometry_output_format: GeometryDataType,
    pub parse_datetime_ignore_remainder: bool,
    pub enable_strict_datetime_parser: bool,
    pub random_function_seed: bool,
}

impl Default for FunctionContext {
    fn default() -> Self {
        FunctionContext {
            tz: TimeZone::UTC,
            now: Default::default(),
            rounding_mode: false,
            disable_variant_check: false,
            openai_api_chat_base_url: "".to_string(),
            openai_api_embedding_base_url: "".to_string(),
            openai_api_key: "".to_string(),
            openai_api_version: "".to_string(),
            openai_api_embedding_model: "".to_string(),
            openai_api_completion_model: "".to_string(),

            geometry_output_format: Default::default(),
            parse_datetime_ignore_remainder: false,
            enable_strict_datetime_parser: true,
            random_function_seed: false,
        }
    }
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
    pub suppress_error: bool,
    /// At the top level expressions require eval to have stricter behavior
    /// to ensure that some internal complexity is not leaking out
    pub strict_eval: bool,
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
        params: Vec<Scalar>,
        args_type: Vec<DataType>,
    },
}

#[derive(Default)]
pub struct FunctionRegistry {
    pub funcs: HashMap<String, Vec<(Arc<Function>, usize)>>,
    #[allow(clippy::type_complexity)]
    pub factories: HashMap<String, Vec<(FunctionFactory, usize)>>,

    /// Aliases map from alias function name to original function name.
    pub aliases: HashMap<String, String>,

    /// Default cast rules for all functions.
    pub default_cast_rules: Vec<(DataType, DataType)>,
    /// Cast rules for specific functions, which will override the default cast rules.
    pub additional_cast_rules: HashMap<String, Vec<(DataType, DataType)>>,
    /// The auto rules that should use TRY_CAST instead of CAST.
    pub auto_try_cast_rules: Vec<(DataType, DataType)>,

    pub properties: HashMap<String, FunctionProperty>,
}

impl Function {
    pub fn passthrough_nullable(self) -> Self {
        debug_assert!(self
            .signature
            .args_type
            .iter()
            .all(|ty| !ty.is_nullable_or_null()));

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
        let new_eval = Box::new(move |val: &[Value<AnyType>], ctx: &mut EvalContext| {
            let num_rows = ctx.num_rows;
            let output = eval(val, ctx);
            if let Some((validity, _)) = ctx.errors.take() {
                match output {
                    Value::Scalar(_) => Value::Scalar(Scalar::Null),
                    Value::Column(column) => {
                        Value::Column(NullableColumn::new_column(column, validity.into()))
                    }
                }
            } else {
                match output {
                    Value::Scalar(scalar) => Value::Scalar(scalar),
                    Value::Column(column) => Value::Column(NullableColumn::new_column(
                        column,
                        Bitmap::new_constant(true, num_rows),
                    )),
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
                factory.create(params, args_type)
            }
        }
    }

    pub fn search_candidates<Index: ColumnIndex>(
        &self,
        name: &str,
        params: &[Scalar],
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
                factory.create(params, &args_type).map(|func| {
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

    // note that if additional_cast_rules is not empty, default cast rules will not be used.
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
            Some(self.properties.get(&func_name).cloned().unwrap_or_default())
        } else {
            None
        }
    }

    pub fn register_function(&mut self, func: Function) {
        let name = func.signature.name.clone();
        let id = self.next_function_id(&name);
        self.funcs
            .entry(name)
            .or_default()
            .push((Arc::new(func), id));
    }

    pub fn register_function_factory(&mut self, name: &str, factory: FunctionFactory) {
        let id = self.next_function_id(name);
        self.factories
            .entry(name.to_string())
            .or_default()
            .push((factory, id));
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
        self.default_cast_rules.extend(default_cast_rules);
    }

    // Note that, if additional_cast_rules is not empty, the default cast rules will not be used
    pub fn register_additional_cast_rules(
        &mut self,
        fn_name: &str,
        additional_cast_rules: impl IntoIterator<Item = (DataType, DataType)>,
    ) {
        self.additional_cast_rules
            .entry(fn_name.to_string())
            .or_default()
            .extend(additional_cast_rules);
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
                            .filter_map(|(factory, _)| {
                                factory.create(&[], &former.signature.args_type)
                            }),
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

    pub fn params(&self) -> &[Scalar] {
        match self {
            FunctionID::Builtin { .. } => &[],
            FunctionID::Factory { params, .. } => params.as_slice(),
        }
    }
}

impl EvalContext<'_> {
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

    pub fn has_error(&self, index: usize) -> bool {
        if self.suppress_error {
            return false;
        }
        match &self.errors {
            Some((b, _)) if index < b.len() => !b.get(index),
            _ => false,
        }
    }

    pub fn render_error(
        span: Span,
        errors: &Option<(MutableBitmap, String)>,
        params: &[Scalar],
        args: &[Value<AnyType>],
        func_name: &str,
        expr_name: &str,
        selection: Option<&[u32]>,
    ) -> Result<()> {
        let Some((valids, error)) = errors else {
            return Ok(());
        };

        let first_error_row = match selection {
            None => valids.iter().enumerate().find(|(_, v)| !v).unwrap().0,
            Some(selection) if valids.len() == 1 => {
                if valids.get(0) || selection.is_empty() {
                    return Ok(());
                }

                selection.first().map(|x| *x as usize).unwrap()
            }
            Some(selection) => {
                let Some(first_invalid) = selection.iter().find(|idx| !valids.get(**idx as usize))
                else {
                    return Ok(());
                };

                *first_invalid as usize
            }
        };

        let args = args
            .iter()
            .map(|arg| arg.index(first_error_row).unwrap().to_string())
            .join(", ");

        let err_msg = if params.is_empty() {
            format!("{error} while evaluating function `{func_name}({args})` in expr `{expr_name}`")
        } else {
            format!(
                    "{error} while evaluating function `{func_name}({params})({args})` in expr `{expr_name}`",
                    params = params.iter().join(", ")
                )
        };

        Err(ErrorCode::BadArguments(err_msg).set_span(span))
    }
}

pub fn passthrough_nullable<F>(
    f: F,
) -> impl Fn(&[Value<AnyType>], &mut EvalContext) -> Value<AnyType>
where F: Fn(&[Value<AnyType>], &mut EvalContext) -> Value<AnyType> {
    move |args, ctx| {
        type T = NullableType<AnyType>;
        type Result = AnyType;

        let mut bitmap: Option<MutableBitmap> = None;
        let mut nonull_args: Vec<Value<Result>> = Vec::with_capacity(args.len());

        let mut len = 1;
        for arg in args {
            let arg = arg.try_downcast::<T>().unwrap();
            match arg {
                Value::Scalar(None) => return Value::Scalar(Scalar::Null),
                Value::Scalar(Some(s)) => {
                    nonull_args.push(Value::Scalar(s.clone()));
                }
                Value::Column(v) => {
                    len = v.len();
                    nonull_args.push(Value::Column(v.column.clone()));
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
            if res.null_count() == 0 {
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
                        let validity: Bitmap = bitmap.into();
                        let validity = databend_common_column::bitmap::and(
                            &nullable_column.validity,
                            &validity,
                        );

                        NullableColumn::new_column(nullable_column.column, validity)
                    }
                    _ => NullableColumn::new_column(column, bitmap.into()),
                };
                Value::Column(result)
            }
        }
    }
}

pub fn error_to_null<I1: AccessType, O: ArgType>(
    func: impl Fn(Value<I1>, &mut EvalContext) -> Value<O> + Copy + Send + Sync,
) -> impl Fn(Value<I1>, &mut EvalContext) -> Value<NullableType<O>> + Copy + Send + Sync {
    debug_assert!(!O::data_type().is_nullable_or_null());
    move |val, ctx| {
        let output = func(val, ctx);
        if let Some((validity, _)) = ctx.errors.take() {
            match output {
                Value::Scalar(_) => Value::Scalar(None),
                Value::Column(column) => {
                    Value::Column(NullableColumn::new(column, validity.into()))
                }
            }
        } else {
            match output {
                Value::Scalar(scalar) => Value::Scalar(Some(scalar)),
                Value::Column(column) => Value::Column(NullableColumn::new(
                    column,
                    Bitmap::new_constant(true, ctx.num_rows),
                )),
            }
        }
    }
}
