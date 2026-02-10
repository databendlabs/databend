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
use std::fmt;
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
use databend_common_io::prelude::BinaryDisplayFormat;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use jiff::Zoned;
use jiff::tz::TimeZone;
use serde::Deserialize;
use serde::Serialize;

use self::function_factory::FunctionFactoryHelper;
use crate::Column;
use crate::ColumnIndex;
use crate::Expr;
use crate::FunctionDomain;
use crate::Scalar;
use crate::function_stat::DeriveStat;
use crate::function_stat::ScalarFunctionStat;
use crate::property::Domain;
use crate::property::FunctionProperty;
use crate::type_check::try_unify_signature;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableDomain;
use crate::types::*;
use crate::values::Value;

pub mod function_builder;
pub mod function_factory;
pub mod function_stat;
pub mod register;
pub mod register_comparison;
pub mod register_vectorize;

pub type AutoCastRules<'a> = &'a [(DataType, DataType)];
pub type DynamicCastRules = Vec<Arc<dyn Fn(&DataType, &DataType) -> bool + Send + Sync>>;

/// A function to build function depending on the const parameters and the type of arguments (before coercion).
///
/// The first argument is the const parameters and the second argument is the types of arguments.
pub trait FunctionFactoryClosure =
    Fn(&[Scalar], &[DataType]) -> Option<Arc<Function>> + Send + Sync + 'static;

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

pub trait ScalarFunctionDomain: Send + Sync + 'static {
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType>;
}

pub trait ScalarFunction: Send + Sync + 'static {
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType>;
}

#[derive(EnumAsInner)]
pub enum FunctionEval {
    /// Scalar function that returns a single value.
    Scalar {
        /// Given the domains of the arguments, return the domain of the output value.
        calc_domain: Box<dyn ScalarFunctionDomain>,

        derive_stat: Option<Box<dyn ScalarFunctionStat>>,
        /// Given a set of arguments, return a single value.
        /// The result must be in the same length as the input arguments if its a column.
        eval: Box<dyn ScalarFunction>,
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

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionContext {
    pub tz: TimeZone,
    pub now: Zoned,
    pub rounding_mode: bool,
    pub disable_variant_check: bool,
    pub enable_selector_executor: bool,

    pub geometry_output_format: GeometryDataType,
    pub binary_input_format: BinaryDisplayFormat,
    pub binary_output_format: BinaryDisplayFormat,
    pub parse_datetime_ignore_remainder: bool,
    pub enable_strict_datetime_parser: bool,
    pub random_function_seed: bool,
    pub week_start: u8,
    pub date_format_style: String,
}

impl Default for FunctionContext {
    fn default() -> Self {
        FunctionContext {
            tz: TimeZone::UTC,
            now: Default::default(),
            rounding_mode: false,
            disable_variant_check: false,
            enable_selector_executor: true,

            geometry_output_format: Default::default(),
            binary_input_format: BinaryDisplayFormat::Utf8,
            binary_output_format: BinaryDisplayFormat::Utf8,
            parse_datetime_ignore_remainder: false,
            enable_strict_datetime_parser: true,
            random_function_seed: false,
            week_start: 0,
            date_format_style: "oracle".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
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
    pub dynamic_cast_rules: HashMap<String, DynamicCastRules>,
    pub properties: HashMap<String, FunctionProperty>,
    pub derive_stat: HashMap<String, DeriveStat>,
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

    // Note that, if additional_cast_rules is not empty, the default cast rules will not be used
    pub fn register_dynamic_cast_rules(
        &mut self,
        fn_name: &str,
        rule: Arc<dyn Fn(&DataType, &DataType) -> bool + Send + Sync>,
    ) {
        self.dynamic_cast_rules
            .entry(fn_name.to_string())
            .or_default()
            .push(rule);
    }

    pub fn get_dynamic_cast_rules(&self, fn_name: &str) -> DynamicCastRules {
        self.dynamic_cast_rules
            .get(fn_name)
            .cloned()
            .unwrap_or_default()
    }

    pub fn register_auto_try_cast_rules(
        &mut self,
        auto_try_cast_rules: impl IntoIterator<Item = (DataType, DataType)>,
    ) {
        self.auto_try_cast_rules.extend(auto_try_cast_rules);
    }

    pub fn get_derive_stat(&self, func_name: &str) -> Option<&DeriveStat> {
        self.derive_stat.get(func_name)
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
            let dynamic_cast_rules = self.get_dynamic_cast_rules(name);
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
                            &dynamic_cast_rules,
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
    pub fn set_error(&mut self, row: usize, error_msg: impl fmt::Display) {
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
                self.errors = Some((valids, error_msg.to_string()));
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
                    Value::Column(NullableColumn::new_unchecked(column, validity.into()))
                }
            }
        } else {
            match output {
                Value::Scalar(scalar) => Value::Scalar(Some(scalar)),
                Value::Column(column) => Value::Column(NullableColumn::new_unchecked(
                    column,
                    Bitmap::new_constant(true, ctx.num_rows),
                )),
            }
        }
    }
}
