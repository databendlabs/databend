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

//! The steps that a cast takes.
//!
//! A cast does not map to one function: it may take no function (`NULL`, nullability, empty
//! containers), one function, a chain of casts, or a cast for each nested value. [`CastPlan`]
//! records these steps; the evaluator runs them and the constant folder derives their domain.
//!
//! The plan of `TRY_CAST` is the plan of `CAST` to the nullable destination. What happens with
//! a row that fails is decided where the plan runs: `CAST` reports an error, `TRY_CAST` turns
//! the row into NULL at the nearest nullable destination. A function only provides the
//! conversion of one step.
//!
//! A step that converts a value runs the function overload registered for its source and
//! destination types, and its domain is the domain of that function. Function overload
//! resolution does not take part: a cast without a registered function or chain has no plan.

use std::fmt;
use std::sync::Arc;

use crate::FunctionRegistry;
use crate::NullableInput;
use crate::Scalar;
use crate::function::Function;
use crate::function::FunctionID;
use crate::function::cast_key;
use crate::type_check::Substitution;
use crate::types::DataType;
use crate::types::NumberScalar;
use crate::types::VectorDataType;
use crate::types::decimal::DecimalSize;

/// How a cast from `src_type` to `dest_type` runs.
#[derive(Clone, Debug)]
pub struct CastPlan {
    pub src_type: DataType,
    pub dest_type: DataType,
    pub step: CastStep,
}

#[derive(Clone, Debug)]
pub enum CastStep {
    /// The value already has the layout of the destination.
    Identity,
    /// `NULL` to any type: NULL for a nullable destination, an error otherwise.
    Null,
    /// A cast function converts the value. With `returns_null`, the function returns NULL for
    /// some non-NULL values, e.g. a JSON null cast from Variant. With `reject_null`, the
    /// function returns the nullable destination type, and a NULL is an error.
    Function {
        call: CastCall,
        reject_null: bool,
        returns_null: bool,
    },
    /// Casts through intermediate types, one cast after another.
    Chain(Vec<CastPlan>),
    /// `T1 NULL -> T2 NULL`.
    Nullable(Box<CastPlan>),
    /// `T1 NULL -> T2`: a NULL is an error.
    FromNullable(Box<CastPlan>),
    /// `T1 -> T2 NULL`.
    ToNullable(Box<CastPlan>),
    EmptyArray,
    EmptyMap,
    /// Casts the elements of each array.
    Array(Box<CastPlan>),
    /// Casts the key-value tuples of each map.
    Map(Box<CastPlan>),
    /// Casts each field.
    Tuple(Vec<CastPlan>),
    /// Casts the elements of each JSON array; a value that is not an array has none.
    VariantToArray(Box<CastPlan>),
    /// Casts the values of each JSON object; a value that is not an object has none.
    VariantToMap(Box<CastPlan>),
    /// Converts a String, Variant or numeric Array value to a vector.
    ToVector,
}

/// The function overload that a cast step runs, on the value that the step converts.
#[derive(Clone, Debug)]
pub struct CastCall {
    pub id: FunctionID,
    pub function: Arc<Function>,
    pub generics: Vec<DataType>,
    pub return_type: DataType,
}

/// A registered overload of a cast function. A factory is instantiated for the types of each
/// step, e.g. with the precision and scale of the destination decimal.
#[derive(Clone, Copy, Debug)]
pub enum CastOverload {
    Builtin(usize),
    Factory(usize),
}

/// The function that a cast step runs.
#[derive(Clone, Debug)]
pub struct CastFunction {
    pub name: String,
    /// The overload for a non-NULL argument.
    pub overload: CastOverload,
    /// The overload for a nullable argument, which passes the NULLs through.
    pub nullable_overload: Option<CastOverload>,
    /// Whether the function returns NULL for some non-NULL values, e.g. a JSON null.
    pub returns_null: bool,
}

impl CastPlan {
    fn new(src_type: &DataType, dest_type: &DataType, step: CastStep) -> Self {
        CastPlan {
            src_type: src_type.clone(),
            dest_type: dest_type.clone(),
            step,
        }
    }
}

/// The plan of a cast, or `None` when the cast has no implementation.
pub fn resolve_cast_plan(
    src_type: &DataType,
    dest_type: &DataType,
    fn_registry: &FunctionRegistry,
) -> Option<CastPlan> {
    let plan = |step| Some(CastPlan::new(src_type, dest_type, step));
    let nested = |src_type: &DataType, dest_type: &DataType| {
        resolve_cast_plan(src_type, dest_type, fn_registry).map(Box::new)
    };

    if src_type == dest_type || src_type.matches_physical_type(dest_type) {
        return plan(CastStep::Identity);
    }
    if src_type == &DataType::Null {
        return plan(CastStep::Null);
    }
    if let Some(via_type) = resolve_cast_chain(src_type, dest_type, fn_registry) {
        let first = resolve_cast_plan(src_type, &via_type, fn_registry)?;
        let second = resolve_cast_plan(&via_type, dest_type, fn_registry)?;
        return plan(CastStep::Chain(vec![first, second]));
    }
    if let Some(plan) = resolve_function_plan(src_type, dest_type, fn_registry) {
        return Some(plan);
    }

    match (src_type, dest_type) {
        (DataType::Nullable(src), DataType::Nullable(dest)) => {
            plan(CastStep::Nullable(nested(src, dest)?))
        }
        (DataType::Nullable(src), _) => plan(CastStep::FromNullable(nested(src, dest_type)?)),
        (_, DataType::Nullable(dest)) => plan(CastStep::ToNullable(nested(src_type, dest)?)),
        (DataType::AggregateState(state), _) => {
            resolve_cast_plan(state.physical_type(), dest_type, fn_registry)
        }
        (DataType::EmptyArray, DataType::Array(_)) => plan(CastStep::EmptyArray),
        (DataType::Array(src), DataType::Array(dest)) => plan(CastStep::Array(nested(src, dest)?)),
        (DataType::Variant, DataType::Array(dest)) => {
            plan(CastStep::VariantToArray(nested(&DataType::Variant, dest)?))
        }
        (DataType::Variant, DataType::Map(deref!(DataType::Tuple(fields))))
            if fields.len() == 2 && fields[0] == DataType::String =>
        {
            plan(CastStep::VariantToMap(nested(
                &DataType::Variant,
                &fields[1],
            )?))
        }
        (DataType::EmptyMap, DataType::Map(_)) => plan(CastStep::EmptyMap),
        (DataType::Map(src), DataType::Map(dest)) => plan(CastStep::Map(nested(src, dest)?)),
        (DataType::Tuple(src_fields), DataType::Tuple(dest_fields))
            if src_fields.len() == dest_fields.len() =>
        {
            let fields = src_fields
                .iter()
                .zip(dest_fields)
                .map(|(src, dest)| resolve_cast_plan(src, dest, fn_registry))
                .collect::<Option<_>>()?;
            plan(CastStep::Tuple(fields))
        }
        (DataType::String | DataType::Variant, DataType::Vector(vector))
            if !matches!(vector, VectorDataType::Int8(_)) =>
        {
            plan(CastStep::ToVector)
        }
        (DataType::Array(inner), DataType::Vector(vector))
            if matches!(
                inner.remove_nullable(),
                DataType::Number(_) | DataType::Decimal(_)
            ) && !matches!(vector, VectorDataType::Int8(_)) =>
        {
            plan(CastStep::ToVector)
        }
        _ => None,
    }
}

/// Whether the cast from `src_type` to `dest_type` converts through `via_type`, so that
/// `CAST(CAST(x AS via_type) AS dest_type)` of an `x` of `src_type` is `CAST(x AS dest_type)`.
pub fn casts_through(
    src_type: &DataType,
    via_type: &DataType,
    dest_type: &DataType,
    fn_registry: &FunctionRegistry,
) -> bool {
    resolve_cast_plan(src_type, dest_type, fn_registry).is_some_and(|plan| {
        matches!(&plan.step, CastStep::Chain(steps)
            if steps.first().is_some_and(|step| step.dest_type == *via_type))
    })
}

/// A step that runs the function registered for the cast from `src_type` to `dest_type`.
fn resolve_function_plan(
    src_type: &DataType,
    dest_type: &DataType,
    fn_registry: &FunctionRegistry,
) -> Option<CastPlan> {
    // NULL has no value to convert, and the same type needs no conversion.
    if src_type == &DataType::Null || src_type.remove_nullable() == dest_type.remove_nullable() {
        return None;
    }
    let function = fn_registry.get_cast_function(src_type, dest_type)?;
    let overload = if src_type.is_nullable() {
        function.nullable_overload?
    } else {
        function.overload
    };
    let call = cast_function_call(&function.name, overload, src_type, dest_type, fn_registry)?;
    let reject_null = if call.return_type == *dest_type {
        false
    } else if function.returns_null && call.return_type == dest_type.wrap_nullable() {
        true
    } else {
        return None;
    };
    Some(CastPlan::new(src_type, dest_type, CastStep::Function {
        call,
        reject_null,
        returns_null: function.returns_null,
    }))
}

/// The call of `overload` of the function `name` on a value of `arg_type`, when the overload
/// takes the value as it is. The parameters of a factory come from `dest_type`.
fn cast_function_call(
    name: &str,
    overload: CastOverload,
    arg_type: &DataType,
    dest_type: &DataType,
    fn_registry: &FunctionRegistry,
) -> Option<CastCall> {
    let id = match overload {
        CastOverload::Builtin(id) => FunctionID::Builtin {
            name: name.to_string(),
            id,
        },
        CastOverload::Factory(id) => FunctionID::Factory {
            name: name.to_string(),
            id,
            params: cast_function_params(dest_type),
            args_type: vec![arg_type.clone()],
        },
    };
    let function = fn_registry.get(&id)?;
    // The overload takes the value as its own type, or as its only generic type, e.g.
    // `to_variant(T0)`.
    let (generics, return_type) = match function.signature.args_type.as_slice() {
        [sig_type] if sig_type == arg_type => (vec![], function.signature.return_type.clone()),
        [DataType::Generic(0)] => {
            let return_type = Substitution::equation(0, arg_type.clone())
                .apply(&function.signature.return_type)
                .ok()?;
            (vec![arg_type.clone()], return_type)
        }
        _ => return None,
    };
    Some(CastCall {
        id,
        function,
        generics,
        return_type,
    })
}

/// The intermediate type of a cast registered as a chain, with the nullability of the source.
fn resolve_cast_chain(
    src_type: &DataType,
    dest_type: &DataType,
    fn_registry: &FunctionRegistry,
) -> Option<DataType> {
    let key = (
        cast_key(&src_type.remove_nullable()),
        cast_key(&dest_type.remove_nullable()),
    );
    let via_type = fn_registry.cast_chains.get(&key)?;
    Some(if src_type.is_nullable() {
        via_type.wrap_nullable()
    } else {
        via_type.clone()
    })
}

/// Params are typed as literals are, so that the call is the one written
/// in SQL, e.g. `to_decimal(10, 2)(x)`.
fn cast_function_params(dest_type: &DataType) -> Vec<Scalar> {
    if let DataType::Decimal(ty) = dest_type.remove_nullable_ref() {
        vec![
            Scalar::Number(NumberScalar::UInt8(ty.precision())),
            Scalar::Number(NumberScalar::UInt8(ty.scale())),
        ]
    } else {
        vec![]
    }
}

/// Finds the overloads of `function_name` that a cast from `src` to `dest` runs. A decimal type
/// stands for every size, so the overloads are found with the default size.
pub(crate) fn resolve_registered_cast(
    fn_registry: &FunctionRegistry,
    src: Option<&DataType>,
    dest: &DataType,
    function_name: &str,
) -> CastFunction {
    let sample = |data_type: &DataType| match data_type {
        DataType::Decimal(_) => DataType::Decimal(DecimalSize::default()),
        data_type => data_type.clone(),
    };
    let overloads = fn_registry
        .funcs
        .get(function_name)
        .into_iter()
        .flatten()
        .map(|(_, id)| CastOverload::Builtin(*id))
        .chain(
            fn_registry
                .factories
                .get(function_name)
                .into_iter()
                .flatten()
                .map(|(_, id)| CastOverload::Factory(*id)),
        )
        .collect::<Vec<_>>();
    let dest = sample(dest);

    let Some(src) = src.map(sample) else {
        // A cast from any type runs a single generic function, for nullable values too.
        let [overload] = overloads.as_slice() else {
            panic!(
                "a cast from any type to {dest} needs one `{function_name}`, found {overloads:?}"
            );
        };
        return CastFunction {
            name: function_name.to_string(),
            overload: *overload,
            nullable_overload: Some(*overload),
            returns_null: false,
        };
    };

    let calls_of_value = overloads
        .iter()
        .filter_map(|overload| {
            let call = cast_function_call(function_name, *overload, &src, &dest, fn_registry)?;
            (call.return_type.remove_nullable() == dest).then_some((*overload, call.return_type))
        })
        .collect::<Vec<_>>();
    let [(overload, return_type)] = calls_of_value.as_slice() else {
        panic!(
            "a cast from {src} to {dest} needs one `{function_name}` overload, found {calls_of_value:?}"
        );
    };
    let nullable_overload = nullable_cast_overload(
        fn_registry,
        function_name,
        *overload,
        &src,
        &dest,
        return_type,
    );
    CastFunction {
        name: function_name.to_string(),
        overload: *overload,
        nullable_overload,
        returns_null: return_type.is_nullable(),
    }
}

/// The overload of `function_name` that runs `overload`, the overload of a value of `src`, on a
/// nullable value and passes the NULLs through, as registration records it: the passthrough or
/// combine variant, the passthrough factory, or `overload` itself when its signature takes them.
/// A variant or factory that handles NULL itself is not one.
fn nullable_cast_overload(
    fn_registry: &FunctionRegistry,
    function_name: &str,
    overload: CastOverload,
    src: &DataType,
    dest: &DataType,
    return_type: &DataType,
) -> Option<CastOverload> {
    let id = cast_function_call(function_name, overload, src, dest, fn_registry)?.id;
    let candidates = match fn_registry.nullable_input(&id) {
        Some(
            NullableInput::PassthroughVariant { variant }
            | NullableInput::CombineVariant { variant },
        ) => vec![CastOverload::Builtin(*variant)],
        Some(NullableInput::PassthroughFactory { factory }) => {
            vec![CastOverload::Factory(*factory)]
        }
        Some(NullableInput::NullAwareVariant { .. } | NullableInput::NullAwareFactories { .. }) => {
            vec![]
        }
        None => vec![overload],
    };
    let nullable_src = src.wrap_nullable();
    let nullable_return_type = return_type.wrap_nullable();
    candidates.into_iter().find(|candidate| {
        cast_function_call(function_name, *candidate, &nullable_src, dest, fn_registry)
            .is_some_and(|call| call.return_type == nullable_return_type)
    })
}

impl fmt::Display for CastPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.step {
            CastStep::Identity => write!(f, "identity"),
            CastStep::Null => write!(f, "null"),
            CastStep::Function {
                call, reject_null, ..
            } => {
                write!(f, "{}", call.function.signature)?;
                if *reject_null {
                    write!(f, " rejecting NULL")?;
                }
                Ok(())
            }
            CastStep::Chain(steps) => {
                for (i, step) in steps.iter().enumerate() {
                    if i > 0 {
                        write!(f, " then ")?;
                    }
                    write!(f, "[{} -> {}: {step}]", step.src_type, step.dest_type)?;
                }
                Ok(())
            }
            CastStep::Nullable(inner) => write!(f, "nullable({inner})"),
            CastStep::FromNullable(inner) => write!(f, "from_nullable({inner})"),
            CastStep::ToNullable(inner) => write!(f, "to_nullable({inner})"),
            CastStep::EmptyArray => write!(f, "empty_array"),
            CastStep::EmptyMap => write!(f, "empty_map"),
            CastStep::Array(inner) => write!(f, "array({inner})"),
            CastStep::Map(inner) => write!(f, "map({inner})"),
            CastStep::Tuple(fields) => {
                write!(f, "tuple(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{field}")?;
                }
                write!(f, ")")
            }
            CastStep::VariantToArray(inner) => write!(f, "variant_array({inner})"),
            CastStep::VariantToMap(inner) => write!(f, "variant_map({inner})"),
            CastStep::ToVector => write!(f, "to_vector"),
        }
    }
}
