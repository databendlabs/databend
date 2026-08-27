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

use std::sync::Arc;

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::BlockEntry;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeType;
use databend_common_expression::types::AggregateFunctionParam;
use databend_common_expression::types::AggregateStateDataType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::utils::column_merge_validity;

use super::AccumulateInput;
use super::AccumulateKeysInput;
use super::AccumulateRowInput;
use super::AggregateCallInstance;
use super::AggregateCallRef;
use super::AggregateEval;
use super::AggregateSignature;
use super::AggregateStateSet;
use super::ArgumentsPattern;
use super::FunctionInputLayout;
use super::MergeResultInput;
use super::MergeSerializedInput;
use super::MergeStatesInput;
use super::RawAggregateCall;
use super::SerializeInput;
use super::state_combinator::aggregate_state_data_type;

type NestedBuild<'a> = dyn Fn(&[Scalar], &[DataType]) -> Result<AggregateCallRef> + 'a;

pub(crate) type LegacySignatureResolver = fn(&[Scalar], &DataType) -> Vec<Vec<DataType>>;

pub(super) fn create(
    request: RawAggregateCall<'_>,
    nested_name: &str,
    nested_aliases: &[&str],
    nested_arguments: &ArgumentsPattern,
    legacy_signature_resolver: Option<LegacySignatureResolver>,
    nested_build: &NestedBuild<'_>,
    returns_state: bool,
) -> Result<AggregateCallRef> {
    let combinator_name = if returns_state {
        "merge_state"
    } else {
        "merge"
    };
    let [argument_type] = request.args_type else {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "Aggregate function {nested_name}_{combinator_name} expects exactly one state argument"
        )));
    };
    let nested_request = NestedRequest {
        name: nested_name,
        aliases: nested_aliases,
        arguments: nested_arguments,
        legacy_signature_resolver,
        build: nested_build,
        params: request.params,
    };

    let state_type = argument_type.remove_nullable();
    let (nested, result_state_type) = if let DataType::AggregateState(state) = &state_type {
        create_from_metadata(nested_request, state, combinator_name)?
    } else {
        create_from_legacy_state(nested_request, &state_type, combinator_name)?
    };

    let return_type = if returns_state {
        result_state_type
    } else {
        nested.signature().return_type.clone()
    };
    let signature = AggregateSignature {
        name: request.name.to_string(),
        params: request.params.to_vec(),
        args_type: request.args_type.to_vec(),
        distinct: request.distinct,
        order_by: request.order_by.to_vec(),
        return_type,
    };
    let features = nested.features().clone();
    let state = nested.state().clone();
    Ok(Arc::new(AggregateCallInstance::new(
        signature,
        FunctionInputLayout::Identity,
        features,
        state,
        MergeEval {
            nested,
            returns_state,
        },
    )))
}

/// The params, metadata, and static builder needed to create a nested
/// aggregate function for the current merge request.
#[derive(Clone, Copy)]
struct NestedRequest<'a> {
    name: &'a str,
    aliases: &'a [&'a str],
    arguments: &'a ArgumentsPattern,
    legacy_signature_resolver: Option<LegacySignatureResolver>,
    build: &'a NestedBuild<'a>,
    params: &'a [Scalar],
}

fn create_from_metadata(
    request: NestedRequest<'_>,
    state: &AggregateStateDataType,
    combinator_name: &str,
) -> Result<(AggregateCallRef, DataType)> {
    let nested_name = request.name;
    if !std::iter::once(request.name)
        .chain(request.aliases.iter().copied())
        .any(|name| state.function_name.eq_ignore_ascii_case(name))
    {
        return Err(ErrorCode::BadDataValueType(format!(
            "Aggregate state for '{}' cannot be merged by {nested_name}_{combinator_name}",
            state.function_name
        )));
    }

    if !request.params.is_empty() && persist_params(request.params)? != state.params {
        return Err(ErrorCode::BadArguments(format!(
            "Aggregate function parameters for {nested_name}_{combinator_name} do not match the persisted state parameters"
        )));
    }

    let nested_params = state
        .params
        .iter()
        .cloned()
        .map(Scalar::from)
        .collect::<Vec<_>>();
    let nested = (request.build)(&nested_params, &state.argument_types)?;
    if serialized_state_type(&nested) != *state.state_type {
        return Err(ErrorCode::BadDataValueType(format!(
            "Aggregate state layout does not match the signature of {nested_name}_{combinator_name}"
        )));
    }

    Ok((nested, DataType::AggregateState(Box::new(state.clone()))))
}

fn create_from_legacy_state(
    request: NestedRequest<'_>,
    state_type: &DataType,
    combinator_name: &str,
) -> Result<(AggregateCallRef, DataType)> {
    let resolved = match request.legacy_signature_resolver {
        Some(resolve) => resolve(request.params, state_type)
            .into_iter()
            .find_map(|argument_types| try_legacy_signature(request, state_type, argument_types)),
        None => LegacySignatureSearch::new(request, state_type).run(),
    };
    let Some((nested, argument_types)) = resolved else {
        let nested_name = request.name;
        return Err(ErrorCode::BadDataValueType(format!(
            "Cannot infer the original aggregate argument from state type '{state_type}' for {nested_name}_{combinator_name}"
        )));
    };
    let result_state_type = aggregate_state_data_type(
        request.name,
        request.params,
        argument_types,
        state_type.clone(),
    )?;
    Ok((nested, result_state_type))
}

fn try_legacy_signature(
    request: NestedRequest<'_>,
    state_type: &DataType,
    argument_types: Vec<DataType>,
) -> Option<(AggregateCallRef, Vec<DataType>)> {
    if !request.arguments.matches_types(&argument_types) {
        return None;
    }
    let nested = (request.build)(request.params, &argument_types).ok()?;
    if serialized_state_type(&nested) != *state_type
        || legacy_signature_has_ambiguous_decimal_scale(request, &argument_types, state_type)
    {
        return None;
    }
    Some((nested, argument_types))
}

fn serialized_state_type(function: &AggregateCallRef) -> DataType {
    StateSerdeType::new(function.state().serde_items().to_vec()).data_type()
}

fn collect_candidates(data_type: &DataType, candidates: &mut Vec<DataType>) {
    if let DataType::Tuple(fields) = data_type {
        for field in fields {
            collect_candidates(field, candidates);
        }
        return;
    }

    if !candidates.contains(data_type) {
        candidates.push(data_type.clone());
    }
    let non_null_type = data_type.remove_nullable();
    if non_null_type != *data_type && !candidates.contains(&non_null_type) {
        candidates.push(non_null_type);
    }
}

/// Recovers the nested signature of a legacy state column, which carries no
/// metadata, by searching candidate argument lists for one whose serialized
/// layout equals the state type.
///
/// The search state (candidates, attempt budget, the list being built) lives
/// here instead of being threaded through recursive calls as positional
/// arguments.
struct LegacySignatureSearch<'a> {
    request: NestedRequest<'a>,
    state_type: &'a DataType,
    candidates: Vec<DataType>,
    argument_types: Vec<DataType>,
    attempts: usize,
}

impl<'a> LegacySignatureSearch<'a> {
    /// Legacy states were only ever produced by aggregates of this arity or
    /// less, so the search never considers wider argument lists.
    const MAX_ARGUMENTS: usize = 3;
    /// Bounds the work spent on a single resolution; the candidate set grows
    /// with the state's tuple width, so the product is capped rather than the
    /// per-arity count.
    const MAX_ATTEMPTS: usize = 4096;

    fn new(request: NestedRequest<'a>, state_type: &'a DataType) -> Self {
        let mut candidates = Vec::new();
        collect_candidates(state_type, &mut candidates);
        Self {
            request,
            state_type,
            candidates,
            argument_types: Vec::new(),
            attempts: 0,
        }
    }

    fn run(mut self) -> Option<(AggregateCallRef, Vec<DataType>)> {
        // Arity 0 is tried last: it only applies to `count`, and trying it first
        // would let a zero-argument state shadow a genuine single-argument one.
        for arity in (1..=Self::MAX_ARGUMENTS).chain(std::iter::once(0)) {
            // The nested function's own argument pattern rules out most arities
            // up front, so those branches are never expanded.
            if !self.request.arguments.accepts_arity(arity) {
                continue;
            }
            self.argument_types.clear();
            self.argument_types.reserve(arity);
            if let Some(signature) = self.search(arity) {
                return Some(signature);
            }
            if self.exhausted() {
                break;
            }
        }
        None
    }

    fn exhausted(&self) -> bool {
        self.attempts >= Self::MAX_ATTEMPTS
    }

    fn search(&mut self, arity: usize) -> Option<(AggregateCallRef, Vec<DataType>)> {
        if self.argument_types.len() == arity {
            return self.try_current();
        }

        for index in 0..self.candidates.len() {
            if self.exhausted() {
                return None;
            }
            self.argument_types.push(self.candidates[index].clone());
            let found = self.search(arity);
            if found.is_some() {
                return found;
            }
            self.argument_types.pop();
        }
        None
    }

    /// Checks the fully built argument list, returning the nested function when
    /// it reproduces the state layout unambiguously.
    fn try_current(&mut self) -> Option<(AggregateCallRef, Vec<DataType>)> {
        self.attempts += 1;
        try_legacy_signature(self.request, self.state_type, self.argument_types.clone())
    }
}

/// A state layout keeps a decimal's scale but not its precision, so when a
/// different scale would produce the same layout the original arguments cannot
/// be recovered and the state is refused rather than guessed.
fn legacy_signature_has_ambiguous_decimal_scale(
    request: NestedRequest<'_>,
    argument_types: &[DataType],
    state_type: &DataType,
) -> bool {
    if request.name.eq_ignore_ascii_case("sum")
        && argument_types.iter().any(is_legacy_decimal_sum_argument)
    {
        return true;
    }

    argument_types.iter().enumerate().any(|(index, data_type)| {
        alternate_decimal_scales(data_type)
            .into_iter()
            .any(|alternate_type| {
                let mut alternate = argument_types.to_vec();
                alternate[index] = alternate_type;
                (request.build)(request.params, &alternate)
                    .is_ok_and(|nested| serialized_state_type(&nested) == *state_type)
            })
    })
}

fn is_legacy_decimal_sum_argument(data_type: &DataType) -> bool {
    match data_type {
        DataType::Decimal(size) => size.scale() == 0 && matches!(size.precision(), 18 | 38 | 76),
        DataType::Nullable(inner) => is_legacy_decimal_sum_argument(inner),
        _ => false,
    }
}

fn alternate_decimal_scales(data_type: &DataType) -> Vec<DataType> {
    match data_type {
        DataType::Decimal(size) => {
            let alternate_scale = if size.scale() == 0 { 1 } else { 0 };
            DecimalSize::new(size.precision(), alternate_scale)
                .map(|size| vec![DataType::Decimal(size)])
                .unwrap_or_default()
        }
        DataType::Nullable(inner) => alternate_decimal_scales(inner)
            .into_iter()
            .map(|alternate| DataType::Nullable(Box::new(alternate)))
            .collect(),
        DataType::Array(inner) => alternate_decimal_scales(inner)
            .into_iter()
            .map(|alternate| DataType::Array(Box::new(alternate)))
            .collect(),
        DataType::Map(inner) => alternate_decimal_scales(inner)
            .into_iter()
            .map(|alternate| DataType::Map(Box::new(alternate)))
            .collect(),
        DataType::Tuple(fields) => fields
            .iter()
            .enumerate()
            .flat_map(|(index, field)| {
                alternate_decimal_scales(field)
                    .into_iter()
                    .map(move |alternate| {
                        let mut alternate_fields = fields.clone();
                        alternate_fields[index] = alternate;
                        DataType::Tuple(alternate_fields)
                    })
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn persist_params(params: &[Scalar]) -> Result<Vec<AggregateFunctionParam>> {
    params
        .iter()
        .cloned()
        .map(AggregateFunctionParam::try_from)
        .collect()
}

struct MergeEval {
    nested: AggregateCallRef,
    returns_state: bool,
}

impl MergeEval {
    fn physical_input(entry: &BlockEntry) -> (BlockEntry, Option<Bitmap>) {
        let validity = column_merge_validity(entry, None);
        let entry = entry.clone().remove_nullable();
        let entry = match entry {
            BlockEntry::Const(value, DataType::AggregateState(state), rows) => {
                BlockEntry::new_const_column(state.physical_type().clone(), value, rows)
            }
            entry => entry,
        };
        (entry, validity)
    }
}

impl AggregateEval for MergeEval {
    fn init_state(&self, state: AggrState<'_>) {
        self.nested.init_state(state)
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let entry = &input.columns[0];
        let (state, validity) = Self::physical_input(entry);
        let validity = match (validity, input.validity) {
            (Some(validity), Some(input_validity)) => Some(&validity & input_validity),
            (Some(validity), None) => Some(validity),
            (None, Some(input_validity)) => Some(input_validity.clone()),
            (None, None) => None,
        };
        let places = vec![input.state.addr; state.len()];
        self.nested.merge_serialized(MergeSerializedInput {
            states: AggregateStateSet::new(&places, input.state.loc),
            state: &state,
            filter: validity.as_ref(),
        })
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let (state, validity) = Self::physical_input(&input.columns[0]);
        self.nested.merge_serialized(MergeSerializedInput {
            states: input.states,
            state: &state,
            filter: validity.as_ref(),
        })
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let (state, validity) = Self::physical_input(&input.columns[0]);
        if validity
            .as_ref()
            .is_some_and(|validity| !validity.get(input.row).unwrap())
        {
            return Ok(());
        }
        let state = state.slice(input.row..input.row + 1);
        let places = [input.state.addr];
        self.nested.merge_serialized(MergeSerializedInput {
            states: AggregateStateSet::new(&places, input.state.loc),
            state: &state,
            filter: None,
        })
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        self.nested.serialize(input)
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        self.nested.merge_serialized(input)
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        self.nested.merge_states(input)
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        if self.returns_state {
            let places = [input.state.addr];
            let builders = input.builder.as_tuple_mut().unwrap().as_mut_slice();
            self.nested.serialize(SerializeInput {
                states: AggregateStateSet::new(&places, input.state.loc),
                builders,
            })
        } else {
            self.nested.merge_result(input)
        }
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        if self.returns_state {
            self.merge_result(input)
        } else {
            self.nested.merge_result_read_only(input)
        }
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { self.nested.drop_state(state) }
    }
}
