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

use std::fmt;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AggregateFunctionParam;
use databend_common_expression::types::AggregateStateDataType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalSize;

use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionCreator;
use super::AggregateFunctionFactory;
use super::AggregateFunctionRef;
use super::AggregateFunctionSortDesc;
use super::CombinatorDescription;
use super::StateAddr;

#[derive(Clone)]
pub struct AggregateMergeCombinator {
    name: String,
    nested: AggregateFunctionRef,
    returns_state: bool,
    state_type: DataType,
}

impl AggregateMergeCombinator {
    pub fn try_create(
        nested_name: &str,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        sort_descs: Vec<AggregateFunctionSortDesc>,
        nested_creator: &AggregateFunctionCreator,
    ) -> Result<AggregateFunctionRef> {
        Self::try_create_inner(
            nested_name,
            params,
            arguments,
            sort_descs,
            nested_creator,
            false,
        )
    }

    pub fn try_create_state(
        nested_name: &str,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        sort_descs: Vec<AggregateFunctionSortDesc>,
        nested_creator: &AggregateFunctionCreator,
    ) -> Result<AggregateFunctionRef> {
        Self::try_create_inner(
            nested_name,
            params,
            arguments,
            sort_descs,
            nested_creator,
            true,
        )
    }

    fn try_create_inner(
        nested_name: &str,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        sort_descs: Vec<AggregateFunctionSortDesc>,
        _nested_creator: &AggregateFunctionCreator,
        returns_state: bool,
    ) -> Result<AggregateFunctionRef> {
        let combinator_name = if returns_state {
            "merge_state"
        } else {
            "merge"
        };
        if arguments.len() != 1 {
            return Err(ErrorCode::NumberArgumentsNotMatch(format!(
                "Aggregate function {nested_name}_{combinator_name} expects exactly one state argument"
            )));
        }

        let state_type = arguments[0].remove_nullable();
        if let DataType::AggregateState(state) = &state_type {
            return Self::try_create_from_metadata(
                nested_name,
                &params,
                state,
                sort_descs,
                returns_state,
            );
        }

        let mut candidates = Vec::new();
        collect_candidates(&state_type, &mut candidates);

        if let Some((nested, argument_types)) =
            find_legacy_signature(nested_name, &params, &state_type, &sort_descs, &candidates)
        {
            return Ok(Arc::new(Self {
                name: format!(
                    "{}({nested_name})",
                    if returns_state {
                        "MergeStateCombinator"
                    } else {
                        "MergeCombinator"
                    }
                ),
                nested,
                returns_state,
                state_type: DataType::AggregateState(Box::new(AggregateStateDataType {
                    function_name: nested_name.to_string(),
                    params: persist_params(&params)?,
                    argument_types,
                    state_type: Box::new(state_type.clone()),
                })),
            }));
        }

        Err(ErrorCode::BadDataValueType(format!(
            "Cannot infer the original aggregate argument from state type '{state_type}' for {nested_name}_{combinator_name}"
        )))
    }

    fn try_create_from_metadata(
        nested_name: &str,
        params: &[Scalar],
        state: &AggregateStateDataType,
        sort_descs: Vec<AggregateFunctionSortDesc>,
        returns_state: bool,
    ) -> Result<AggregateFunctionRef> {
        let combinator_name = if returns_state {
            "merge_state"
        } else {
            "merge"
        };
        if !state.function_name.eq_ignore_ascii_case(nested_name) {
            return Err(ErrorCode::BadDataValueType(format!(
                "Aggregate state for '{}' cannot be merged by {nested_name}_{combinator_name}",
                state.function_name
            )));
        }

        if !params.is_empty() && persist_params(params)? != state.params {
            return Err(ErrorCode::BadArguments(format!(
                "Aggregate function parameters for {nested_name}_{combinator_name} do not match the persisted state parameters"
            )));
        }

        let nested = AggregateFunctionFactory::instance().get(
            nested_name,
            state.params.iter().cloned().map(Scalar::from).collect(),
            state.argument_types.clone(),
            sort_descs,
        )?;
        if nested.serialize_data_type() != *state.state_type {
            return Err(ErrorCode::BadDataValueType(format!(
                "Aggregate state layout does not match the signature of {nested_name}_{combinator_name}"
            )));
        }

        Ok(Arc::new(Self {
            name: format!(
                "{}({nested_name})",
                if returns_state {
                    "MergeStateCombinator"
                } else {
                    "MergeCombinator"
                }
            ),
            nested,
            returns_state,
            state_type: DataType::AggregateState(Box::new(state.clone())),
        }))
    }

    pub fn combinator_desc() -> CombinatorDescription {
        CombinatorDescription::creator(Box::new(Self::try_create))
    }

    pub fn state_combinator_desc() -> CombinatorDescription {
        CombinatorDescription::creator(Box::new(Self::try_create_state))
    }
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

fn find_legacy_signature(
    nested_name: &str,
    params: &[Scalar],
    state_type: &DataType,
    sort_descs: &[AggregateFunctionSortDesc],
    candidates: &[DataType],
) -> Option<(AggregateFunctionRef, Vec<DataType>)> {
    const MAX_LEGACY_ARGUMENTS: usize = 3;
    const MAX_SIGNATURE_ATTEMPTS: usize = 4096;

    let mut attempts = 0;
    for arity in (1..=MAX_LEGACY_ARGUMENTS).chain(std::iter::once(0)) {
        let mut argument_types = Vec::with_capacity(arity);
        if let Some(signature) = find_legacy_signature_with_arity(
            nested_name,
            params,
            state_type,
            sort_descs,
            candidates,
            arity,
            &mut argument_types,
            &mut attempts,
            MAX_SIGNATURE_ATTEMPTS,
        ) {
            return Some(signature);
        }
        if attempts >= MAX_SIGNATURE_ATTEMPTS {
            break;
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn find_legacy_signature_with_arity(
    nested_name: &str,
    params: &[Scalar],
    state_type: &DataType,
    sort_descs: &[AggregateFunctionSortDesc],
    candidates: &[DataType],
    arity: usize,
    argument_types: &mut Vec<DataType>,
    attempts: &mut usize,
    max_attempts: usize,
) -> Option<(AggregateFunctionRef, Vec<DataType>)> {
    if argument_types.len() == arity {
        *attempts += 1;
        let nested = AggregateFunctionFactory::instance()
            .get(
                nested_name,
                params.to_vec(),
                argument_types.clone(),
                sort_descs.to_vec(),
            )
            .ok()?;
        if nested.serialize_data_type() != *state_type
            || legacy_signature_has_ambiguous_decimal_scale(
                nested_name,
                params,
                state_type,
                sort_descs,
                argument_types,
            )
        {
            return None;
        }
        return Some((nested, argument_types.clone()));
    }

    for candidate in candidates {
        if *attempts >= max_attempts {
            return None;
        }
        argument_types.push(candidate.clone());
        if let Some(signature) = find_legacy_signature_with_arity(
            nested_name,
            params,
            state_type,
            sort_descs,
            candidates,
            arity,
            argument_types,
            attempts,
            max_attempts,
        ) {
            return Some(signature);
        }
        argument_types.pop();
    }
    None
}

fn legacy_signature_has_ambiguous_decimal_scale(
    nested_name: &str,
    params: &[Scalar],
    state_type: &DataType,
    sort_descs: &[AggregateFunctionSortDesc],
    argument_types: &[DataType],
) -> bool {
    if nested_name.eq_ignore_ascii_case("sum")
        && argument_types.iter().any(is_legacy_decimal_sum_argument)
    {
        return true;
    }

    argument_types.iter().enumerate().any(|(index, data_type)| {
        alternate_decimal_scales(data_type)
            .into_iter()
            .any(|alternate_type| {
                let mut alternate_arguments = argument_types.to_vec();
                alternate_arguments[index] = alternate_type;
                AggregateFunctionFactory::instance()
                    .get(
                        nested_name,
                        params.to_vec(),
                        alternate_arguments,
                        sort_descs.to_vec(),
                    )
                    .is_ok_and(|nested| nested.serialize_data_type() == *state_type)
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

impl AggregateFunction for AggregateMergeCombinator {
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataType> {
        if self.returns_state {
            Ok(self.state_type.clone())
        } else {
            self.nested.return_type()
        }
    }

    fn init_state(&self, place: AggrState) {
        self.nested.init_state(place)
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        self.nested.register_state(registry)
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let places = vec![place.addr; input_rows];
        self.nested
            .batch_merge(&places, place.loc, &columns[0], validity)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        self.nested.batch_merge(places, loc, &columns[0], None)
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        self.nested.batch_merge(
            &[place.addr],
            place.loc,
            &columns[0].slice(row..row + 1),
            None,
        )
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        self.nested.serialize_type()
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        self.nested.batch_serialize(places, loc, builders)
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        self.nested.batch_merge(places, loc, state, filter)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        self.nested.merge_states(place, rhs)
    }

    fn merge_result(
        &self,
        place: AggrState,
        read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        if self.returns_state {
            let builders = builder.as_tuple_mut().unwrap().as_mut_slice();
            self.nested
                .batch_serialize(&[place.addr], place.loc, builders)
        } else {
            self.nested.merge_result(place, read_only, builder)
        }
    }

    fn need_manual_drop_state(&self) -> bool {
        self.nested.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: AggrState) {
        unsafe { self.nested.drop_state(place) }
    }
}

impl fmt::Display for AggregateMergeCombinator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}
