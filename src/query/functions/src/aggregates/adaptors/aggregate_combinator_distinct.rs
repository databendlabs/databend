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

use std::alloc::Layout;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::NumberColumnBuilder;
use databend_common_expression::with_number_mapped_type;

use super::AggrState;
use super::AggrStateLoc;
use super::AggregateCountFunction;
use super::AggregateFunction;
use super::AggregateFunctionCombinatorNull;
use super::AggregateFunctionCreator;
use super::AggregateFunctionDescription;
use super::AggregateFunctionFeatures;
use super::AggregateFunctionSortDesc;
use super::CombinatorDescription;
use super::StateAddr;
use super::aggregate_distinct_state::AggregateDistinctNumberState;
use super::aggregate_distinct_state::AggregateDistinctState;
use super::aggregate_distinct_state::AggregateDistinctStringState;
use super::aggregate_distinct_state::AggregateUniqStringState;
use super::aggregate_distinct_state::DistinctStateFunc;
use super::aggregate_null_result::AggregateNullResultFunction;
use super::assert_variadic_arguments;

pub struct AggregateDistinctCombinator<State> {
    name: String,

    nested_name: String,
    arguments: Vec<DataType>,
    skip_null: bool,
    nested: Arc<dyn AggregateFunction>,
    _s: PhantomData<fn(State)>,
}

impl<State> Clone for AggregateDistinctCombinator<State> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            nested_name: self.nested_name.clone(),
            arguments: self.arguments.clone(),
            skip_null: self.skip_null,
            nested: self.nested.clone(),
            _s: PhantomData,
        }
    }
}

impl<State> AggregateDistinctCombinator<State>
where State: Send + 'static
{
    fn get_state(place: AggrState) -> &mut State {
        place
            .addr
            .next(place.loc[0].into_custom().unwrap().1)
            .get::<State>()
    }

    fn set_state(place: AggrState, state: State) {
        place
            .addr
            .next(place.loc[0].into_custom().unwrap().1)
            .write_state(state);
    }
}

impl<State> AggregateFunction for AggregateDistinctCombinator<State>
where State: DistinctStateFunc
{
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataType> {
        self.nested.return_type()
    }

    fn init_state(&self, place: AggrState) {
        Self::set_state(place, State::new());
        self.nested.init_state(place.remove_first_loc());
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<State>()));
        self.nested.register_state(registry);
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = Self::get_state(place);
        state.batch_add(columns, validity, input_rows, self.skip_null)
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let state = Self::get_state(place);
        state.add(columns, row, self.skip_null)
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        State::serialize_type(None)
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        State::batch_serialize(places, &loc[..1], builders)
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        State::batch_merge(places, &loc[..1], state, filter)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = Self::get_state(place);
        let other = Self::get_state(rhs);
        state.merge(other)
    }

    fn merge_result(
        &self,
        place: AggrState,
        read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let state = Self::get_state(place);
        let nested_place = place.remove_first_loc();

        // faster path for count
        if self.nested.name() == "AggregateCountFunction" {
            match builder {
                ColumnBuilder::Number(NumberColumnBuilder::UInt64(builder)) => {
                    builder.push(state.len() as u64);
                }
                _ => unreachable!(),
            }
            Ok(())
        } else {
            if state.is_empty() {
                return self.nested.merge_result(nested_place, read_only, builder);
            }
            let entries = &state.build_entries(&self.arguments).unwrap();
            self.nested
                .accumulate(nested_place, entries.into(), None, state.len())?;
            // merge_result
            self.nested.merge_result(nested_place, read_only, builder)
        }
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = Self::get_state(place);
        unsafe { std::ptr::drop_in_place(state) };

        if self.nested.need_manual_drop_state() {
            self.nested.drop_state(place.remove_first_loc());
        }
    }

    fn get_if_condition(&self, columns: ProjectedBlock) -> Option<Bitmap> {
        self.nested.get_if_condition(columns)
    }
}

impl<State> fmt::Display for AggregateDistinctCombinator<State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.nested_name.as_str() {
            "uniq" => write!(f, "uniq"),
            _ => write!(f, "{}_distinct", self.nested_name),
        }
    }
}

pub fn aggregate_combinator_distinct_desc() -> CombinatorDescription {
    CombinatorDescription::creator(Box::new(try_create))
}

pub fn aggregate_uniq_desc() -> AggregateFunctionDescription {
    let features = AggregateFunctionFeatures {
        returns_default_when_only_null: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(|nested_name, params, arguments, sort_descs| {
            let creator = Box::new(AggregateCountFunction::try_create) as _;
            try_create(nested_name, params, arguments, sort_descs, &creator)
        }),
        features,
    )
}

pub fn aggregate_count_distinct_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator_with_features(
        Box::new(|_, params, arguments, _| {
            let count_creator = Box::new(AggregateCountFunction::try_create) as _;
            match *arguments {
                [DataType::Nullable(_)] => {
                    let new_arguments =
                        AggregateFunctionCombinatorNull::transform_arguments(&arguments)?;
                    let nested = try_create(
                        "count",
                        params.clone(),
                        new_arguments,
                        vec![],
                        &count_creator,
                    )?;
                    AggregateFunctionCombinatorNull::try_create(params, arguments, nested, true)
                }
                ref arguments
                    if !arguments.is_empty() && arguments.iter().all(DataType::is_null) =>
                {
                    AggregateNullResultFunction::try_create(DataType::Number(
                        NumberDataType::UInt64,
                    ))
                }
                _ => try_create("count", params, arguments, vec![], &count_creator),
            }
        }),
        AggregateFunctionFeatures {
            returns_default_when_only_null: true,
            keep_nullable: true,
            ..Default::default()
        },
    )
}

fn try_create(
    nested_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    sort_descs: Vec<AggregateFunctionSortDesc>,
    nested_creator: &AggregateFunctionCreator,
) -> Result<Arc<dyn AggregateFunction>> {
    let name = format!("DistinctCombinator({nested_name})");
    assert_variadic_arguments(&name, arguments.len(), (1, 32))?;

    let nested_arguments = match nested_name {
        "count" | "uniq" => vec![],
        _ => arguments.clone(),
    };
    let nested = nested_creator(nested_name, params, nested_arguments, sort_descs)?;

    match *arguments {
        [DataType::Number(ty)] => with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                Ok(Arc::new(AggregateDistinctCombinator::<
                    AggregateDistinctNumberState<NUM_TYPE>,
                > {
                    nested_name: nested_name.to_owned(),
                    arguments,
                    skip_null: false,
                    nested,
                    name,
                    _s: PhantomData,
                }))
            }
        }),
        [DataType::String] if matches!(nested_name, "count" | "uniq") => {
            Ok(Arc::new(AggregateDistinctCombinator::<
                AggregateUniqStringState,
            > {
                name,
                arguments,
                skip_null: false,
                nested,
                nested_name: nested_name.to_owned(),
                _s: PhantomData,
            }))
        }
        [DataType::String] => Ok(Arc::new(AggregateDistinctCombinator::<
            AggregateDistinctStringState,
        > {
            nested_name: nested_name.to_owned(),
            arguments,
            skip_null: false,
            nested,
            name,
            _s: PhantomData,
        })),
        _ => Ok(Arc::new(AggregateDistinctCombinator::<
            AggregateDistinctState,
        > {
            nested_name: nested_name.to_owned(),
            skip_null: nested_name == "count"
                && arguments.len() > 1
                && arguments.iter().any(DataType::is_nullable_or_null),
            arguments,
            nested,
            name,
            _s: PhantomData,
        })),
    }
}
