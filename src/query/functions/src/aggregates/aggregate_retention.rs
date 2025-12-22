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
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt32Type;

use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionRef;
use super::AggregateFunctionSortDesc;
use super::StateAddr;
use super::assert_params;
use super::assert_variadic_arguments;
use super::batch_merge1;
use super::batch_serialize1;

struct AggregateRetentionState {
    events: u32,
}

impl AggregateRetentionState {
    #[inline(always)]
    fn add(&mut self, event: u8) {
        self.events |= 1 << event;
    }

    fn merge(&mut self, other: &Self) {
        self.events |= other.events;
    }
}

#[derive(Clone)]
pub struct AggregateRetentionFunction {
    display_name: String,
    events_size: u8,
}

impl AggregateFunction for AggregateRetentionFunction {
    fn name(&self) -> &str {
        "AggregateRetentionFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Number(
            NumberDataType::UInt8,
        ))))
    }

    fn init_state(&self, place: AggrState) {
        place.write(|| AggregateRetentionState { events: 0 });
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(
            Layout::new::<AggregateRetentionState>(),
        ));
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        _validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        let views = columns
            .iter()
            .map(|entry| entry.downcast::<BooleanType>().unwrap())
            .collect::<Vec<_>>();
        for i in 0..input_rows {
            for j in 0..self.events_size {
                if views[j as usize].index(i).unwrap() {
                    state.add(j);
                }
            }
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        let views = columns
            .iter()
            .map(|entry| entry.downcast::<BooleanType>().unwrap())
            .collect::<Vec<_>>();
        for (row, place) in places.iter().enumerate() {
            let state = AggrState::new(*place, loc).get::<AggregateRetentionState>();
            for j in 0..self.events_size {
                let view = &views[j as usize];
                if unsafe { view.index_unchecked(row) } {
                    state.add(j);
                }
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        let views = columns
            .iter()
            .map(|entry| entry.downcast::<BooleanType>().unwrap())
            .collect::<Vec<_>>();
        for j in 0..self.events_size {
            let view = &views[j as usize];
            if unsafe { view.index_unchecked(row) } {
                state.add(j);
            }
        }
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        vec![UInt32Type::data_type().into()]
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<UInt32Type, AggregateRetentionState, _>(
            places,
            loc,
            builders,
            |state, builder| {
                builder.push_item(state.events);
                Ok(())
            },
        )
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<UInt32Type, AggregateRetentionState, _>(
            places,
            loc,
            state,
            filter,
            |state, events| {
                let rhs = AggregateRetentionState { events };
                state.merge(&rhs);
                Ok(())
            },
        )
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        let other = rhs.get::<AggregateRetentionState>();
        state.merge(other);
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        let builder = builder.as_array_mut().unwrap();
        let inner = builder
            .builder
            .as_number_mut()
            .unwrap()
            .as_u_int8_mut()
            .unwrap();

        inner.reserve(self.events_size as usize);
        if state.events & 1 == 1 {
            inner.push(1u8);
            for i in 1..self.events_size {
                if state.events & (1 << i) != 0 {
                    inner.push(1u8);
                } else {
                    inner.push(0u8);
                }
            }
        } else {
            for _ in 0..self.events_size {
                inner.push(0u8);
            }
        }
        builder.offsets.push(builder.builder.len() as u64);
        Ok(())
    }
}

impl fmt::Display for AggregateRetentionFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl AggregateRetentionFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataType>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_owned(),
            events_size: arguments.len() as u8,
        }))
    }
}

pub fn try_create_aggregate_retention_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    assert_params(display_name, params.len(), 0)?;
    assert_variadic_arguments(display_name, arguments.len(), (1, 32))?;

    for argument in arguments.iter() {
        if !argument.is_boolean() {
            return Err(ErrorCode::BadArguments(
                "The arguments of AggregateRetention should be an expression which returns a Boolean result",
            ));
        }
    }

    AggregateRetentionFunction::try_create(display_name, arguments)
}

pub fn aggregate_retention_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_retention_function))
}
