// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_datavalues::with_match_integer_type_id;
use common_datavalues::with_match_float_type_id;
use common_exception::Result;
use common_io::prelude::*;

use super::aggregate_distinct_state::AggregateDistinctIntegerState;
use super::aggregate_distinct_state::AggregateDistinctState;
use super::aggregate_distinct_state::AggregateDistinctStringState;
use super::aggregate_distinct_state::AggregateDistinctFloatState;
use super::aggregate_distinct_state::DataGroupValues;
use super::aggregate_distinct_state::DistinctStateFunc;
use super::aggregate_function::AggregateFunction;
use super::aggregate_function_factory::AggregateFunctionCreator;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_function_factory::CombinatorDescription;
use super::aggregator_common::assert_variadic_arguments;
use super::AggregateCountFunction;
use super::StateAddr;

#[derive(Clone)]
pub struct AggregateDistinctCombinator<S, State> {
    name: String,

    nested_name: String,
    arguments: Vec<DataField>,
    nested: Arc<dyn AggregateFunction>,
    _s: PhantomData<S>,
    _state: PhantomData<State>,
}

impl<S, State> AggregateFunction for AggregateDistinctCombinator<S, State>
where
    S: Send + Sync,
    State: DistinctStateFunc<S>,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataTypeImpl> {
        self.nested.return_type()
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| State::new());
        let layout = Layout::new::<State>();
        let netest_place = place.next(layout.size());
        self.nested.init_state(netest_place);
    }

    fn state_layout(&self) -> Layout {
        let layout = Layout::new::<State>();
        let netesed = self.nested.state_layout();
        Layout::from_size_align(layout.size() + netesed.size(), layout.align()).unwrap()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<State>();
        state.batch_add(columns, validity, input_rows)
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], row: usize) -> Result<()> {
        let state = place.get::<State>();
        state.add(columns, row)
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<State>();
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<State>();
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<State>();
        let rhs = rhs.get::<State>();
        state.merge(rhs)
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        let state = place.get::<State>();

        let layout = Layout::new::<State>();
        let netest_place = place.next(layout.size());

        // faster path for count
        if self.nested.name() == "AggregateFunctionCount" {
            let mut builder: &mut MutablePrimitiveColumn<u64> =
                Series::check_get_mutable_column(array)?;
            builder.append_value(state.len() as u64);
            Ok(())
        } else {
            if state.is_empty() {
                return self.nested.merge_result(netest_place, array);
            }
            let columns = state.build_columns(&self.arguments).unwrap();

            self.nested
                .accumulate(netest_place, &columns, None, state.len())?;
            // merge_result
            self.nested.merge_result(netest_place, array)
        }
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<State>();
        std::ptr::drop_in_place(state);

        if self.nested.need_manual_drop_state() {
            let layout = Layout::new::<State>();
            let netest_place = place.next(layout.size());
            self.nested.drop_state(netest_place);
        }
    }
}

impl<S, State> fmt::Display for AggregateDistinctCombinator<S, State> {
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

pub fn aggregate_combinator_uniq_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        returns_default_when_only_null: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(Box::new(try_create_uniq), features)
}

pub fn try_create_uniq(
    nested_name: &str,
    params: Vec<DataValue>,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    let creator: AggregateFunctionCreator = Box::new(AggregateCountFunction::try_create);
    try_create(nested_name, params, arguments, &creator)
}

pub fn try_create(
    nested_name: &str,
    params: Vec<DataValue>,
    arguments: Vec<DataField>,
    nested_creator: &AggregateFunctionCreator,
) -> Result<Arc<dyn AggregateFunction>> {
    let name = format!("DistinctCombinator({})", nested_name);
    assert_variadic_arguments(&name, arguments.len(), (1, 32))?;

    let nested_arguments = match nested_name {
        "count" | "uniq" => vec![],
        _ => arguments.clone(),
    };
    let nested = nested_creator(nested_name, params, nested_arguments)?;
    if arguments.len() == 1 {
        let data_type = arguments[0].data_type().clone();
        let phid = data_type.data_type_id();
        if phid.is_integer() {
            with_match_integer_type_id!(phid, |$T| {
                return Ok(Arc::new(AggregateDistinctCombinator::<
                    $T,
                    AggregateDistinctIntegerState<$T>,
                > {
                    nested_name: nested_name.to_owned(),
                    arguments,
                    nested,
                    name,
                    _s: PhantomData,
                    _state: PhantomData,
                }));
            }, {
                panic!("Unsupported type for AggregateDistinctIntegerState");
            })
        }
        if phid.is_string() {
            return Ok(Arc::new(AggregateDistinctCombinator::<
                Vec<u8>,
                AggregateDistinctStringState,
            > {
                nested_name: nested_name.to_owned(),
                arguments,
                nested,
                name,
                _s: PhantomData,
                _state: PhantomData,
            }));
        }
        if phid.is_floating() {
            with_match_float_type_id!(phid, |$T| {
                return Ok(Arc::new(AggregateDistinctCombinator::<
                    $T,
                    AggregateDistinctFloatState<$T>,
                > {
                    nested_name: nested_name.to_owned(),
                    arguments,
                    nested,
                    name,
                    _s: PhantomData,
                    _state: PhantomData,
                }));
            }, {
                panic!("Unsupported type for AggregateDistinctFloatState");
            })
        }
    }
    Ok(Arc::new(AggregateDistinctCombinator::<
        DataGroupValues,
        AggregateDistinctState,
    > {
        nested_name: nested_name.to_owned(),
        arguments,
        nested,
        name,
        _s: PhantomData,
        _state: PhantomData,
    }))
}
