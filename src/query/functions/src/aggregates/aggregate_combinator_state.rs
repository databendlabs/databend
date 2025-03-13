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

use databend_common_exception::Result;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;

use super::AggregateFunctionFactory;
use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionCreator;
use crate::aggregates::aggregate_function_factory::AggregateFunctionSortDesc;
use crate::aggregates::aggregate_function_factory::CombinatorDescription;
use crate::aggregates::AggrState;
use crate::aggregates::AggrStateLoc;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;

#[derive(Clone)]
pub struct AggregateStateCombinator {
    name: String,
    nested: AggregateFunctionRef,
}

impl AggregateStateCombinator {
    pub fn try_create(
        nested_name: &str,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        sort_descs: Vec<AggregateFunctionSortDesc>,
        _nested_creator: &AggregateFunctionCreator,
    ) -> Result<AggregateFunctionRef> {
        let arg_name = arguments
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let name = format!("StateCombinator({nested_name}, {arg_name})");
        let nested =
            AggregateFunctionFactory::instance().get(nested_name, params, arguments, sort_descs)?;
        Ok(Arc::new(AggregateStateCombinator { name, nested }))
    }

    pub fn combinator_desc() -> CombinatorDescription {
        CombinatorDescription::creator(Box::new(Self::try_create))
    }
}

impl AggregateFunction for AggregateStateCombinator {
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn init_state(&self, place: AggrState) {
        self.nested.init_state(place);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        self.nested.register_state(registry);
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        self.nested.accumulate(place, columns, validity, input_rows)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: InputColumns,
        input_rows: usize,
    ) -> Result<()> {
        self.nested
            .accumulate_keys(places, loc, columns, input_rows)
    }

    fn accumulate_row(&self, place: AggrState, columns: InputColumns, row: usize) -> Result<()> {
        self.nested.accumulate_row(place, columns, row)
    }

    fn serialize(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        self.nested.serialize(place, writer)
    }

    #[inline]
    fn merge(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        self.nested.merge(place, reader)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        self.nested.merge_states(place, rhs)
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        let builder = builder.as_binary_mut().unwrap();
        self.nested.serialize(place, &mut builder.data)?;
        builder.commit_row();
        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        self.nested.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: AggrState) {
        self.nested.drop_state(place);
    }

    fn get_own_null_adaptor(
        &self,
        _nested_function: super::AggregateFunctionRef,
        _params: Vec<Scalar>,
        _arguments: Vec<DataType>,
    ) -> Result<Option<super::AggregateFunctionRef>> {
        Ok(Some(Arc::new(self.clone())))
    }
}

impl fmt::Display for AggregateStateCombinator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}
