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

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::StateAddr;

use super::*;
use crate::aggregates::AggrStateRegistry;
use crate::aggregates::AggregateFunction as LegacyAggregateFunction;
use crate::aggregates::AggregateFunctionRef as LegacyAggregateFunctionRef;

pub struct AggregateFunctionV2LegacyAdapter {
    function: AggregateFunctionRef,
    order_by: Vec<AggregateRuntimeOrderByItem>,
}

impl AggregateFunctionV2LegacyAdapter {
    pub fn create(
        function: AggregateFunctionRef,
        order_by: Vec<AggregateRuntimeOrderByItem>,
    ) -> LegacyAggregateFunctionRef {
        Arc::new(Self { function, order_by })
    }
}

impl LegacyAggregateFunction for AggregateFunctionV2LegacyAdapter {
    fn name(&self) -> &str {
        &self.function.signature().name
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.function.signature().return_type.clone())
    }

    fn init_state(&self, place: AggrState<'_>) {
        self.function.init_state(place);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        for state in self.function.state().fields() {
            registry.register(*state);
        }
    }

    fn accumulate(
        &self,
        place: AggrState<'_>,
        columns: ProjectedBlock<'_>,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        if columns.is_empty() {
            return self.function.accumulate_row_count(AccumulateRowCountInput {
                state: place,
                rows: input_rows,
            });
        }

        self.function.accumulate(AccumulateInput {
            state: place,
            columns,
            order_by: &self.order_by,
            validity,
        })
    }

    fn accumulate_keys(
        &self,
        addrs: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock<'_>,
        input_rows: usize,
    ) -> Result<()> {
        if columns.is_empty() {
            return self
                .function
                .accumulate_row_count_keys(AccumulateRowCountKeysInput {
                    states: AggregateStateSet::new(&addrs[..input_rows], loc),
                });
        }

        self.function.accumulate_keys(AccumulateKeysInput {
            states: AggregateStateSet::new(&addrs[..input_rows], loc),
            columns,
            order_by: &self.order_by,
        })
    }

    fn accumulate_row(
        &self,
        place: AggrState<'_>,
        columns: ProjectedBlock<'_>,
        row: usize,
    ) -> Result<()> {
        self.function.accumulate_row(AccumulateRowInput {
            state: place,
            columns,
            row,
        })
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        self.function.state().serde_items().to_vec()
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        self.function.serialize(SerializeInput {
            states: AggregateStateSet::new(places, loc),
            builders,
        })
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        self.function.merge_serialized(MergeSerializedInput {
            states: AggregateStateSet::new(places, loc),
            state,
            filter,
        })
    }

    fn merge_states(&self, place: AggrState<'_>, rhs: AggrState<'_>) -> Result<()> {
        self.function
            .merge_states(MergeStatesInput { state: place, rhs })
    }

    fn merge_result(
        &self,
        place: AggrState<'_>,
        read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let input = MergeResultInput {
            state: place,
            builder,
        };
        if read_only {
            self.function.merge_result_read_only(input)
        } else {
            self.function.merge_result(input)
        }
    }

    fn need_manual_drop_state(&self) -> bool {
        self.function.state().need_manual_drop()
    }

    unsafe fn drop_state(&self, place: AggrState<'_>) {
        unsafe { self.function.drop_state(place) };
    }
}

impl fmt::Display for AggregateFunctionV2LegacyAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.function.signature().name)
    }
}
