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

use super::*;

// Keep v1's redundant outer input-presence flag for nullable aggregates.
// Native unary nullable results (stddev) always retain their flag: it distinguishes
// empty input from all-NULL input and is part of result semantics.
// Nullable-input ownership is independent of this layout switch. This must gate
// both the state description and evaluator: MV refresh, reads and compaction
// must agree on the persisted layout, including ordinary aggregation.
// This switch covers only this compatibility stage, not all v1 state formats.
// TODO: define and validate the non-compatible layout before disabling this.
// Correctness with this hard switch off is intentionally not guaranteed yet.
pub(super) const PRESERVE_V1_INPUT_ROWS_FLAG: bool = true;

/// Preserve v1's outer input-presence flag separately from the nested non-null
/// flag. An all-NULL batch can have input rows without contributing a value.
/// The nested evaluator owns non-empty result nullability; this flag is part of the
/// persisted state contract, including ordinary aggregation used by compaction.
/// V1 layered OrNull over its nullable-input adaptor, so the two Boolean fields
/// represent different facts and cannot be collapsed or reconstructed from each
/// other. Keep this compatibility detail out of the nested aggregate evaluator.
/// `enabled` must match the extra flag in the state description; the builder
/// controls both; semantically required flags are independent of the switch.
pub(super) struct InputRowsEval<I> {
    nested: I,
    enabled: bool,
}

// Flag storage and column handling do not depend on the nested evaluator.
fn input_rows_flag(state: AggrState<'_>) -> &mut u8 {
    state_at(state, state.loc.len() - 1)
}

fn mark_input_rows(states: &AggregateStateSet<'_>) {
    for state in states.iter() {
        *input_rows_flag(state) = 1;
    }
}

fn serialize_input_rows(states: &AggregateStateSet<'_>, builder: &mut ColumnBuilder) {
    for state in states.iter() {
        builder.push(ScalarRef::Boolean(*input_rows_flag(state) != 0));
    }
}

impl<I> InputRowsEval<I> {
    pub(super) fn new(nested: I, enabled: bool) -> Self {
        Self { nested, enabled }
    }

    fn inner_state<'a>(&self, state: AggrState<'a>) -> AggrState<'a> {
        if self.enabled {
            state.remove_last_loc()
        } else {
            state
        }
    }

    fn inner_states<'a>(&self, states: AggregateStateSet<'a>) -> AggregateStateSet<'a> {
        if self.enabled {
            states.without_last_loc()
        } else {
            states
        }
    }
}

impl<I: AggregateEval> AggregateEval for InputRowsEval<I> {
    fn init_state(&self, state: AggrState<'_>) {
        if self.enabled {
            *input_rows_flag(state) = 0;
        }
        self.nested.init_state(self.inner_state(state));
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let rows = input.columns.num_rows();
        if self.enabled && rows > 0 && input.validity.is_none_or(|v| v.null_count() != rows) {
            *input_rows_flag(input.state) = 1;
        }
        self.nested.accumulate(AccumulateInput {
            state: self.inner_state(input.state),
            ..input
        })
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        if self.enabled {
            mark_input_rows(&input.states);
        }
        self.nested.accumulate_keys(AccumulateKeysInput {
            states: self.inner_states(input.states),
            ..input
        })
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        if self.enabled {
            *input_rows_flag(input.state) = 1;
        }
        self.nested.accumulate_row(AccumulateRowInput {
            state: self.inner_state(input.state),
            ..input
        })
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        if !self.enabled {
            return self.nested.serialize(input);
        }
        let (builders, flag) = input.builders.split_at_mut(input.builders.len() - 1);
        serialize_input_rows(&input.states, &mut flag[0]);
        self.nested.serialize(SerializeInput {
            states: self.inner_states(input.states),
            builders,
        })
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        if !self.enabled {
            return self.nested.merge_serialized(input);
        }
        let flag_field = serialized_field_count(input.state) - 1;
        let filter = combined_serialized_flag_filter(input.state, input.filter, flag_field);
        for (row, state) in input.states.iter().enumerate() {
            if filter.as_ref().is_none_or(|v| v.get(row).unwrap()) {
                *input_rows_flag(state) = 1;
            }
        }
        let state = project_serialized_fields(input.state, 0, flag_field);
        self.nested.merge_serialized(MergeSerializedInput {
            states: self.inner_states(input.states),
            state: &state,
            filter: filter.as_ref(),
        })
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        if self.enabled {
            if *input_rows_flag(input.rhs) == 0 {
                return Ok(());
            }
            *input_rows_flag(input.state) = 1;
        }
        self.nested.merge_states(MergeStatesInput {
            state: self.inner_state(input.state),
            rhs: self.inner_state(input.rhs),
        })
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        // Preserve the legacy outer OrNull flag: when no input rows arrived,
        // return NULL without finalizing the native nullable kernel.
        if self.enabled && *input_rows_flag(input.state) == 0 {
            input.builder.push(ScalarRef::Null);
            return Ok(());
        }
        self.nested.merge_result(MergeResultInput {
            state: self.inner_state(input.state),
            ..input
        })
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        if self.enabled && *input_rows_flag(input.state) == 0 {
            input.builder.push(ScalarRef::Null);
            return Ok(());
        }
        self.nested.merge_result_read_only(MergeResultInput {
            state: self.inner_state(input.state),
            ..input
        })
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { self.nested.drop_state(self.inner_state(state)) }
    }
}
