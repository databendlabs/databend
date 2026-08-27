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

use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnView;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::types::UInt32Type;

use super::AggregateRegistration;
use super::adaptors::*;

#[derive(Default)]
pub struct AggregateRetentionState {
    events: u32,
}

impl AggregateRetentionState {
    fn add(&mut self, event: usize) {
        self.events |= 1 << event;
    }

    fn merge(&mut self, rhs: &Self) {
        self.events |= rhs.events;
    }
}

pub struct RetentionEval {
    events_size: usize,
}

struct RetentionBuilder;

impl RetentionBuilder {
    fn register(registry: &mut AggregateRegistry) {
        NameRoute::new(
            &["retention"],
            Self::retention_arguments(),
            Self::RETENTION_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::multi_arg(false, Self::create))
        .then(MergeRoute::multi_arg(true, Self::create))
        .then(PlainRoute::multi_arg(Self::create))
        .then(IfRoute::multi_arg(Self::create))
        .then(StateRoute::multi_arg(Self::create).with_features(Self::RETENTION_STATE_FEATURES))
        .then(DistinctRoute::<true>::multi_arg(Self::create))
        .register(registry);
    }
}

inventory::submit! {
    AggregateRegistration {
        register: RetentionBuilder::register,
    }
}

impl RetentionBuilder {
    fn retention_arguments() -> ArgumentsPattern {
        ArgumentsPattern::variadic(
            vec![],
            ArgumentPattern::exact(DataType::Boolean),
            1,
            Some(32),
        )
    }

    const RETENTION_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "calculates event retention flags",
        definition: "retention(cond1, cond2, ...)",
        example: "select retention(event1, event2) from t",
    };

    const RETENTION_STATE_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the serialized aggregate state",
        definition: "aggregate_state(args...)",
        example: "select retention_state(event1, event2) from t",
    };
}

impl RetentionEval {
    pub fn new(events_size: usize) -> Self {
        debug_assert!((1..=32).contains(&events_size));
        Self { events_size }
    }

    fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(
            vec![AggrStateType::Custom(
                Layout::new::<AggregateRetentionState>(),
            )],
            vec![StateSerdeItem::DataType(UInt32Type::data_type())],
        )
    }

    fn boolean_views(&self, columns: ProjectedBlock<'_>) -> Vec<ColumnView<BooleanType>> {
        debug_assert_eq!(columns.len(), self.events_size);
        (0..self.events_size)
            .map(|event| columns[event].downcast::<BooleanType>().unwrap())
            .collect()
    }

    fn accumulate_row_into_state(
        &self,
        state: &mut AggregateRetentionState,
        views: &[ColumnView<BooleanType>],
        row: usize,
    ) {
        for (event, view) in views.iter().enumerate() {
            if unsafe { view.index_unchecked(row) } {
                state.add(event);
            }
        }
    }
}

impl AggregateEval for RetentionEval {
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateRetentionState::default);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateRetentionState>();
        let views = self.boolean_views(input.columns);
        for row in 0..input.columns.num_rows() {
            self.accumulate_row_into_state(state, &views, row);
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let views = self.boolean_views(input.columns);
        for (row, state) in input.states.iter().enumerate() {
            let state = state.get::<AggregateRetentionState>();
            self.accumulate_row_into_state(state, &views, row);
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateRetentionState>();
        let views = self.boolean_views(input.columns);
        self.accumulate_row_into_state(state, &views, input.row);
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            let state = state.get::<AggregateRetentionState>();
            input.builders[0].push(ScalarRef::Number(NumberScalar::UInt32(state.events)));
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            let ScalarRef::Number(NumberScalar::UInt32(events)) =
                super::serialized_scalar_at(input.state, row, 0)
            else {
                unreachable!()
            };
            state
                .get::<AggregateRetentionState>()
                .merge(&AggregateRetentionState { events });
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        let rhs = input.rhs.get::<AggregateRetentionState>();
        input.state.get::<AggregateRetentionState>().merge(rhs);
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateRetentionState>();
        let builder = input.builder.as_array_mut().unwrap();
        let inner = builder
            .builder
            .as_number_mut()
            .unwrap()
            .as_u_int8_mut()
            .unwrap();

        inner.reserve(self.events_size);
        if state.events & 1 == 1 {
            inner.push(1u8);
            for event in 1..self.events_size {
                inner.push(u8::from(state.events & (1 << event) != 0));
            }
        } else {
            for _ in 0..self.events_size {
                inner.push(0u8);
            }
        }
        builder.offsets.push(builder.builder.len() as u64);
        Ok(())
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(state.get::<AggregateRetentionState>()) };
    }
}

impl RetentionBuilder {
    fn create(build: MultiArgBuildContext<'_, impl Combinator>) -> Result<AggregateCallRef> {
        let events_size = build.args_type().len();
        build.create_multi_arg_or_null(
            DataType::Array(Box::new(UInt8Type::data_type())).wrap_nullable(),
            RetentionEval::state_description(),
            RetentionEval::new(events_size),
        )
    }
}
