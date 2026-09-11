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
use std::collections::HashSet;

use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;

use super::adaptors::*;

pub(super) fn create(build: DirectBuildContext<'_, impl Combinator>) -> Result<AggregateCallRef> {
    debug_assert!(build.args_type().len() > 1);
    build.create(
        UInt64Type::data_type(),
        AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<RowUniqSet>())],
            vec![RowUniqSet::serde_item()],
        )
        .with_manual_drop(true),
        MultiArgSkipNullEval::new(RowUniqEval),
    )
}

struct RowUniqSet {
    keys: HashSet<Vec<u8>>,
}

impl RowUniqSet {
    fn new() -> Self {
        Self {
            keys: HashSet::new(),
        }
    }
    fn len(&self) -> usize {
        self.keys.len()
    }
    fn serde_item() -> StateSerdeItem {
        StateSerdeItem::DataType(ArrayType::<BinaryType>::data_type())
    }

    fn merge(&mut self, rhs: &Self) {
        self.keys.extend(rhs.keys.iter().cloned());
    }
    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = ArrayType::<BinaryType>::downcast_builder(builder);
        for key in &self.keys {
            builder.put_item(key);
        }
        builder.commit_row();
        Ok(())
    }
    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let ScalarRef::Array(values) = value else {
            unreachable!()
        };
        let values = BinaryType::try_downcast_column(&values).unwrap();
        self.keys
            .extend(BinaryType::iter_column(&values).map(<[u8]>::to_vec));
        Ok(())
    }

    fn add_row(&mut self, columns: ProjectedBlock<'_>, row: usize) -> Result<()> {
        let values = columns
            .iter()
            .map(|column| column.index(row).unwrap().to_owned())
            .collect::<Vec<Scalar>>();
        self.keys.insert(borsh::to_vec(&values)?);
        Ok(())
    }
}

struct RowUniqEval;

impl AggregateEval for RowUniqEval {
    fn init_state(&self, state: AggrState<'_>) {
        state.write(RowUniqSet::new);
    }
    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let set = input.state.get::<RowUniqSet>();
        for row in 0..input.columns.num_rows() {
            if input.validity.is_none_or(|v| v.get(row).unwrap()) {
                set.add_row(input.columns, row)?;
            }
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            state.get::<RowUniqSet>().add_row(input.columns, row)?;
        }
        Ok(())
    }
    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        input
            .state
            .get::<RowUniqSet>()
            .add_row(input.columns, input.row)
    }
    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            state
                .get::<RowUniqSet>()
                .serialize(&mut input.builders[0])?;
        }
        Ok(())
    }
    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_none_or(|v| v.get(row).unwrap()) {
                state
                    .get::<RowUniqSet>()
                    .merge_serialized(super::serialized_scalar_at(input.state, row, 0))?;
            }
        }
        Ok(())
    }
    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        input
            .state
            .get::<RowUniqSet>()
            .merge(input.rhs.get::<RowUniqSet>());
        Ok(())
    }
    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.merge_result_read_only(input)
    }
    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        input.builder.push(ScalarRef::Number(NumberScalar::UInt64(
            input.state.get::<RowUniqSet>().len() as u64,
        )));
        Ok(())
    }
    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(state.get::<RowUniqSet>()) };
    }
}
