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

use std::marker::PhantomData;

use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NullableType;

use super::*;

pub(crate) trait ArrayCollectState<T>: Clone + Send + Sync + 'static
where T: AccessType
{
    fn state_description(return_type: DataType) -> AggregateStateDescription;

    fn add(&mut self, value: Option<T::ScalarRef<'_>>);

    fn add_batch(&mut self, column: ColumnView<T>, validity: Option<&Bitmap>) -> Result<()>;

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()>;

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()>;

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()>;

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()>;
}

// DataType::Null is handled by the builder. For other types, this evaluator
// preserves NULL values as None and excludes only rows rejected by selection.
pub(crate) struct ArrayCollectEval<T, State> {
    _p: PhantomData<fn(T, State)>,
}

impl<T, State> Default for ArrayCollectEval<T, State> {
    fn default() -> Self {
        Self { _p: PhantomData }
    }
}

impl<T, State> AggregateEval for ArrayCollectEval<T, State>
where
    T: AccessType,
    State: Default + ArrayCollectState<T>,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(State::default);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let state = input.state.get::<State>();
        let entry = &input.columns[0];
        debug_assert!(
            !entry.data_type().is_null(),
            "Null input must be specialized by the builder"
        );
        // Selection excludes rows. Nullable values are delivered as None, just
        // as they are in accumulate_row and accumulate_keys.
        if entry.data_type().is_nullable() {
            let values = entry.downcast::<NullableType<T>>().unwrap();
            for (row, value) in values.iter().enumerate() {
                if input
                    .validity
                    .is_none_or(|validity| validity.get(row).unwrap())
                {
                    state.add(value);
                }
            }
            return Ok(());
        }
        let column = entry.downcast::<T>().unwrap();
        if let Some(validity) = input.validity.filter(|validity| validity.null_count() != 0) {
            for (value, valid) in column.iter().zip(validity.iter()) {
                if valid {
                    state.add(Some(value));
                }
            }
            Ok(())
        } else {
            state.add_batch(column, None)
        }
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let entry = &input.columns[0];
        debug_assert!(
            !entry.data_type().is_null(),
            "Null input must be specialized by the builder"
        );
        if entry.data_type().is_nullable() {
            let values = entry.downcast::<NullableType<T>>().unwrap();
            for (value, state) in values.iter().zip(input.states.iter()) {
                state.get::<State>().add(value);
            }
            return Ok(());
        }

        let values = entry.downcast::<T>().unwrap();
        for (value, state) in values.iter().zip(input.states.iter()) {
            state.get::<State>().add(Some(value));
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let state = input.state.get::<State>();
        let entry = &input.columns[0];
        debug_assert!(
            !entry.data_type().is_null(),
            "Null input must be specialized by the builder"
        );
        if entry.data_type().is_nullable() {
            let values = entry.downcast::<NullableType<T>>().unwrap();
            state.add(values.index(input.row).unwrap());
            return Ok(());
        }

        let values = entry.downcast::<T>().unwrap();
        state.add(Some(values.index(input.row).unwrap()));
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            state.get::<State>().serialize(&mut input.builders[0])?;
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            state
                .get::<State>()
                .merge_serialized(super::serialized_scalar_at(input.state, row, 0))?;
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        input
            .state
            .get::<State>()
            .merge_owned(input.rhs.get::<State>())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = input.state.get::<State>();
        state.merge_result(input.builder)
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        let mut state = input.state.get::<State>().clone();
        state.merge_result(input.builder)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(state.get::<State>()) };
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::Layout;
    use std::sync::Arc;

    use databend_common_expression::AggrStateType;
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::StateSerdeItem;
    use databend_common_expression::types::UInt64Type;

    use super::*;

    // Observe the evaluator's calls without assigning any aggregate semantics
    // to NULL. A selected NULL must remain distinguishable from an excluded row.
    #[derive(Clone, Default)]
    struct Probe(Vec<Option<u64>>);

    impl ArrayCollectState<UInt64Type> for Probe {
        fn state_description(_: DataType) -> AggregateStateDescription {
            AggregateStateDescription::new(
                vec![AggrStateType::Custom(Layout::new::<Self>())],
                vec![StateSerdeItem::Binary(None)],
            )
            .with_manual_drop(true)
        }
        fn add(&mut self, value: Option<u64>) {
            self.0.push(value);
        }
        fn add_batch(
            &mut self,
            column: ColumnView<UInt64Type>,
            validity: Option<&Bitmap>,
        ) -> Result<()> {
            assert!(validity.is_none());
            self.0.extend(column.iter().map(Some));
            Ok(())
        }
        fn serialize(&self, _: &mut ColumnBuilder) -> Result<()> {
            unreachable!()
        }
        fn merge_serialized(&mut self, _: ScalarRef<'_>) -> Result<()> {
            unreachable!()
        }
        fn merge_owned(&mut self, _: &mut Self) -> Result<()> {
            unreachable!()
        }
        fn merge_result(&mut self, _: &mut ColumnBuilder) -> Result<()> {
            unreachable!()
        }
    }

    fn function(arg_type: DataType) -> AggregateCallRef {
        Arc::new(AggregateCallInstance::new(
            AggregateSignature {
                name: "probe".into(),
                params: vec![],
                args_type: vec![arg_type],
                distinct: false,
                order_by: vec![],
                return_type: DataType::Null,
            },
            FunctionInputLayout::Identity,
            AggregateFeatures::default(),
            Probe::state_description(DataType::Null),
            ArrayCollectEval::<UInt64Type, Probe>::default(),
        ))
    }

    #[test]
    fn nullable_values_are_equivalent_across_accumulation_paths() -> Result<()> {
        let cases = [
            (
                BlockEntry::from(UInt64Type::from_data(vec![1u64, 2, 3])),
                vec![Some(1), Some(2), Some(3)],
            ),
            (
                BlockEntry::from(UInt64Type::from_data_with_validity(
                    vec![1u64, 99, 3],
                    vec![true, false, true],
                )),
                vec![Some(1), None, Some(3)],
            ),
            (
                BlockEntry::new_const_column(
                    DataType::Number(databend_common_expression::types::NumberDataType::UInt64)
                        .wrap_nullable(),
                    Scalar::Null,
                    3,
                ),
                vec![None; 3],
            ),
            (
                BlockEntry::from(UInt64Type::from_data_with_validity(
                    vec![99u64; 3],
                    vec![false; 3],
                )),
                vec![None; 3],
            ),
        ];
        for (entry, expected) in cases {
            let function = function(entry.data_type());
            for mode in 0..3 {
                let owner = AggregateStateOwner::new(vec![function.clone()])?;
                let entries = [entry.clone()];
                match mode {
                    0 => function.accumulate(AccumulateInput {
                        state: owner.state(0),
                        columns: (&entries).into(),
                        validity: None,
                    })?,
                    1 => {
                        for row in 0..3 {
                            function.accumulate_row(AccumulateRowInput {
                                state: owner.state(0),
                                columns: (&entries).into(),
                                row,
                            })?;
                        }
                    }
                    _ => {
                        let state = owner.state(0);
                        let places = vec![state.addr; 3];
                        function.accumulate_keys(AccumulateKeysInput {
                            states: AggregateStateSet::new(&places, state.loc),
                            columns: (&entries).into(),
                        })?;
                    }
                }
                assert_eq!(owner.state(0).get::<Probe>().0, expected);
            }
        }
        Ok(())
    }

    #[test]
    fn selection_excludes_rows_without_turning_them_into_nulls() -> Result<()> {
        for nullable in [false, true] {
            let entry: BlockEntry = if nullable {
                UInt64Type::from_data_with_validity(vec![1u64, 99, 3], vec![true, false, true])
                    .into()
            } else {
                UInt64Type::from_data(vec![1u64, 2, 3]).into()
            };
            let function = function(entry.data_type());
            for selection in [vec![true; 3], vec![false, true, true], vec![false; 3]] {
                let owner = AggregateStateOwner::new(vec![function.clone()])?;
                let expected = [Some(1), if nullable { None } else { Some(2) }, Some(3)]
                    .into_iter()
                    .zip(&selection)
                    .filter_map(|(value, selected)| selected.then_some(value))
                    .collect::<Vec<_>>();
                let validity: Bitmap = selection.into_iter().collect();
                function.accumulate(AccumulateInput {
                    state: owner.state(0),
                    columns: std::slice::from_ref(&entry).into(),
                    validity: Some(&validity),
                })?;
                assert_eq!(owner.state(0).get::<Probe>().0, expected);
            }
        }
        Ok(())
    }
}
