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

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::aggregate::AggrState;
pub use databend_common_expression::aggregate::aggregate_function_v2::*;
use databend_common_expression::types::DataType;

mod build_context;
mod combinator;
mod distinct_combinator;
pub(crate) mod if_combinator;
#[cfg(test)]
pub(crate) mod legacy_adapter;
pub(super) mod merge_combinator;
pub(crate) mod multi_arg_nullable;
mod name_route;
mod null_argument_result;
mod sort_combinator;
pub(crate) mod state_combinator;
mod unary;
mod unary_nullable;
mod unary_state;

pub(super) use combinator::CombinatorImpl;
pub(super) use combinator::DistinctCombinator;
pub(super) use combinator::IfCombinator;
pub(super) use combinator::PlainCombinator;
pub(super) use combinator::StateCombinator;
pub(super) use distinct_combinator::AggregateDistinctImplementation;
pub(super) use distinct_combinator::AggregateDistinctState;
pub(super) use merge_combinator::LegacySignatureResolver;
pub(super) use multi_arg_nullable::AggregateMultiArgOrNullImplementation;
pub(super) use multi_arg_nullable::AggregateMultiArgSkipNullImplementation;
pub(super) use name_route::*;
pub(super) use null_argument_result::try_create_null_argument_result_function;
pub(super) use unary::*;
pub(super) use unary_nullable::UnaryOrNull;
pub(super) use unary_nullable::UnarySkipNull;
pub(super) use unary_state::AggregateUnaryState;
pub(super) use unary_state::AggregateUnaryStateImplementation;

pub(super) struct UnaryBuildContext<'a, C> {
    request: AggregateFunctionRequest<'a>,
    signature_args_type: &'a [DataType],
    combinator_args_type: &'a [DataType],
    features: FunctionFeatures,
    combinator: C,
    arg_type: DataType,
}

pub(super) struct MultiArgBuildContext<'a, C> {
    request: AggregateFunctionRequest<'a>,
    signature_args_type: &'a [DataType],
    combinator_args_type: &'a [DataType],
    features: FunctionFeatures,
    combinator: C,
    args_type: Vec<DataType>,
}

pub(super) struct DirectBuildContext<'a, C> {
    request: AggregateFunctionRequest<'a>,
    signature_args_type: &'a [DataType],
    combinator_args_type: &'a [DataType],
    features: FunctionFeatures,
    combinator: C,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum NullPolicy {
    #[default]
    Skip,
    Keep,
    ReturnsDefaultWhenOnlyNull,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StateCombinatorPlan {
    pub(crate) strip_nullable_input: bool,
    pub(crate) nullable_input_result_flag: bool,
}

pub(super) type UnaryBuildFn<C> =
    for<'a> fn(UnaryBuildContext<'a, C>) -> Result<AggregateFunctionRef>;

pub(super) type MultiArgBuildFn<C> =
    for<'a> fn(MultiArgBuildContext<'a, C>) -> Result<AggregateFunctionRef>;

pub(super) type DirectBuildFn<C> =
    for<'a> fn(DirectBuildContext<'a, C>) -> Result<AggregateFunctionRef>;

pub(crate) fn state_at<T>(state: AggrState<'_>, index: usize) -> &mut T
where T: Send + 'static {
    state.addr.next(state.loc[index].offset()).get::<T>()
}

pub(crate) fn write_state_at<T>(state: AggrState<'_>, index: usize, value: T)
where T: Send + 'static {
    state
        .addr
        .next(state.loc[index].offset())
        .write_state(value)
}

pub(crate) fn serialized_field_count(state: &BlockEntry) -> usize {
    match state.data_type() {
        DataType::Tuple(fields) => fields.len(),
        _ => 1,
    }
}

pub(crate) fn project_serialized_fields(
    state: &BlockEntry,
    start: usize,
    end: usize,
) -> BlockEntry {
    debug_assert!(start < end);
    match state {
        BlockEntry::Column(Column::Tuple(fields)) => {
            if end - start == 1 {
                fields[start].clone().into()
            } else {
                Column::Tuple(fields[start..end].to_vec()).into()
            }
        }
        BlockEntry::Const(Scalar::Tuple(values), DataType::Tuple(data_types), num_rows) => {
            if end - start == 1 {
                BlockEntry::new_const_column(
                    data_types[start].clone(),
                    values[start].clone(),
                    *num_rows,
                )
            } else {
                BlockEntry::new_const_column(
                    DataType::Tuple(data_types[start..end].to_vec()),
                    Scalar::Tuple(values[start..end].to_vec()),
                    *num_rows,
                )
            }
        }
        _ => {
            debug_assert_eq!(start, 0);
            debug_assert_eq!(end, 1);
            state.clone()
        }
    }
}

pub(crate) fn serialized_scalar_at(state: &BlockEntry, row: usize, field: usize) -> ScalarRef<'_> {
    let scalar = state.index(row).unwrap();
    match scalar {
        ScalarRef::Tuple(fields) => fields[field].clone(),
        _ => {
            debug_assert_eq!(field, 0);
            scalar
        }
    }
}

pub(crate) fn serialized_bool_at(state: &BlockEntry, row: usize, field: usize) -> bool {
    match serialized_scalar_at(state, row, field) {
        ScalarRef::Boolean(value) => value,
        other => unreachable!("expected serialized boolean field, got {other:?}"),
    }
}

pub(crate) fn serialized_binary_at(state: &BlockEntry, row: usize, field: usize) -> &[u8] {
    match serialized_scalar_at(state, row, field) {
        ScalarRef::Binary(value) => value,
        other => unreachable!("expected serialized binary field, got {other:?}"),
    }
}

pub(crate) fn combined_serialized_flag_filter(
    state: &BlockEntry,
    filter: Option<&Bitmap>,
    field: usize,
) -> Option<Bitmap> {
    let mut values = Vec::with_capacity(state.len());
    let mut has_false = false;
    for row in 0..state.len() {
        let value = filter.is_none_or(|filter| filter.get(row).unwrap())
            && serialized_bool_at(state, row, field);
        has_false |= !value;
        values.push(value);
    }
    has_false.then(|| Bitmap::from(values))
}

#[cfg(test)]
mod tests {
    use std::alloc::Layout;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use bumpalo::Bump;
    use databend_common_exception::Result;
    use databend_common_expression::FromData;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::aggregate::AggrStateType;
    use databend_common_expression::aggregate::StateAddr;
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::BooleanType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::UInt64Type;

    use super::legacy_adapter::AggregateFunctionV2LegacyAdapter;
    use super::sort_combinator::AggregateSortImplementation;
    use super::sort_combinator::AggregateSortState;
    use super::*;

    struct SumState {
        value: u64,
    }

    #[derive(Clone)]
    enum SumDropCounter {
        Shared(Arc<AtomicUsize>),
    }

    impl SumDropCounter {
        fn increment(&self) {
            match self {
                Self::Shared(drop_count) => {
                    drop_count.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }

    struct SumFunctionInfo {
        drop_count: SumDropCounter,
    }

    struct SumImplementation {
        function_info: Arc<SumFunctionInfo>,
    }

    impl SumImplementation {
        fn new(function_info: Arc<SumFunctionInfo>) -> Self {
            Self { function_info }
        }

        fn add_value(state: &mut SumState, value: ScalarRef<'_>) {
            let ScalarRef::Number(NumberScalar::UInt64(value)) = value else {
                unreachable!()
            };
            state.value += value;
        }
    }

    impl AggrImpl for SumImplementation {
        fn init_state(&self, state: AggrState<'_>) {
            state.write(|| SumState { value: 0 });
        }

        fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
            let state = input.state.get::<SumState>();
            let values = input.columns[0].downcast::<UInt64Type>().unwrap();
            for row in 0..input.columns.num_rows() {
                if input
                    .validity
                    .is_some_and(|validity| !validity.get(row).unwrap())
                {
                    continue;
                }
                state.value += values.index(row).unwrap();
            }
            Ok(())
        }

        fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
            for (row, state) in input.states.iter().enumerate() {
                self.accumulate_row(AccumulateRowInput {
                    state,
                    columns: input.columns,
                    row,
                })?;
            }
            Ok(())
        }

        fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
            let value = input.columns[0].index(input.row).unwrap();
            Self::add_value(input.state.get::<SumState>(), value);
            Ok(())
        }

        fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
            for state in input.states.iter() {
                input.builders[0].push(ScalarRef::Number(NumberScalar::UInt64(
                    state.get::<SumState>().value,
                )));
            }
            Ok(())
        }

        fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
            for (row, state) in input.states.iter().enumerate() {
                if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                    continue;
                }
                Self::add_value(
                    state.get::<SumState>(),
                    serialized_scalar_at(input.state, row, 0),
                );
            }
            Ok(())
        }

        fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
            input.state.get::<SumState>().value += input.rhs.get::<SumState>().value;
            Ok(())
        }

        fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
            input.builder.push(ScalarRef::Number(NumberScalar::UInt64(
                input.state.get::<SumState>().value,
            )));
            Ok(())
        }

        unsafe fn drop_state(&self, state: AggrState<'_>) {
            self.function_info.drop_count.increment();
            unsafe { std::ptr::drop_in_place(state.get::<SumState>()) };
        }
    }

    fn plain_sum(drop_count: Arc<AtomicUsize>) -> impl AggrImpl {
        plain_sum_with_counter(SumDropCounter::Shared(drop_count))
    }

    fn plain_sum_with_counter(drop_count: SumDropCounter) -> impl AggrImpl {
        let function_info = sum_function_info(drop_count);
        SumImplementation::new(function_info)
    }

    fn sum_function_info(drop_count: SumDropCounter) -> Arc<SumFunctionInfo> {
        Arc::new(SumFunctionInfo { drop_count })
    }

    fn distinct_sum_state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(
            vec![
                AggrStateType::Custom(Layout::new::<AggregateDistinctState>()),
                AggrStateType::Custom(Layout::new::<SumState>()),
            ],
            vec![
                StateSerdeItem::DataType(DataType::Array(Box::new(DataType::Binary))),
                StateSerdeItem::DataType(UInt64Type::data_type()),
            ],
        )
        .with_manual_drop(true)
    }

    fn full_modifier_state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(
            vec![
                AggrStateType::Custom(Layout::new::<AggregateSortState>()),
                AggrStateType::Custom(Layout::new::<AggregateDistinctState>()),
                AggrStateType::Custom(Layout::new::<SumState>()),
                AggrStateType::Bool,
            ],
            vec![
                StateSerdeItem::Binary(None),
                StateSerdeItem::DataType(DataType::Array(Box::new(DataType::Binary))),
                StateSerdeItem::DataType(UInt64Type::data_type()),
                StateSerdeItem::DataType(DataType::Boolean),
            ],
        )
        .with_manual_drop(true)
    }

    fn or_null_sum_state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(
            vec![
                AggrStateType::Custom(Layout::new::<SumState>()),
                AggrStateType::Bool,
            ],
            vec![
                StateSerdeItem::DataType(UInt64Type::data_type()),
                StateSerdeItem::DataType(DataType::Boolean),
            ],
        )
        .with_manual_drop(true)
    }

    fn state_serde_data_type(item: &StateSerdeItem) -> DataType {
        match item {
            StateSerdeItem::DataType(data_type) => data_type.clone(),
            StateSerdeItem::Binary(_) => DataType::Binary,
        }
    }

    fn serialize_state(
        function: &AggregateFunctionRef,
        owner: &AggregateStateOwner,
    ) -> Result<BlockEntry> {
        let data_types = function
            .state()
            .serde_items()
            .iter()
            .map(state_serde_data_type)
            .collect::<Vec<_>>();
        let mut builders = data_types
            .iter()
            .map(|data_type| ColumnBuilder::with_capacity(data_type, 1))
            .collect::<Vec<_>>();
        function.serialize(SerializeInput {
            states: owner.state_set(0),
            builders: &mut builders,
        })?;
        let columns = builders
            .into_iter()
            .map(ColumnBuilder::build)
            .collect::<Vec<_>>();
        Ok(if columns.len() == 1 {
            columns.into_iter().next().unwrap().into()
        } else {
            Column::Tuple(columns).into()
        })
    }

    fn full_modifier_order_by() -> Vec<AggregateRuntimeOrderByItem> {
        vec![AggregateRuntimeOrderByItem {
            input: AggregateRuntimeOrderByInput::SortKey { offset: 2 },
            data_type: UInt64Type::data_type(),
            asc: true,
            nulls_first: false,
        }]
    }

    fn full_modifier_function(
        drop_count: Arc<AtomicUsize>,
        order_by: Vec<AggregateRuntimeOrderByItem>,
    ) -> AggregateFunctionRef {
        let implementation =
            AggregateMultiArgOrNullImplementation::new(AggregateSortImplementation::new(
                AggregateDistinctImplementation::<false>::new(plain_sum(drop_count), vec![
                    UInt64Type::data_type(),
                    DataType::Boolean,
                ]),
                vec![
                    UInt64Type::data_type(),
                    DataType::Boolean,
                    UInt64Type::data_type(),
                ],
                order_by,
            ));
        Arc::new(AggregateFunction::new(
            AggregateFunctionSignature {
                name: "sum_probe_full_modifiers".to_string(),
                params: vec![],
                args_type: vec![UInt64Type::data_type(), DataType::Boolean],
                distinct: true,
                order_by: vec![],
                return_type: UInt64Type::data_type().wrap_nullable(),
            },
            FunctionFeatures {
                is_decomposable: false,
                sort_policy: SortPolicy::Required,
                distinct_policy: DistinctPolicy::Required,
                ..Default::default()
            },
            full_modifier_state_description(),
            implementation,
        ))
    }

    fn full_modifier_entries() -> Vec<BlockEntry> {
        vec![
            UInt64Type::from_data(vec![2, 2, 5, 9, 0, 1]).into(),
            BooleanType::from_data(vec![true, true, true, false, true, true]).into(),
            UInt64Type::from_data(vec![3, 1, 2, 0, 4, 5]).into(),
        ]
    }

    fn direct_full_modifier_result(
        function: &AggregateFunctionRef,
        order_by: &[AggregateRuntimeOrderByItem],
        entries: &[BlockEntry],
    ) -> Result<Column> {
        let owner = AggregateStateOwner::new(vec![function.clone()])?;
        function.accumulate(AccumulateInput {
            state: owner.state(0),
            columns: entries.into(),
            order_by,
            validity: None,
        })?;
        let mut builder = ColumnBuilder::with_capacity(&UInt64Type::data_type().wrap_nullable(), 1);
        function.merge_result(MergeResultInput {
            state: owner.state(0),
            builder: &mut builder,
        })?;
        Ok(builder.build())
    }

    #[test]
    fn test_or_null_emits_null_without_input_rows() -> Result<()> {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let function: AggregateFunctionRef = Arc::new(AggregateFunction::new(
            AggregateFunctionSignature {
                name: "sum_probe_or_null".to_string(),
                params: vec![],
                args_type: vec![UInt64Type::data_type()],
                distinct: false,
                order_by: vec![],
                return_type: UInt64Type::data_type().wrap_nullable(),
            },
            FunctionFeatures::default(),
            or_null_sum_state_description(),
            AggregateMultiArgOrNullImplementation::new(plain_sum(drop_count.clone())),
        ));

        {
            let owner = AggregateStateOwner::new(vec![function.clone()])?;
            let mut builder =
                ColumnBuilder::with_capacity(&UInt64Type::data_type().wrap_nullable(), 1);
            function.merge_result(MergeResultInput {
                state: owner.state(0),
                builder: &mut builder,
            })?;
            let column = builder.build();
            assert_eq!(unsafe { column.index_unchecked(0) }, ScalarRef::Null);
        }

        assert_eq!(drop_count.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test]
    fn test_distinct_serialized_merge_restores_key_set() -> Result<()> {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let function: AggregateFunctionRef = Arc::new(AggregateFunction::new(
            AggregateFunctionSignature {
                name: "sum_probe_distinct".to_string(),
                params: vec![],
                args_type: vec![UInt64Type::data_type()],
                distinct: true,
                order_by: vec![],
                return_type: UInt64Type::data_type(),
            },
            FunctionFeatures {
                distinct_policy: DistinctPolicy::Required,
                ..Default::default()
            },
            distinct_sum_state_description(),
            AggregateDistinctImplementation::<false>::new(plain_sum(drop_count.clone()), vec![
                UInt64Type::data_type(),
            ]),
        ));

        {
            let source_owner = AggregateStateOwner::new(vec![function.clone()])?;
            let entries = [UInt64Type::from_data(vec![2, 2, 5]).into()];
            function.accumulate(AccumulateInput {
                state: source_owner.state(0),
                columns: (&entries).into(),
                order_by: &[],
                validity: None,
            })?;
            let serialized_state = serialize_state(&function, &source_owner)?;

            let serialized_owner = AggregateStateOwner::new(vec![function.clone()])?;
            function.merge_serialized(MergeSerializedInput {
                states: serialized_owner.state_set(0),
                state: &serialized_state,
                filter: None,
            })?;

            let mut builder = ColumnBuilder::with_capacity(&UInt64Type::data_type(), 1);
            function.merge_result(MergeResultInput {
                state: serialized_owner.state(0),
                builder: &mut builder,
            })?;
            let column = builder.build();
            assert_eq!(
                unsafe { column.index_unchecked(0) },
                ScalarRef::Number(NumberScalar::UInt64(7))
            );
        }

        assert_eq!(drop_count.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test]
    fn test_legacy_adapter_runs_v2_function_through_old_state_framework() -> Result<()> {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let order_by = full_modifier_order_by();
        let function = full_modifier_function(drop_count.clone(), order_by.clone());
        let entries = full_modifier_entries();
        let direct_result = direct_full_modifier_result(&function, &order_by, &entries)?;

        let legacy = AggregateFunctionV2LegacyAdapter::create(function.clone(), order_by);
        let legacy_functions: Vec<crate::aggregates::AggregateFunctionRef> = vec![legacy.clone()];
        let layout = crate::aggregates::get_states_layout(&legacy_functions)?;
        let loc = &layout.states_loc[0];

        let result_arena = Bump::new();
        let result_addr: StateAddr = result_arena.alloc_layout(layout.layout).into();
        legacy.init_state(AggrState::new(result_addr, loc));
        legacy.accumulate(
            AggrState::new(result_addr, loc),
            (&entries).into(),
            None,
            entries[0].len(),
        )?;
        let mut builder = ColumnBuilder::with_capacity(&UInt64Type::data_type().wrap_nullable(), 1);
        legacy.merge_result(AggrState::new(result_addr, loc), false, &mut builder)?;
        assert_eq!(builder.build(), direct_result);

        let serialize_arena = Bump::new();
        let source_addr: StateAddr = serialize_arena.alloc_layout(layout.layout).into();
        legacy.init_state(AggrState::new(source_addr, loc));
        legacy.accumulate(
            AggrState::new(source_addr, loc),
            (&entries).into(),
            None,
            entries[0].len(),
        )?;
        let mut serialize_builders = layout.serialize_builders(1);
        {
            let builders = serialize_builders[0].as_tuple_mut().unwrap().as_mut_slice();
            legacy.batch_serialize(&[source_addr], loc, builders)?;
        }
        let serialized_state: BlockEntry = serialize_builders.pop().unwrap().build().into();

        let merge_arena = Bump::new();
        let merged_addr: StateAddr = merge_arena.alloc_layout(layout.layout).into();
        legacy.init_state(AggrState::new(merged_addr, loc));
        legacy.batch_merge(&[merged_addr], loc, &serialized_state, None)?;
        let mut builder = ColumnBuilder::with_capacity(&UInt64Type::data_type().wrap_nullable(), 1);
        legacy.merge_result(AggrState::new(merged_addr, loc), false, &mut builder)?;
        assert_eq!(builder.build(), direct_result);

        unsafe {
            legacy.drop_state(AggrState::new(result_addr, loc));
            legacy.drop_state(AggrState::new(source_addr, loc));
            legacy.drop_state(AggrState::new(merged_addr, loc));
        }

        assert_eq!(drop_count.load(Ordering::SeqCst), 4);
        Ok(())
    }

    #[test]
    fn test_intrusive_modifiers_compose_as_concrete_implementations() -> Result<()> {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let order_by = full_modifier_order_by();
        let function = full_modifier_function(drop_count.clone(), order_by.clone());

        {
            let owner = AggregateStateOwner::new(vec![function.clone()])?;
            let entries = full_modifier_entries();

            function.accumulate(AccumulateInput {
                state: owner.state(0),
                columns: (&entries).into(),
                order_by: &order_by,
                validity: None,
            })?;

            let mut builder =
                ColumnBuilder::with_capacity(&UInt64Type::data_type().wrap_nullable(), 1);
            function.merge_result(MergeResultInput {
                state: owner.state(0),
                builder: &mut builder,
            })?;
            let column = builder.build();
            assert_eq!(
                unsafe { column.index_unchecked(0) },
                ScalarRef::Number(NumberScalar::UInt64(17))
            );
        }

        {
            let left = AggregateStateOwner::new(vec![function.clone()])?;
            let right = AggregateStateOwner::new(vec![function.clone()])?;
            let left_entries: Vec<BlockEntry> = vec![
                UInt64Type::from_data(vec![2, 5]).into(),
                BooleanType::from_data(vec![true, true]).into(),
                UInt64Type::from_data(vec![1, 2]).into(),
            ];
            function.accumulate(AccumulateInput {
                state: left.state(0),
                columns: (&left_entries).into(),
                order_by: &order_by,
                validity: None,
            })?;

            let right_entries: Vec<BlockEntry> = vec![
                UInt64Type::from_data(vec![2, 1]).into(),
                BooleanType::from_data(vec![true, true]).into(),
                UInt64Type::from_data(vec![0, 3]).into(),
            ];
            function.accumulate(AccumulateInput {
                state: right.state(0),
                columns: (&right_entries).into(),
                order_by: &order_by,
                validity: None,
            })?;

            function.merge_states(MergeStatesInput {
                state: left.state(0),
                rhs: right.state(0),
            })?;

            let mut builder =
                ColumnBuilder::with_capacity(&UInt64Type::data_type().wrap_nullable(), 1);
            function.merge_result(MergeResultInput {
                state: left.state(0),
                builder: &mut builder,
            })?;
            let column = builder.build();
            assert_eq!(
                unsafe { column.index_unchecked(0) },
                ScalarRef::Number(NumberScalar::UInt64(8))
            );
        }

        {
            let source_owner = AggregateStateOwner::new(vec![function.clone()])?;
            let entries: Vec<BlockEntry> = vec![
                UInt64Type::from_data(vec![2, 2, 5, 9, 0, 1]).into(),
                BooleanType::from_data(vec![true, true, true, false, true, true]).into(),
                UInt64Type::from_data(vec![3, 1, 2, 0, 4, 5]).into(),
            ];

            function.accumulate(AccumulateInput {
                state: source_owner.state(0),
                columns: (&entries).into(),
                order_by: &order_by,
                validity: None,
            })?;
            let serialized_state = serialize_state(&function, &source_owner)?;

            let serialized_owner = AggregateStateOwner::new(vec![function.clone()])?;
            function.merge_serialized(MergeSerializedInput {
                states: serialized_owner.state_set(0),
                state: &serialized_state,
                filter: None,
            })?;

            let mut builder =
                ColumnBuilder::with_capacity(&UInt64Type::data_type().wrap_nullable(), 1);
            function.merge_result(MergeResultInput {
                state: serialized_owner.state(0),
                builder: &mut builder,
            })?;
            let column = builder.build();
            assert_eq!(
                unsafe { column.index_unchecked(0) },
                ScalarRef::Number(NumberScalar::UInt64(17))
            );
        }

        assert_eq!(drop_count.load(Ordering::SeqCst), 5);
        Ok(())
    }
}
