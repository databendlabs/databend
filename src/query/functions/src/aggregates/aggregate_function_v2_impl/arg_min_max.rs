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
use std::cmp::Ordering;
use std::marker::PhantomData;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnView;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AnyNumberType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::EmptyArrayType;
use databend_common_expression::types::EmptyMapType;
use databend_common_expression::types::NullType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::with_number_mapped_type;

use super::AggregateFunctionV2Factory;
use super::adaptors_v2 as v2;
use super::adaptors_v2::*;
use super::min_max_any::TYPE_MAX;
use super::min_max_any::TYPE_MIN;
use super::serialized_scalar_at;

struct ArgMinMaxBuilder;

impl ArgMinMaxBuilder {
    fn register(registry: &mut v2::AggregateFunctionRegistry) {
        v2::DirectNameRoute::new(
            &["arg_min"],
            ArgMinMaxBuilder::arg_min_max_arguments(),
            ArgMinMaxBuilder::ARG_MIN_FEATURES,
            v2::NullPolicy::Skip,
        )
        .then(v2::MergeRoute::multi_arg(false, Self::create::<TYPE_MIN>))
        .then(v2::MergeRoute::multi_arg(true, Self::create::<TYPE_MIN>))
        .then(v2::PlainRoute::multi_arg(Self::create::<TYPE_MIN>))
        .then(v2::IfRoute::multi_arg(Self::create::<TYPE_MIN>))
        .then(v2::StateRoute::multi_arg(Self::create::<TYPE_MIN>))
        .then(v2::DistinctAliasRoute::multi_arg(Self::create::<TYPE_MIN>))
        .register(registry);
        v2::DirectNameRoute::new(
            &["arg_max"],
            ArgMinMaxBuilder::arg_min_max_arguments(),
            ArgMinMaxBuilder::ARG_MAX_FEATURES,
            v2::NullPolicy::Skip,
        )
        .then(v2::MergeRoute::multi_arg(false, Self::create::<TYPE_MAX>))
        .then(v2::MergeRoute::multi_arg(true, Self::create::<TYPE_MAX>))
        .then(v2::PlainRoute::multi_arg(Self::create::<TYPE_MAX>))
        .then(v2::IfRoute::multi_arg(Self::create::<TYPE_MAX>))
        .then(v2::StateRoute::multi_arg(Self::create::<TYPE_MAX>))
        .then(v2::DistinctAliasRoute::multi_arg(Self::create::<TYPE_MAX>))
        .register(registry);
    }
}

inventory::submit! {
    AggregateFunctionV2Factory {
        register: ArgMinMaxBuilder::register,
    }
}

impl ArgMinMaxBuilder {
    fn arg_min_max_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![
            AggregateArgumentPattern::any(),
            AggregateArgumentPattern::any(),
        ])
    }

    const ARG_MIN_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the argument associated with the minimum value",
        definition: "arg_min(arg, value)",
        example: "select arg_min(name, score) from t",
    };

    const ARG_MAX_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the argument associated with the maximum value",
        definition: "arg_max(arg, value)",
        example: "select arg_max(name, score) from t",
    };
}

pub struct AggregateArgMinMaxState<A, V, const CMP_TYPE: u8>
where
    A: ValueType,
    V: ValueType,
{
    data: Option<(V::Scalar, A::Scalar)>,
}

impl<A, V, const CMP_TYPE: u8> Default for AggregateArgMinMaxState<A, V, CMP_TYPE>
where
    A: ValueType,
    V: ValueType,
{
    fn default() -> Self {
        Self { data: None }
    }
}

impl<A, V, const CMP_TYPE: u8> AggregateArgMinMaxState<A, V, CMP_TYPE>
where
    A: ValueType,
    V: ValueType,
    A::Scalar: BorshSerialize + BorshDeserialize,
    V::Scalar: BorshSerialize + BorshDeserialize,
    for<'a, 'b> V::ScalarRef<'a>: PartialOrd<V::ScalarRef<'b>>,
{
    pub fn state_description(
        arg_type: DataType,
        value_type: DataType,
    ) -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::DataType(BooleanType::data_type()),
            StateSerdeItem::DataType(value_type),
            StateSerdeItem::DataType(arg_type),
        ])
        .with_manual_drop(true)
    }

    fn add(&mut self, value: V::ScalarRef<'_>, arg: A::ScalarRef<'_>) {
        if self.should_change(&value) {
            self.data = Some((V::to_owned_scalar(value), A::to_owned_scalar(arg)));
        }
    }

    fn add_batch(&mut self, args: ColumnView<A>, values: ColumnView<V>, validity: Option<&Bitmap>) {
        match validity {
            Some(validity) => {
                for ((arg, value), valid) in args.iter().zip(values.iter()).zip(validity.iter()) {
                    if valid {
                        self.add(value, arg);
                    }
                }
            }
            None => {
                for (arg, value) in args.iter().zip(values.iter()) {
                    self.add(value, arg);
                }
            }
        }
    }

    fn should_change(&self, value: &V::ScalarRef<'_>) -> bool {
        match &self.data {
            Some((current, _)) => should_change::<V, CMP_TYPE>(&V::to_scalar_ref(current), value),
            None => true,
        }
    }

    fn merge(&mut self, rhs: &Self) {
        if let Some((value, arg)) = &rhs.data
            && self.should_change(&V::to_scalar_ref(value))
        {
            self.data = Some((value.to_owned(), arg.to_owned()));
        }
    }

    fn merge_result(&self, mut builder: A::ColumnBuilderMut<'_>) {
        match &self.data {
            Some((_, arg)) => builder.push_item(A::to_scalar_ref(arg)),
            None => builder.push_default(),
        }
    }
}

fn should_change<V, const CMP_TYPE: u8>(
    current: &V::ScalarRef<'_>,
    value: &V::ScalarRef<'_>,
) -> bool
where
    V: ValueType,
    for<'a, 'b> V::ScalarRef<'a>: PartialOrd<V::ScalarRef<'b>>,
{
    match CMP_TYPE {
        TYPE_MIN => matches!(current.partial_cmp(value), Some(Ordering::Greater)),
        TYPE_MAX => matches!(current.partial_cmp(value), Some(Ordering::Less)),
        _ => unreachable!(),
    }
}

impl ArgMinMaxBuilder {
    fn create<const CMP_TYPE: u8>(
        build: v2::MultiArgBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        let arg_type = build.args_type()[0].clone();
        let value_type = build.args_type()[1].clone();
        Self::create_for_arg::<CMP_TYPE>(build, arg_type, value_type)
    }

    fn create_for_arg<const CMP_TYPE: u8>(
        build: v2::MultiArgBuildContext<'_, impl v2::CombinatorImpl>,
        arg_type: DataType,
        value_type: DataType,
    ) -> Result<AggregateFunctionRef> {
        match &arg_type {
            DataType::String => {
                Self::create_for_value::<StringType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::Boolean => {
                Self::create_for_value::<BooleanType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::Timestamp => {
                Self::create_for_value::<TimestampType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::Date => {
                Self::create_for_value::<DateType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::Null => {
                Self::create_for_value::<NullType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::EmptyArray => {
                Self::create_for_value::<EmptyArrayType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::EmptyMap => {
                Self::create_for_value::<EmptyMapType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::Number(_) => {
                Self::create_for_value::<AnyNumberType, CMP_TYPE>(build, arg_type, value_type)
            }
            _ => Self::create_for_value::<AnyType, CMP_TYPE>(build, arg_type, value_type),
        }
    }

    fn create_for_value<A, const CMP_TYPE: u8>(
        build: v2::MultiArgBuildContext<'_, impl v2::CombinatorImpl>,
        arg_type: DataType,
        value_type: DataType,
    ) -> Result<AggregateFunctionRef>
    where
        A: ValueType,
        A::Scalar: BorshSerialize + BorshDeserialize,
    {
        match &value_type {
            DataType::String => {
                Self::create_instance::<A, StringType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::Boolean => {
                Self::create_instance::<A, BooleanType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::Timestamp => {
                Self::create_instance::<A, TimestampType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::Date => {
                Self::create_instance::<A, DateType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::Null => {
                Self::create_instance::<A, NullType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::EmptyArray => {
                Self::create_instance::<A, EmptyArrayType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::EmptyMap => {
                Self::create_instance::<A, EmptyMapType, CMP_TYPE>(build, arg_type, value_type)
            }
            DataType::Number(_) => {
                with_number_mapped_type!(|NUM| match &value_type {
                    DataType::Number(NumberDataType::NUM) => {
                        Self::create_instance::<A, NumberType<NUM>, CMP_TYPE>(
                            build, arg_type, value_type,
                        )
                    }
                    _ => unreachable!(),
                })
            }
            _ => Self::create_instance::<A, AnyType, CMP_TYPE>(build, arg_type, value_type),
        }
    }

    fn create_instance<A, V, const CMP_TYPE: u8>(
        build: v2::MultiArgBuildContext<'_, impl v2::CombinatorImpl>,
        arg_type: DataType,
        value_type: DataType,
    ) -> Result<AggregateFunctionRef>
    where
        A: ValueType,
        A::Scalar: BorshSerialize + BorshDeserialize,
        V: ValueType,
        V::Scalar: BorshSerialize + BorshDeserialize,
        for<'a, 'b> V::ScalarRef<'a>: PartialOrd<V::ScalarRef<'b>>,
    {
        let state = AggregateArgMinMaxState::<A, V, CMP_TYPE>::state_description(
            arg_type.clone(),
            value_type,
        );
        let implementation = AggregateArgMinMaxImplementation::<A, V, CMP_TYPE>::new();
        build.create_multi_arg_or_null(arg_type.wrap_nullable(), state, implementation)
    }
}

struct AggregateArgMinMaxImplementation<A, V, const CMP_TYPE: u8>
where
    A: ValueType,
    V: ValueType,
{
    _p: PhantomData<fn(A, V)>,
}

impl<A, V, const CMP_TYPE: u8> AggregateArgMinMaxImplementation<A, V, CMP_TYPE>
where
    A: ValueType,
    V: ValueType,
{
    fn new() -> Self {
        Self { _p: PhantomData }
    }
}

impl<A, V, const CMP_TYPE: u8> AggrImpl for AggregateArgMinMaxImplementation<A, V, CMP_TYPE>
where
    A: ValueType,
    A::Scalar: BorshSerialize + BorshDeserialize,
    V: ValueType,
    V::Scalar: BorshSerialize + BorshDeserialize,
    for<'a, 'b> V::ScalarRef<'a>: PartialOrd<V::ScalarRef<'b>>,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateArgMinMaxState::<A, V, CMP_TYPE>::default);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let args = input.columns[0].downcast::<A>().unwrap();
        let values = input.columns[1].downcast::<V>().unwrap();
        input
            .state
            .get::<AggregateArgMinMaxState<A, V, CMP_TYPE>>()
            .add_batch(args, values, input.validity);
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
        let args = input.columns[0].downcast::<A>().unwrap();
        let values = input.columns[1].downcast::<V>().unwrap();
        let arg = args.index(input.row).unwrap();
        let value = values.index(input.row).unwrap();
        input
            .state
            .get::<AggregateArgMinMaxState<A, V, CMP_TYPE>>()
            .add(value, arg);
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        let (flag_builders, builders) = input.builders.split_at_mut(1);
        let (value_builders, arg_builders) = builders.split_at_mut(1);
        let mut flag_builder = BooleanType::downcast_builder(&mut flag_builders[0]);
        let mut value_builder = V::downcast_builder(&mut value_builders[0]);
        let mut arg_builder = A::downcast_builder(&mut arg_builders[0]);
        for state in input.states.iter() {
            let state = state.get::<AggregateArgMinMaxState<A, V, CMP_TYPE>>();
            match &state.data {
                Some((value, arg)) => {
                    flag_builder.push_item(true);
                    value_builder.push_item(V::to_scalar_ref(value));
                    arg_builder.push_item(A::to_scalar_ref(arg));
                }
                None => {
                    flag_builder.push_item(false);
                    value_builder.push_default();
                    arg_builder.push_default();
                }
            }
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            let ScalarRef::Boolean(flag) = serialized_scalar_at(input.state, row, 0) else {
                unreachable!()
            };
            if !flag {
                continue;
            }
            let value = serialized_scalar_at(input.state, row, 1);
            let arg = serialized_scalar_at(input.state, row, 2);
            let value = V::try_downcast_scalar(&value)?;
            let arg = A::try_downcast_scalar(&arg)?;
            state
                .get::<AggregateArgMinMaxState<A, V, CMP_TYPE>>()
                .add(value, arg);
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateArgMinMaxState<A, V, CMP_TYPE>>();
        let rhs = input.rhs.get::<AggregateArgMinMaxState<A, V, CMP_TYPE>>();
        state.merge(rhs);
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let builder = A::downcast_builder(input.builder);
        input
            .state
            .get::<AggregateArgMinMaxState<A, V, CMP_TYPE>>()
            .merge_result(builder);
        Ok(())
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        let state = state.get::<AggregateArgMinMaxState<A, V, CMP_TYPE>>();
        unsafe { std::ptr::drop_in_place(state) };
    }
}
