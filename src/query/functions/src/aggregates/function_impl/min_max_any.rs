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
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::EmptyArrayType;
use databend_common_expression::types::EmptyMapType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::i256;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;

use super::FunctionFactory;
use super::adaptors::*;
use super::serialized_scalar_at;

pub const TYPE_ANY: u8 = 0;
pub const TYPE_MIN: u8 = 1;
pub const TYPE_MAX: u8 = 2;

struct MinMaxAnyBuilder;

impl MinMaxAnyBuilder {
    fn register(registry: &mut AggregateFunctionRegistry) {
        Self::route::<TYPE_MIN>().register(registry);
        Self::route::<TYPE_MAX>().register(registry);
        Self::route::<TYPE_ANY>().register(registry);
    }
}

inventory::submit! {
    FunctionFactory {
        register: MinMaxAnyBuilder::register,
    }
}

impl MinMaxAnyBuilder {
    fn min_max_any_arguments() -> ArgumentsPattern {
        ArgumentsPattern::fixed(vec![ArgumentPattern::any()])
    }

    const MIN_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the minimum input value",
        definition: "min(expr)",
        example: "select min(number) from numbers(10)",
    };

    const MAX_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the maximum input value",
        definition: "max(expr)",
        example: "select max(number) from numbers(10)",
    };

    const ANY_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: false,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns any input value",
        definition: "any(expr)",
        example: "select any(number) from numbers(10)",
    };
}

pub struct AggregateMinMaxAnyState<T, const CMP_TYPE: u8>
where T: ValueType
{
    value: Option<T::Scalar>,
}

impl<T, const CMP_TYPE: u8> Default for AggregateMinMaxAnyState<T, CMP_TYPE>
where T: ValueType
{
    fn default() -> Self {
        Self { value: None }
    }
}

impl<T, const CMP_TYPE: u8> AggregateMinMaxAnyState<T, CMP_TYPE>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    pub fn state_description(
        data_type: DataType,
        need_manual_drop: bool,
    ) -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::DataType(BooleanType::data_type()),
            StateSerdeItem::DataType(data_type),
        ])
        .with_manual_drop(need_manual_drop)
    }
}

impl<T, const CMP_TYPE: u8> UnaryState<T, T> for AggregateMinMaxAnyState<T, CMP_TYPE>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
    for<'a, 'b> T::ScalarRef<'a>: PartialOrd<T::ScalarRef<'b>>,
{
    type FunctionInfo = ();

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        match &self.value {
            Some(current) => {
                if should_change::<T, CMP_TYPE>(&T::to_scalar_ref(current), &value) {
                    self.value = Some(T::to_owned_scalar(value));
                }
            }
            None => {
                self.value = Some(T::to_owned_scalar(value));
            }
        }
        Ok(())
    }

    fn add_batch(
        &mut self,
        values: ColumnView<T>,
        validity: Option<&Bitmap>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        match validity {
            Some(validity) => {
                for (value, valid) in values.iter().zip(validity.iter()) {
                    if valid {
                        self.add(value, function_info)?;
                    }
                }
            }
            None => {
                for value in values.iter() {
                    self.add(value, function_info)?;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(value) = &rhs.value {
            self.add(T::to_scalar_ref(value), &())?;
        }
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: T::ColumnBuilderMut<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        match &self.value {
            Some(value) => builder.push_item(T::to_scalar_ref(value)),
            None => builder.push_default(),
        }
        Ok(())
    }

    fn serialize(
        &self,
        _builders: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        unreachable!("min/max/any state serializes with two columns");
    }

    fn merge_serialized(
        &mut self,
        _value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        unreachable!("min/max/any state merges two serialized columns");
    }
}

fn should_change<T, const CMP_TYPE: u8>(
    current: &T::ScalarRef<'_>,
    value: &T::ScalarRef<'_>,
) -> bool
where
    T: ValueType,
    for<'a, 'b> T::ScalarRef<'a>: PartialOrd<T::ScalarRef<'b>>,
{
    match CMP_TYPE {
        TYPE_ANY => false,
        TYPE_MIN => matches!(current.partial_cmp(value), Some(Ordering::Greater)),
        TYPE_MAX => matches!(current.partial_cmp(value), Some(Ordering::Less)),
        _ => unreachable!(),
    }
}

impl MinMaxAnyBuilder {
    fn legacy_signatures(_params: &[Scalar], state_type: &DataType) -> Vec<Vec<DataType>> {
        let DataType::Tuple(fields) = state_type else {
            return Vec::new();
        };
        match fields.as_slice() {
            [DataType::Boolean, argument_type, ..] => vec![vec![argument_type.clone()]],
            _ => Vec::new(),
        }
    }

    fn route<const CMP_TYPE: u8>() -> DirectNameRoute {
        let (names, features, resolver) = match CMP_TYPE {
            TYPE_MIN => (
                &["min"][..],
                Self::MIN_FEATURES,
                Some(Self::legacy_signatures as LegacySignatureResolver),
            ),
            TYPE_MAX => (
                &["max"][..],
                Self::MAX_FEATURES,
                Some(Self::legacy_signatures as LegacySignatureResolver),
            ),
            TYPE_ANY => (&["any", "any_value"][..], Self::ANY_FEATURES, None),
            _ => unreachable!(),
        };
        let route = DirectNameRoute::new(
            names,
            Self::min_max_any_arguments(),
            features,
            NullPolicy::Skip,
        );
        let route = match resolver {
            Some(resolver) => route
                .then(
                    MergeRoute::unary(false, Self::create::<CMP_TYPE>)
                        .with_legacy_signature_resolver(resolver),
                )
                .then(
                    MergeRoute::unary(true, Self::create::<CMP_TYPE>)
                        .with_legacy_signature_resolver(resolver),
                ),
            None => route
                .then(MergeRoute::unary(false, Self::create::<CMP_TYPE>))
                .then(MergeRoute::unary(true, Self::create::<CMP_TYPE>)),
        };
        route
            .then(PlainRoute::unary(Self::create::<CMP_TYPE>))
            .then(IfRoute::unary(Self::create::<CMP_TYPE>))
            .then(StateRoute::unary(Self::create::<CMP_TYPE>))
            .then(DistinctAliasRoute::unary(Self::create::<CMP_TYPE>))
    }

    fn create<const CMP_TYPE: u8>(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        let data_type = build.arg_type().clone();
        let need_drop = need_manual_drop_state(&data_type);

        match data_type {
            DataType::Boolean => {
                Self::create_instance::<BooleanType, CMP_TYPE>(build, DataType::Boolean, need_drop)
            }
            DataType::Timestamp => Self::create_instance::<TimestampType, CMP_TYPE>(
                build,
                DataType::Timestamp,
                need_drop,
            ),
            DataType::Date => {
                Self::create_instance::<DateType, CMP_TYPE>(build, DataType::Date, need_drop)
            }
            DataType::EmptyArray => Self::create_instance::<EmptyArrayType, CMP_TYPE>(
                build,
                DataType::EmptyArray,
                need_drop,
            ),
            DataType::EmptyMap => Self::create_instance::<EmptyMapType, CMP_TYPE>(
                build,
                DataType::EmptyMap,
                need_drop,
            ),
            DataType::String => {
                Self::create_instance::<StringType, CMP_TYPE>(build, DataType::String, need_drop)
            }
            DataType::Number(number_type) => {
                let data_type = DataType::Number(number_type);
                with_number_mapped_type!(|NUM| match &data_type {
                    DataType::Number(NumberDataType::NUM) => {
                        Self::create_instance::<NumberType<NUM>, CMP_TYPE>(
                            build,
                            data_type.clone(),
                            need_drop,
                        )
                    }
                    _ => unreachable!(),
                })
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => Self::create_instance::<
                        DecimalType<DECIMAL>,
                        CMP_TYPE,
                    >(
                        build, DataType::Decimal(size), need_drop,
                    ),
                })
            }
            data_type => Self::create_instance::<AnyType, CMP_TYPE>(build, data_type, need_drop),
        }
    }

    fn create_instance<T, const CMP_TYPE: u8>(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
        return_type: DataType,
        need_manual_drop: bool,
    ) -> Result<AggregateFunctionRef>
    where
        T: ValueType,
        T::Scalar: BorshSerialize + BorshDeserialize,
        for<'a, 'b> T::ScalarRef<'a>: PartialOrd<T::ScalarRef<'b>>,
    {
        let state = AggregateMinMaxAnyState::<T, CMP_TYPE>::state_description(
            return_type.clone(),
            need_manual_drop,
        );
        let implementation = AggregateMinMaxAnyImplementation::<T, CMP_TYPE>::new();

        build.create_unary_or_null_with_impl::<T, T, _>(
            return_type.wrap_nullable(),
            state,
            implementation,
        )
    }
}

struct AggregateMinMaxAnyImplementation<T, const CMP_TYPE: u8>
where T: ValueType
{
    _p: PhantomData<fn(T)>,
}

impl<T, const CMP_TYPE: u8> AggregateMinMaxAnyImplementation<T, CMP_TYPE>
where T: ValueType
{
    fn new() -> Self {
        Self { _p: PhantomData }
    }
}

impl<T, const CMP_TYPE: u8> UnaryAggrImpl<T, T> for AggregateMinMaxAnyImplementation<T, CMP_TYPE>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
    for<'a, 'b> T::ScalarRef<'a>: PartialOrd<T::ScalarRef<'b>>,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateMinMaxAnyState::<T, CMP_TYPE>::default);
    }

    fn accumulate(&self, input: UnaryAccumulateInput<'_>) -> Result<()> {
        let values = input.column.downcast::<T>().unwrap();
        let state = input.state.get::<AggregateMinMaxAnyState<T, CMP_TYPE>>();
        state.add_batch(values, input.validity, &())
    }

    fn accumulate_keys(&self, input: UnaryAccumulateKeysInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            self.accumulate_row(UnaryAccumulateRowInput {
                state,
                column: input.column,
                row,
            })?;
        }
        Ok(())
    }

    fn accumulate_row(&self, input: UnaryAccumulateRowInput<'_>) -> Result<()> {
        let values = input.column.downcast::<T>().unwrap();
        let value = values.index(input.row).unwrap();
        input
            .state
            .get::<AggregateMinMaxAnyState<T, CMP_TYPE>>()
            .add(value, &())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        let (flag_builders, value_builders) = input.builders.split_at_mut(1);
        let mut flag_builder = BooleanType::downcast_builder(&mut flag_builders[0]);
        let mut value_builder = T::downcast_builder(&mut value_builders[0]);
        for state in input.states.iter() {
            let state = state.get::<AggregateMinMaxAnyState<T, CMP_TYPE>>();
            match &state.value {
                Some(value) => {
                    flag_builder.push_item(true);
                    value_builder.push_item(T::to_scalar_ref(value));
                }
                None => {
                    flag_builder.push_item(false);
                    value_builder.push_default();
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
            let value = T::try_downcast_scalar(&value)?;
            state
                .get::<AggregateMinMaxAnyState<T, CMP_TYPE>>()
                .add(value, &())?;
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateMinMaxAnyState<T, CMP_TYPE>>();
        let rhs = input.rhs.get::<AggregateMinMaxAnyState<T, CMP_TYPE>>();
        state.merge(rhs)
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let builder = T::downcast_builder(input.builder);
        input
            .state
            .get::<AggregateMinMaxAnyState<T, CMP_TYPE>>()
            .merge_result(builder, &())
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        let state = state.get::<AggregateMinMaxAnyState<T, CMP_TYPE>>();
        unsafe { std::ptr::drop_in_place(state) };
    }
}

fn need_manual_drop_state(data_type: &DataType) -> bool {
    match data_type {
        DataType::Binary
        | DataType::String
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Bitmap
        | DataType::Tuple(_)
        | DataType::Variant
        | DataType::Geometry
        | DataType::Geography
        | DataType::Vector(_) => true,
        DataType::Nullable(data_type) => need_manual_drop_state(data_type),
        DataType::AggregateState(state) => need_manual_drop_state(state.physical_type()),
        DataType::Null
        | DataType::EmptyArray
        | DataType::EmptyMap
        | DataType::Boolean
        | DataType::Number(_)
        | DataType::Decimal(_)
        | DataType::Timestamp
        | DataType::TimestampTz
        | DataType::Date
        | DataType::Interval
        | DataType::Opaque(_)
        | DataType::Generic(_)
        | DataType::StageLocation => false,
    }
}
