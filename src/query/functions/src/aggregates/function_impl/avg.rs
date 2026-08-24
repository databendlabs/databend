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
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::i256;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::number::Number;
use databend_common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use num_traits::AsPrimitive;

use super::FunctionFactory;
use super::adaptors::*;

struct AvgBuilder;

impl AvgBuilder {
    const NAME: &'static str = "avg";

    const FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "averages non-null numeric values",
        definition: "avg(expr)",
        example: "select avg(number) from numbers(10)",
    };

    fn arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::any_numeric()])
    }

    fn register(registry: &mut AggregateFunctionRegistry) {
        DirectNameRoute::new(
            &[Self::NAME],
            Self::arguments(),
            Self::FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::unary(false, Self::create))
        .then(MergeRoute::unary(true, Self::create))
        .then(PlainRoute::unary(Self::create))
        .then(IfRoute::unary(Self::create))
        .then(StateRoute::unary(Self::create))
        .then(DistinctRoute::unary(Self::create))
        .register(registry);
    }
}

inventory::submit! {
    FunctionFactory {
        register: AvgBuilder::register,
    }
}

pub struct AggregateNumberAvgState<I, S>
where S: ArgType
{
    value: S::Scalar,
    count: u64,
    _i: PhantomData<fn(I)>,
}

impl<I, S> Default for AggregateNumberAvgState<I, S>
where
    I: ValueType,
    S: ArgType,
    I::Scalar: Number + AsPrimitive<S::Scalar>,
    S::Scalar: Number + AsPrimitive<f64> + std::ops::AddAssign,
{
    fn default() -> Self {
        Self {
            value: S::Scalar::default(),
            count: 0,
            _i: PhantomData,
        }
    }
}

impl<I, S> AggregateNumberAvgState<I, S>
where
    I: ValueType,
    S: ArgType,
    I::Scalar: Number + AsPrimitive<S::Scalar>,
    S::Scalar: Number + AsPrimitive<f64> + std::ops::AddAssign,
{
    pub fn state_description(sum_type: DataType) -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::DataType(sum_type),
            StateSerdeItem::DataType(UInt64Type::data_type()),
        ])
    }
}

impl<I, S> AvgState<I, Float64Type> for AggregateNumberAvgState<I, S>
where
    I: ValueType,
    S: ArgType,
    I::Scalar: Number + AsPrimitive<S::Scalar>,
    S::Scalar: Number + AsPrimitive<f64> + std::ops::AddAssign,
{
    type FunctionInfo = ();

    fn state_description(sum_type: DataType) -> AggregateStateDescription {
        AggregateNumberAvgState::<I, S>::state_description(sum_type)
    }

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: I::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.count += 1;
        self.value += I::to_owned_scalar(value).as_();
        Ok(())
    }

    fn merge(&mut self, rhs: &Self, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.count += rhs.count;
        self.value += rhs.value;
        Ok(())
    }

    fn serialize(
        &self,
        builders: &mut [ColumnBuilder],
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let (sum_builders, count_builders) = builders.split_at_mut(1);
        let mut sum_builder = S::downcast_builder(&mut sum_builders[0]);
        sum_builder.push_item(S::to_scalar_ref(&self.value));
        count_builders[0].push(ScalarRef::Number(NumberScalar::UInt64(self.count)));
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        sum: ScalarRef<'_>,
        count: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let sum = S::try_downcast_scalar(&sum)?;
        let ScalarRef::Number(NumberScalar::UInt64(count)) = count else {
            unreachable!()
        };
        self.value += S::to_owned_scalar(sum);
        self.count += count;
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: <Float64Type as ValueType>::ColumnBuilderMut<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let value = self.value.as_() / (self.count as f64);
        builder.push_item(F64::from(value));
        Ok(())
    }
}

pub struct AggregateDecimalAvgState<const SHOULD_CHECK_OVERFLOW: bool, T>
where T: Decimal
{
    value: T,
    count: u64,
}

impl<const SHOULD_CHECK_OVERFLOW: bool, T> Default
    for AggregateDecimalAvgState<SHOULD_CHECK_OVERFLOW, T>
where T: Decimal
{
    fn default() -> Self {
        Self {
            value: T::default(),
            count: 0,
        }
    }
}

impl<const SHOULD_CHECK_OVERFLOW: bool, T> AggregateDecimalAvgState<SHOULD_CHECK_OVERFLOW, T>
where T: Decimal
{
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::DataType(DataType::Decimal(T::default_decimal_size())),
            StateSerdeItem::DataType(UInt64Type::data_type()),
        ])
    }
}

pub struct DecimalAvgData {
    scale_add: u8,
}

impl<const SHOULD_CHECK_OVERFLOW: bool, T> AggregateDecimalAvgState<SHOULD_CHECK_OVERFLOW, T>
where T: Decimal + std::ops::AddAssign
{
    fn add_internal(&mut self, count: u64, value: T) -> Result<()> {
        self.count += count;
        self.value += value;
        if SHOULD_CHECK_OVERFLOW && (self.value > T::DECIMAL_MAX || self.value < T::DECIMAL_MIN) {
            return Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {:?} not in [{}, {}]",
                self.value,
                T::DECIMAL_MIN,
                T::DECIMAL_MAX,
            )));
        }
        Ok(())
    }
}

impl<const SHOULD_CHECK_OVERFLOW: bool, T> AvgState<DecimalType<T>, DecimalType<T>>
    for AggregateDecimalAvgState<SHOULD_CHECK_OVERFLOW, T>
where T: Decimal + std::ops::AddAssign
{
    type FunctionInfo = DecimalAvgData;

    fn state_description(_sum_type: DataType) -> AggregateStateDescription {
        AggregateDecimalAvgState::<SHOULD_CHECK_OVERFLOW, T>::state_description()
    }

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.add_internal(1, value)
    }

    fn merge(&mut self, rhs: &Self, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.add_internal(rhs.count, rhs.value)
    }

    fn serialize(
        &self,
        builders: &mut [ColumnBuilder],
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let (sum_builders, count_builders) = builders.split_at_mut(1);
        let mut sum_builder = DecimalType::<T>::downcast_builder(&mut sum_builders[0]);
        sum_builder.push_item(DecimalType::<T>::to_scalar_ref(&self.value));
        count_builders[0].push(ScalarRef::Number(NumberScalar::UInt64(self.count)));
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        sum: ScalarRef<'_>,
        count: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let sum = DecimalType::<T>::try_downcast_scalar(&sum)?;
        let ScalarRef::Number(NumberScalar::UInt64(count)) = count else {
            unreachable!()
        };
        self.add_internal(count, sum)
    }

    fn merge_result(
        &mut self,
        mut builder: <DecimalType<T> as ValueType>::ColumnBuilderMut<'_>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        match self
            .value
            .checked_mul(T::e(function_info.scale_add))
            .and_then(|v| v.checked_div(T::from_i128_uncheck(self.count.into())))
        {
            Some(value) => {
                builder.push_item(DecimalType::<T>::to_scalar_ref(&value));
                Ok(())
            }
            None => Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {} mul {}",
                self.value,
                T::e(function_info.scale_add)
            ))),
        }
    }
}

impl AvgBuilder {
    fn create(build: UnaryBuildContext<'_, impl CombinatorImpl>) -> Result<AggregateFunctionRef> {
        let data_type = build.arg_type().clone();
        let display_name = build.name().to_string();

        with_number_mapped_type!(|NUM| match &data_type {
            DataType::Number(NumberDataType::NUM) => {
                type SumNumber = <NUM as ResultTypeOfUnary>::Sum;
                type State = AggregateNumberAvgState<NumberType<NUM>, NumberType<SumNumber>>;
                Self::create_number::<State, NumberType<NUM>>(
                    build,
                    NumberType::<SumNumber>::data_type(),
                )
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => {
                        let decimal_size =
                            DecimalSize::new_unchecked(DECIMAL::MAX_PRECISION, size.scale().max(4));
                        let should_check_overflow = DECIMAL::MAX_PRECISION > i64::MAX_PRECISION
                            && size.precision() > i64::MAX_PRECISION;
                        let scale_add = decimal_size.scale() - size.scale();
                        let return_type = DataType::Decimal(decimal_size);
                        let function_info = DecimalAvgData { scale_add };

                        if should_check_overflow {
                            Self::create_decimal::<AggregateDecimalAvgState<true, DECIMAL>, DECIMAL>(
                                build,
                                return_type,
                                function_info,
                            )
                        } else {
                            Self::create_decimal::<AggregateDecimalAvgState<false, DECIMAL>, DECIMAL>(
                                build,
                                return_type,
                                function_info,
                            )
                        }
                    }
                })
            }
            _ => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                display_name, data_type
            ))),
        })
    }

    fn create_number<S, I>(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
        sum_type: DataType,
    ) -> Result<AggregateFunctionRef>
    where
        S: AvgState<I, Float64Type, FunctionInfo = ()>,
        I: AccessType,
    {
        let state = S::state_description(sum_type);
        Self::create_instance::<S, I, Float64Type>(
            build,
            Float64Type::data_type(),
            state,
            Arc::new(()),
        )
    }

    fn create_decimal<S, T>(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
        return_type: DataType,
        function_info: DecimalAvgData,
    ) -> Result<AggregateFunctionRef>
    where
        S: AvgState<DecimalType<T>, DecimalType<T>, FunctionInfo = DecimalAvgData>,
        T: Decimal,
    {
        Self::create_instance::<S, DecimalType<T>, DecimalType<T>>(
            build,
            return_type,
            S::state_description(DataType::Null),
            Arc::new(function_info),
        )
    }

    fn create_instance<S, I, R>(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
        return_type: DataType,
        state: AggregateStateDescription,
        function_info: Arc<S::FunctionInfo>,
    ) -> Result<AggregateFunctionRef>
    where
        S: AvgState<I, R>,
        I: AccessType,
        R: ValueType,
    {
        let implementation = AggregateAvgImplementation::<S, I, R>::new(function_info);
        build.create_unary_or_null_with_impl::<I, R, _>(
            return_type.wrap_nullable(),
            state,
            implementation,
        )
    }
}

trait AvgState<I, R>: Send + 'static
where
    I: AccessType,
    R: ValueType,
{
    type FunctionInfo: Send + Sync + 'static;

    fn init(function_info: &Self::FunctionInfo) -> Self;

    fn add(&mut self, value: I::ScalarRef<'_>, function_info: &Self::FunctionInfo) -> Result<()>;

    fn add_batch(
        &mut self,
        values: ColumnView<I>,
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

    fn merge(&mut self, rhs: &Self, function_info: &Self::FunctionInfo) -> Result<()>;

    fn merge_owned(&mut self, rhs: &mut Self, function_info: &Self::FunctionInfo) -> Result<()> {
        self.merge(rhs, function_info)
    }

    fn serialize(
        &self,
        builders: &mut [ColumnBuilder],
        function_info: &Self::FunctionInfo,
    ) -> Result<()>;

    fn merge_serialized(
        &mut self,
        sum: ScalarRef<'_>,
        count: ScalarRef<'_>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()>;

    fn merge_result(
        &mut self,
        builder: R::ColumnBuilderMut<'_>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()>;

    unsafe fn drop_state(state: &mut Self, _function_info: &Self::FunctionInfo) {
        unsafe { std::ptr::drop_in_place(state) };
    }

    fn state_description(sum_type: DataType) -> AggregateStateDescription;
}

struct AggregateAvgImplementation<S, I, R>
where
    S: AvgState<I, R>,
    I: AccessType,
    R: ValueType,
{
    function_info: Arc<S::FunctionInfo>,
    _p: PhantomData<fn(S, I, R)>,
}

impl<S, I, R> AggregateAvgImplementation<S, I, R>
where
    S: AvgState<I, R>,
    I: AccessType,
    R: ValueType,
{
    fn new(function_info: Arc<S::FunctionInfo>) -> Self {
        Self {
            function_info,
            _p: PhantomData,
        }
    }
}

impl<S, I, R> UnaryAggrImpl<I, R> for AggregateAvgImplementation<S, I, R>
where
    S: AvgState<I, R>,
    I: AccessType,
    R: ValueType,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(|| S::init(&self.function_info));
    }

    fn accumulate(&self, input: UnaryAccumulateInput<'_>) -> Result<()> {
        let values = input.column.downcast::<I>().unwrap();
        let state = input.state.get::<S>();
        state.add_batch(values, input.validity, &self.function_info)
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
        let values = input.column.downcast::<I>().unwrap();
        let value = values.index(input.row).unwrap();
        let state = input.state.get::<S>();
        state.add(value, &self.function_info)
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            state
                .get::<S>()
                .serialize(input.builders, &self.function_info)?;
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            state.get::<S>().merge_serialized(
                super::serialized_scalar_at(input.state, row, 0),
                super::serialized_scalar_at(input.state, row, 1),
                &self.function_info,
            )?;
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        input
            .state
            .get::<S>()
            .merge_owned(input.rhs.get::<S>(), &self.function_info)
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let builder = R::downcast_builder(input.builder);
        input
            .state
            .get::<S>()
            .merge_result(builder, &self.function_info)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        let state = state.get::<S>();
        unsafe { S::drop_state(state, &self.function_info) };
    }
}
