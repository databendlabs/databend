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

use databend_common_column::types::months_days_micros;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::i256;
use databend_common_expression::types::number::Number;
use databend_common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use num_traits::AsPrimitive;

use super::AggregateFunctionDefinition;
use super::AggregateFunctionV2Factory;
use super::adaptors_v2 as v2;
use super::adaptors_v2::UnaryState;
use super::adaptors_v2::create_unary_distinct_or_null_aggregate_function;
use super::adaptors_v2::*;
use super::aggregate_function_signature;

pub struct AggregateNumberSumState<R>
where R: ArgType
{
    value: R::Scalar,
}

impl<R> Default for AggregateNumberSumState<R>
where
    R: ArgType,
    R::Scalar: Default,
{
    fn default() -> Self {
        Self {
            value: R::Scalar::default(),
        }
    }
}

pub type AggregateSumUInt64State = AggregateNumberSumState<NumberType<u64>>;

struct SumBuilder;

impl SumBuilder {
    fn register(registry: &mut v2::AggregateFunctionRegistry) {
        let sum = Self::sum_definition();
        let sum0 = Self::sum0_definition("sum0");
        let sum_zero = Self::sum0_definition("sum_zero");

        sum.register_with_merge_combinators(registry);
        AggregateFunctionDefinition::new(
            "sum_distinct",
            SumBuilder::sum_distinct_arguments(),
            SumBuilder::SUM_DISTINCT_FEATURES,
            SumBuilder::create_distinct,
        )
        .register(registry);
        sum0.register_with_merge_combinators(registry);
        sum_zero.register_with_merge_combinators(registry);

        AggregateFunctionDefinition::new(
            "sum_if",
            AggregateArgumentsPattern::if_condition(Self::sum_arguments()),
            Self::SUM_IF_FEATURES,
            Self::try_create,
        )
        .register(registry);
        AggregateFunctionDefinition::new(
            "sum_state",
            Self::sum_arguments(),
            Self::STATE_FEATURES,
            Self::try_create,
        )
        .register(registry);
        AggregateFunctionDefinition::new(
            "sum0_state",
            Self::sum_zero_arguments(),
            Self::STATE_FEATURES,
            Self::try_create_zero,
        )
        .register(registry);
        AggregateFunctionDefinition::new(
            "sum_zero_state",
            Self::sum_zero_arguments(),
            Self::SUM_ZERO_STATE_FEATURES,
            Self::try_create_zero,
        )
        .register(registry);
        AggregateFunctionDefinition::new(
            "sum0_distinct",
            Self::sum_zero_arguments(),
            Self::SUM_ZERO_FEATURES,
            Self::try_create_zero,
        )
        .register(registry);
        AggregateFunctionDefinition::new(
            "sum_zero_distinct",
            Self::sum_zero_arguments(),
            Self::SUM_ZERO_FEATURES,
            Self::try_create_zero,
        )
        .register(registry);
    }
}

inventory::submit! {
    AggregateFunctionV2Factory {
        register: SumBuilder::register,
    }
}

impl SumBuilder {
    fn sum_definition() -> AggregateFunctionDefinition {
        AggregateFunctionDefinition::new(
            "sum",
            Self::sum_arguments(),
            Self::SUM_FEATURES,
            Self::try_create,
        )
    }

    fn sum0_definition(name: &'static str) -> AggregateFunctionDefinition {
        AggregateFunctionDefinition::new(
            name,
            Self::sum_zero_arguments(),
            Self::SUM_ZERO_FEATURES,
            Self::try_create_zero,
        )
    }

    fn sum_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::one_of(vec![
            AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::any_numeric()]),
            AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::exact(
                DataType::Interval,
            )]),
        ])
    }

    fn sum_zero_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::exact(
            NumberType::<u64>::data_type(),
        )])
    }

    fn sum_distinct_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::one_of(vec![
            AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::any_numeric()]),
            AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::exact(
                DataType::Interval,
            )]),
        ])
    }

    const SUM_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "sums non-null numeric or interval values",
        definition: "sum(expr)",
        example: "select sum(number) from numbers(10)",
    };

    const SUM_DISTINCT_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "sums distinct non-null numeric or interval values",
        definition: "sum_distinct(expr)",
        example: "select sum_distinct(number) from numbers(10)",
    };

    const SUM_ZERO_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "sums UInt64 values and returns zero when no values are aggregated",
        definition: "sum0(expr)",
        example: "select sum0(number) from numbers(10)",
    };

    const SUM_IF_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "sums input values matching a boolean condition",
        definition: "sum_if(expr, cond)",
        example: "select sum_if(number, number > 0) from numbers(10)",
    };

    const STATE_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the serialized aggregate state",
        definition: "aggregate_state(args...)",
        example: "select sum_state(number) from numbers(10)",
    };

    const SUM_ZERO_STATE_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the serialized aggregate state",
        definition: "aggregate_state(args...)",
        example: "select sum_zero_state(number) from numbers(10)",
    };
}

impl<R> AggregateNumberSumState<R>
where R: ArgType
{
    pub fn state_description(data_type: DataType) -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::DataType(data_type),
        ])
    }
}

impl<T, R> UnaryState<NumberType<T>, NumberType<R>> for AggregateNumberSumState<NumberType<R>>
where
    T: Number + AsPrimitive<R>,
    R: Number + AsPrimitive<f64> + std::ops::AddAssign,
{
    type FunctionInfo = ();

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.value += value.as_();
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value += rhs.value;
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: <NumberType<R> as ValueType>::ColumnBuilderMut<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        builder.push_item(NumberType::<R>::to_scalar_ref(&self.value));
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let mut builder = NumberType::<R>::downcast_builder(builder);
        builder.push_item(NumberType::<R>::to_scalar_ref(&self.value));
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let value = NumberType::<R>::try_downcast_scalar(&value)?;
        self.value += NumberType::<R>::to_owned_scalar(value);
        Ok(())
    }
}

pub struct AggregateDecimalSumState<const SHOULD_CHECK_OVERFLOW: bool, T>
where T: Decimal
{
    value: T::U64Array,
}

impl<const SHOULD_CHECK_OVERFLOW: bool, T> Default
    for AggregateDecimalSumState<SHOULD_CHECK_OVERFLOW, T>
where T: Decimal
{
    fn default() -> Self {
        Self {
            value: T::U64Array::default(),
        }
    }
}

impl<const SHOULD_CHECK_OVERFLOW: bool, T> AggregateDecimalSumState<SHOULD_CHECK_OVERFLOW, T>
where T: Decimal
{
    pub fn state_description(data_type: DataType) -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::DataType(data_type),
        ])
    }
}

impl<const SHOULD_CHECK_OVERFLOW: bool, T> UnaryState<DecimalType<T>, DecimalType<T>>
    for AggregateDecimalSumState<SHOULD_CHECK_OVERFLOW, T>
where T: Decimal + std::ops::AddAssign
{
    type FunctionInfo = ();

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T, _function_info: &Self::FunctionInfo) -> Result<()> {
        let mut sum = T::from_u64_array(self.value);
        sum += value;

        if SHOULD_CHECK_OVERFLOW && (sum > T::DECIMAL_MAX || sum < T::DECIMAL_MIN) {
            return Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {:?} not in [{}, {}]",
                sum,
                T::DECIMAL_MIN,
                T::DECIMAL_MAX,
            )));
        }

        self.value = sum.to_u64_array();
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.add(T::from_u64_array(rhs.value), &())
    }

    fn merge_result(
        &mut self,
        mut builder: <DecimalType<T> as ValueType>::ColumnBuilderMut<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let value = T::from_u64_array(self.value);
        builder.push_item(DecimalType::<T>::to_scalar_ref(&value));
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let value = T::from_u64_array(self.value);
        let mut builder = DecimalType::<T>::downcast_builder(builder);
        builder.push_item(DecimalType::<T>::to_scalar_ref(&value));
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let value = DecimalType::<T>::try_downcast_scalar(&value)?;
        self.add(value, &())
    }
}

#[derive(Default)]
pub struct AggregateIntervalSumState {
    value: months_days_micros,
}

impl AggregateIntervalSumState {
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::DataType(DataType::Interval),
        ])
    }
}

impl UnaryState<IntervalType, IntervalType> for AggregateIntervalSumState {
    type FunctionInfo = ();

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(
        &mut self,
        value: months_days_micros,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let res = self.value.total_micros() + value.total_micros();
        self.value = months_days_micros(res as i128);
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        let res = self.value.total_micros() + rhs.value.total_micros();
        self.value = months_days_micros(res as i128);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: <IntervalType as ValueType>::ColumnBuilderMut<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        builder.push_item(IntervalType::to_scalar_ref(&self.value));
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let mut builder = IntervalType::downcast_builder(builder);
        builder.push_item(IntervalType::to_scalar_ref(&self.value));
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let value = IntervalType::try_downcast_scalar(&value)?;
        self.add(value, &())
    }
}

impl SumBuilder {
    fn try_create_zero(request: AggregateFunctionRequest<'_>) -> Result<AggregateFunctionRef> {
        if (request.name.eq_ignore_ascii_case("sum0")
            || request.name.eq_ignore_ascii_case("sum_zero"))
            && request.args_type.iter().any(DataType::is_null)
        {
            return Err(ErrorCode::InvalidArgument(format!(
                "Invalid argument type for {}, must be uint64",
                request.name
            )));
        }
        let route = v2::AggregateFunctionNameRoutePath::root(request);

        if let Some(route) = route.names(&["sum0", "sum_zero"]) {
            return route
                .plain_or_null()
                .build_with_unary_input(Self::SUM_ZERO_FEATURES, Self::create_zero);
        }

        if let Some(route) = route.names(&["sum0_state"]) {
            if let Some(function) = route.state_null_argument_result()? {
                return Ok(function);
            }
            let state_plan = route.state_nullable_input_plan(true);
            return route
                .state_combinator(state_plan)
                .build_with_unary_input(Self::STATE_FEATURES, Self::create_zero);
        }

        if let Some(route) = route.names(&["sum_zero_state"]) {
            if let Some(function) = route.state_null_argument_result()? {
                return Ok(function);
            }
            let state_plan = route.state_nullable_input_plan(true);
            return route
                .state_combinator(state_plan)
                .build_with_unary_input(Self::SUM_ZERO_STATE_FEATURES, Self::create_zero);
        }

        if let Some(route) = route.names(&["sum0_distinct", "sum_zero_distinct"]) {
            return route
                .distinct_combinator(v2::NullPolicy::ReturnsDefaultWhenOnlyNull, true)
                .build_with_unary_input(Self::SUM_ZERO_FEATURES, Self::create_zero);
        }

        route.unknown()
    }

    fn try_create(request: AggregateFunctionRequest<'_>) -> Result<AggregateFunctionRef> {
        let route = v2::AggregateFunctionNameRoutePath::root(request);

        if let Some(route) = route.names(&["sum"]) {
            if let Some(function) = route.plain_null_argument_result(false)? {
                return Ok(function);
            }
            return route
                .plain_or_null()
                .build_with_unary_input(Self::SUM_FEATURES, Self::create);
        }

        if let Some(route) = route.names(&["sum_if"]) {
            if let Some(function) = route.null_argument_result(false)? {
                return Ok(function);
            }
            return route
                .if_combinator(v2::NullPolicy::Skip, true)?
                .build_with_unary_input(Self::SUM_IF_FEATURES, Self::create);
        }

        if let Some(route) = route.names(&["sum_state"]) {
            if let Some(function) = route.state_null_argument_result()? {
                return Ok(function);
            }
            let state_plan = route.state_nullable_input_plan(false);
            return route
                .state_combinator(state_plan)
                .build_with_unary_input(Self::STATE_FEATURES, Self::create);
        }

        route.unknown()
    }

    fn create(
        build: v2::UnaryBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        let data_type = build.arg_type().clone();

        #[rustfmt::skip]
        let function = with_number_mapped_type!(|NUM| match &data_type {
            DataType::Number(NumberDataType::NUM) => {
                type ResultNumber = <NUM as ResultTypeOfUnary>::Sum;
                type State = AggregateNumberSumState<NumberType<ResultNumber>>;
                build.create_unary_or_null::<
                    State,
                    NumberType<NUM>,
                    NumberType<ResultNumber>,
                >(
                    NumberType::<ResultNumber>::data_type().wrap_nullable(),
                    State::state_description(NumberType::<ResultNumber>::data_type()),
                    (),
                )
            }
            DataType::Interval => build.create_unary_or_null::<
                AggregateIntervalSumState,
                IntervalType,
                IntervalType,
            >(
                DataType::Interval.wrap_nullable(),
                AggregateIntervalSumState::state_description(),
                (),
            ),
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => {
                        let decimal_size =
                            DecimalSize::new_unchecked(DECIMAL::MAX_PRECISION, size.scale());
                        let return_type = DataType::Decimal(decimal_size);
                        let should_check_overflow = DECIMAL::MAX_PRECISION > i64::MAX_PRECISION
                            && size.precision() > i64::MAX_PRECISION;

                        if should_check_overflow {
                            build.create_unary_or_null::<
                                AggregateDecimalSumState<true, DECIMAL>,
                                DecimalType<DECIMAL>,
                                DecimalType<DECIMAL>,
                            >(
                                return_type.wrap_nullable(),
                                AggregateDecimalSumState::<true, DECIMAL>::state_description(
                                    return_type,
                                ),
                                (),
                            )
                        } else {
                            build.create_unary_or_null::<
                                AggregateDecimalSumState<false, DECIMAL>,
                                DecimalType<DECIMAL>,
                                DecimalType<DECIMAL>,
                            >(
                                return_type.wrap_nullable(),
                                AggregateDecimalSumState::<false, DECIMAL>::state_description(
                                    return_type,
                                ),
                                (),
                            )
                        }
                    }
                })
            }
            _ => Err(ErrorCode::BadDataValueType(format!(
                "sum does not support type '{:?}'",
                data_type
            ))),
        });
        function
    }

    fn create_distinct(request: AggregateFunctionRequest<'_>) -> Result<AggregateFunctionRef> {
        if request.args_type[0].is_null() {
            return v2::try_create_null_argument_result_function(request, false);
        }

        let features = &Self::SUM_DISTINCT_FEATURES;
        let combinator = v2::PlainCombinator;
        let distinct_args_type = request
            .args_type
            .iter()
            .map(DataType::remove_nullable)
            .collect::<Vec<_>>();

        let data_type = request.args_type[0].remove_nullable();

        with_number_mapped_type!(|NUM| match &data_type {
            DataType::Number(NumberDataType::NUM) => {
                type ResultNumber = <NUM as ResultTypeOfUnary>::Sum;
                type State = AggregateNumberSumState<NumberType<ResultNumber>>;
                create_unary_distinct_or_null_aggregate_function::<
                    State,
                    NumberType<NUM>,
                    NumberType<ResultNumber>,
                    _,
                >(
                    combinator,
                    aggregate_function_signature(
                        request,
                        NumberType::<ResultNumber>::data_type().wrap_nullable(),
                    ),
                    features.clone(),
                    State::state_description(NumberType::<ResultNumber>::data_type()),
                    (),
                    distinct_args_type.clone(),
                )
            }
            DataType::Interval => create_unary_distinct_or_null_aggregate_function::<
                AggregateIntervalSumState,
                IntervalType,
                IntervalType,
                _,
            >(
                combinator,
                aggregate_function_signature(request, DataType::Interval.wrap_nullable()),
                features.clone(),
                AggregateIntervalSumState::state_description(),
                (),
                distinct_args_type.clone(),
            ),
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => {
                        let decimal_size =
                            DecimalSize::new_unchecked(DECIMAL::MAX_PRECISION, size.scale());
                        let return_type = DataType::Decimal(decimal_size);
                        let should_check_overflow = DECIMAL::MAX_PRECISION > i64::MAX_PRECISION
                            && size.precision() > i64::MAX_PRECISION;

                        if should_check_overflow {
                            create_unary_distinct_or_null_aggregate_function::<
                                AggregateDecimalSumState<true, DECIMAL>,
                                DecimalType<DECIMAL>,
                                DecimalType<DECIMAL>,
                                _,
                            >(
                                combinator,
                                aggregate_function_signature(request, return_type.wrap_nullable()),
                                features.clone(),
                                AggregateDecimalSumState::<true, DECIMAL>::state_description(
                                    return_type,
                                ),
                                (),
                                distinct_args_type.clone(),
                            )
                        } else {
                            create_unary_distinct_or_null_aggregate_function::<
                                AggregateDecimalSumState<false, DECIMAL>,
                                DecimalType<DECIMAL>,
                                DecimalType<DECIMAL>,
                                _,
                            >(
                                combinator,
                                aggregate_function_signature(request, return_type.wrap_nullable()),
                                features.clone(),
                                AggregateDecimalSumState::<false, DECIMAL>::state_description(
                                    return_type,
                                ),
                                (),
                                distinct_args_type.clone(),
                            )
                        }
                    }
                })
            }
            _ => Err(ErrorCode::BadDataValueType(format!(
                "sum_distinct does not support type '{:?}'",
                request.args_type[0]
            ))),
        })
    }

    fn create_zero(
        build: v2::UnaryBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        if build.arg_type().remove_nullable() != NumberType::<u64>::data_type() {
            return Err(ErrorCode::InvalidArgument(format!(
                "Invalid argument type for {}, must be uint64",
                build.name()
            )));
        }

        build.create_unary::<AggregateSumUInt64State, NumberType<u64>, NumberType<u64>>(
            NumberType::<u64>::data_type(),
            AggregateSumUInt64State::state_description(NumberType::<u64>::data_type()),
            (),
        )
    }
}
