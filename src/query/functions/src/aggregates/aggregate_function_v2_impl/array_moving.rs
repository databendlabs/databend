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
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::i256;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::number::Number;
use databend_common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use num_traits::AsPrimitive;

use super::super::extract_number_param;
use super::FunctionFactory;
use super::adaptors::*;

struct ArrayMovingBuilder;

impl ArrayMovingBuilder {
    fn register(registry: &mut AggregateFunctionRegistry) {
        Self::avg_route().register(registry);
        Self::sum_route().register(registry);
    }
}

inventory::submit! {
    FunctionFactory {
        register: ArrayMovingBuilder::register,
    }
}

impl ArrayMovingBuilder {
    fn array_moving_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::one_of(vec![
            AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::any_numeric()]),
            AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::exact(DataType::Null)]),
        ])
    }
}

#[derive(Clone, Copy)]
enum ArrayMovingKind {
    Avg,
    Sum,
}

#[derive(Clone)]
struct ArrayMovingInfo {
    window_size: Option<usize>,
    return_type: DataType,
    scale_add: u8,
    kind: ArrayMovingKind,
}

pub struct AggregateNumberArrayMovingState<I, S>
where
    I: ValueType,
    S: ValueType,
{
    values: Vec<I::Scalar>,
    _p: PhantomData<fn(S)>,
}

impl<I, S> Default for AggregateNumberArrayMovingState<I, S>
where
    I: ValueType,
    S: ValueType,
{
    fn default() -> Self {
        Self {
            values: Vec::new(),
            _p: PhantomData,
        }
    }
}

impl<I, S> AggregateNumberArrayMovingState<I, S>
where
    I: ValueType,
    S: ValueType,
{
    pub fn state_description(serialized_type: DataType) -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::DataType(serialized_type),
        ])
        .with_manual_drop(true)
    }
}

impl<I, S> AggregateNumberArrayMovingState<I, S>
where
    I: ValueType + AccessType + ArgType,
    S: ValueType + AccessType + ArgType,
    I::Scalar: Number + AsPrimitive<S::Scalar>,
    S::Scalar: Number + AsPrimitive<f64> + std::ops::AddAssign + std::ops::SubAssign,
{
    fn add_default(&mut self) {
        self.values.push(I::Scalar::default());
    }

    fn add(&mut self, value: I::ScalarRef<'_>) {
        self.values.push(I::to_owned_scalar(value));
    }

    fn append(&mut self, rhs: &mut Self) {
        self.values.append(&mut rhs.values);
    }

    fn serialize(&self, builder: &mut ColumnBuilder) {
        let mut inner_builder = ColumnBuilder::with_capacity(&I::data_type(), self.values.len());
        {
            let mut typed_builder = I::downcast_builder(&mut inner_builder);
            for value in &self.values {
                typed_builder.push_item(I::to_scalar_ref(value));
            }
        }
        builder.push(ScalarRef::Array(inner_builder.build()));
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let values = ArrayType::<I>::try_downcast_scalar(&value)?;
        for value in I::iter_column(&values) {
            self.add(value);
        }
        Ok(())
    }

    fn merge_sum_result(&self, builder: &mut ColumnBuilder, window_size: usize) {
        let mut sum = S::Scalar::default();
        let mut inner_builder = ColumnBuilder::with_capacity(&S::data_type(), self.values.len());
        {
            let mut typed_builder = S::downcast_builder(&mut inner_builder);
            for (index, value) in self.values.iter().enumerate() {
                sum += value.as_();
                if index >= window_size {
                    sum -= self.values[index - window_size].as_();
                }
                typed_builder.push_item(S::to_scalar_ref(&sum));
            }
        }
        builder.push(ScalarRef::Array(inner_builder.build()));
    }

    fn merge_avg_result(&self, builder: &mut ColumnBuilder, window_size: usize) {
        let mut sum = S::Scalar::default();
        let mut values = Vec::with_capacity(self.values.len());
        for (index, value) in self.values.iter().enumerate() {
            sum += value.as_();
            if index >= window_size {
                sum -= self.values[index - window_size].as_();
            }
            values.push(F64::from(sum.as_() / window_size as f64));
        }
        let column = Float64Type::upcast_column(values.into());
        builder.push(ScalarRef::Array(column));
    }
}

pub struct AggregateDecimalArrayMovingState<T>
where T: Decimal
{
    values: Vec<T>,
}

impl<T> Default for AggregateDecimalArrayMovingState<T>
where T: Decimal
{
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> AggregateDecimalArrayMovingState<T>
where T: Decimal
{
    pub fn state_description(serialized_type: DataType) -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::DataType(serialized_type),
        ])
        .with_manual_drop(true)
    }
}

impl<T> AggregateDecimalArrayMovingState<T>
where T: Decimal + std::fmt::Debug + std::ops::AddAssign + std::ops::SubAssign
{
    fn add_default(&mut self) {
        self.values.push(T::default());
    }

    fn add(&mut self, value: T) {
        self.values.push(value);
    }

    fn append(&mut self, rhs: &mut Self) {
        self.values.append(&mut rhs.values);
    }

    fn check_overflow(value: T) -> Result<()> {
        if value > T::DECIMAL_MAX || value < T::DECIMAL_MIN {
            return Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {} not in [{}, {}]",
                value,
                T::DECIMAL_MIN,
                T::DECIMAL_MAX,
            )));
        }
        Ok(())
    }

    fn serialize(&self, builder: &mut ColumnBuilder) {
        let mut inner_builder = ColumnBuilder::with_capacity(
            &DataType::Decimal(T::default_decimal_size()),
            self.values.len(),
        );
        {
            let mut typed_builder = DecimalType::<T>::downcast_builder(&mut inner_builder);
            for value in &self.values {
                typed_builder.push_item(*value);
            }
        }
        builder.push(ScalarRef::Array(inner_builder.build()));
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let values = ArrayType::<DecimalType<T>>::try_downcast_scalar(&value)?;
        for value in DecimalType::<T>::iter_column(&values) {
            self.add(value);
        }
        Ok(())
    }

    fn merge_sum_result(&self, builder: &mut ColumnBuilder, window_size: usize) -> Result<()> {
        let mut sum = T::default();
        let mut values = Vec::with_capacity(self.values.len());
        for (index, value) in self.values.iter().enumerate() {
            sum += *value;
            Self::check_overflow(sum)?;
            if index >= window_size {
                sum -= self.values[index - window_size];
            }
            values.push(sum);
        }

        let data_type = builder.data_type();
        let inner_type = data_type.as_array().unwrap();
        let decimal_size = inner_type.as_decimal().unwrap();
        let column = T::upcast_column(values.into(), *decimal_size);
        builder.push(ScalarRef::Array(column));
        Ok(())
    }

    fn merge_avg_result(
        &self,
        builder: &mut ColumnBuilder,
        window_size: usize,
        scale_add: u8,
    ) -> Result<()> {
        let mut sum = T::default();
        let mut values = Vec::with_capacity(self.values.len());
        for (index, value) in self.values.iter().enumerate() {
            sum += *value;
            Self::check_overflow(sum)?;
            if index >= window_size {
                sum -= self.values[index - window_size];
            }
            let avg = match sum
                .checked_mul(T::e(scale_add))
                .and_then(|value| value.checked_div(T::from_i64(window_size as i64)))
            {
                Some(value) => value,
                None => {
                    return Err(ErrorCode::Overflow(format!(
                        "Decimal overflow: {} mul {}",
                        sum,
                        T::e(scale_add)
                    )));
                }
            };
            values.push(avg);
        }

        let data_type = builder.data_type();
        let inner_type = data_type.as_array().unwrap();
        let decimal_size = inner_type.as_decimal().unwrap();
        let column = T::upcast_column(values.into(), *decimal_size);
        builder.push(ScalarRef::Array(column));
        Ok(())
    }
}

struct AggregateArrayMovingImplementation<State> {
    info: ArrayMovingInfo,
    _p: PhantomData<fn(State)>,
}

impl<State> AggregateArrayMovingImplementation<State> {
    fn new(info: ArrayMovingInfo) -> Self {
        Self {
            info,
            _p: PhantomData,
        }
    }
}

impl<I, S> AggrImpl for AggregateArrayMovingImplementation<AggregateNumberArrayMovingState<I, S>>
where
    I: ValueType + AccessType + ArgType,
    S: ValueType + AccessType + ArgType,
    I::Scalar: Number + AsPrimitive<S::Scalar>,
    S::Scalar: Number + AsPrimitive<f64> + std::ops::AddAssign + std::ops::SubAssign,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateNumberArrayMovingState::<I, S>::default);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateNumberArrayMovingState<I, S>>();
        let entry = &input.columns[0];
        if entry.data_type().is_null() {
            for _ in 0..input.columns.num_rows() {
                state.add_default();
            }
            return Ok(());
        }

        let (not_null, nulls) = entry.clone().split_nullable();
        let values = not_null.downcast::<I>().unwrap();
        match nulls.and_bitmap(input.validity) {
            ColumnView::Const(false, _) => {
                for _ in 0..input.columns.num_rows() {
                    state.add_default();
                }
            }
            ColumnView::Const(true, _) => {
                for value in values.iter() {
                    state.add(value);
                }
            }
            ColumnView::Column(validity) => {
                for (value, valid) in values.iter().zip(validity.iter()) {
                    if valid {
                        state.add(value);
                    } else {
                        state.add_default();
                    }
                }
            }
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let entry = &input.columns[0];
        if entry.data_type().is_null() {
            for state in input.states.iter() {
                state
                    .get::<AggregateNumberArrayMovingState<I, S>>()
                    .add_default();
            }
            return Ok(());
        }

        let (not_null, nulls) = entry.clone().split_nullable();
        let values = not_null.downcast::<I>()?;
        match nulls {
            ColumnView::Const(false, _) => {
                for state in input.states.iter() {
                    state
                        .get::<AggregateNumberArrayMovingState<I, S>>()
                        .add_default();
                }
            }
            ColumnView::Const(true, _) => {
                for (row, state) in input.states.iter().enumerate() {
                    state
                        .get::<AggregateNumberArrayMovingState<I, S>>()
                        .add(values.index(row).unwrap());
                }
            }
            ColumnView::Column(validity) => {
                for (row, state) in input.states.iter().enumerate() {
                    let state = state.get::<AggregateNumberArrayMovingState<I, S>>();
                    if validity.get(row).unwrap() {
                        state.add(values.index(row).unwrap());
                    } else {
                        state.add_default();
                    }
                }
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateNumberArrayMovingState<I, S>>();
        let entry = &input.columns[0];
        if entry.data_type().is_null() {
            state.add_default();
            return Ok(());
        }

        let (not_null, nulls) = entry.clone().split_nullable();
        let values = not_null.downcast::<I>()?;
        match nulls {
            ColumnView::Const(false, _) => state.add_default(),
            ColumnView::Const(true, _) => state.add(values.index(input.row).unwrap()),
            ColumnView::Column(validity) => {
                if validity.get(input.row).unwrap() {
                    state.add(values.index(input.row).unwrap());
                } else {
                    state.add_default();
                }
            }
        }
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            state
                .get::<AggregateNumberArrayMovingState<I, S>>()
                .serialize(&mut input.builders[0]);
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            state
                .get::<AggregateNumberArrayMovingState<I, S>>()
                .merge_serialized(super::serialized_scalar_at(input.state, row, 0))?;
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        input
            .state
            .get::<AggregateNumberArrayMovingState<I, S>>()
            .append(input.rhs.get::<AggregateNumberArrayMovingState<I, S>>());
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateNumberArrayMovingState<I, S>>();
        let window_size = self.info.window_size.unwrap_or(state.values.len());
        match self.info.kind {
            ArrayMovingKind::Avg => state.merge_avg_result(input.builder, window_size),
            ArrayMovingKind::Sum => state.merge_sum_result(input.builder, window_size),
        }
        state.values.clear();
        Ok(())
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateNumberArrayMovingState<I, S>>();
        let window_size = self.info.window_size.unwrap_or(state.values.len());
        match self.info.kind {
            ArrayMovingKind::Avg => state.merge_avg_result(input.builder, window_size),
            ArrayMovingKind::Sum => state.merge_sum_result(input.builder, window_size),
        }
        Ok(())
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe {
            std::ptr::drop_in_place(state.get::<AggregateNumberArrayMovingState<I, S>>());
        }
    }
}

impl<T> AggrImpl for AggregateArrayMovingImplementation<AggregateDecimalArrayMovingState<T>>
where T: Decimal + std::fmt::Debug + std::ops::AddAssign + std::ops::SubAssign
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateDecimalArrayMovingState::<T>::default);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateDecimalArrayMovingState<T>>();
        let entry = &input.columns[0];
        if entry.data_type().is_null() {
            for _ in 0..input.columns.num_rows() {
                state.add_default();
            }
            return Ok(());
        }

        let (not_null, nulls) = entry.clone().split_nullable();
        let values = not_null.downcast::<DecimalType<T>>().unwrap();
        match nulls.and_bitmap(input.validity) {
            ColumnView::Const(false, _) => {
                for _ in 0..input.columns.num_rows() {
                    state.add_default();
                }
            }
            ColumnView::Const(true, _) => {
                for value in values.iter() {
                    state.add(value);
                }
            }
            ColumnView::Column(validity) => {
                for (value, valid) in values.iter().zip(validity.iter()) {
                    if valid {
                        state.add(value);
                    } else {
                        state.add_default();
                    }
                }
            }
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let entry = &input.columns[0];
        if entry.data_type().is_null() {
            for state in input.states.iter() {
                state
                    .get::<AggregateDecimalArrayMovingState<T>>()
                    .add_default();
            }
            return Ok(());
        }

        let (not_null, nulls) = entry.clone().split_nullable();
        let values = not_null.downcast::<DecimalType<T>>()?;
        match nulls {
            ColumnView::Const(false, _) => {
                for state in input.states.iter() {
                    state
                        .get::<AggregateDecimalArrayMovingState<T>>()
                        .add_default();
                }
            }
            ColumnView::Const(true, _) => {
                for (row, state) in input.states.iter().enumerate() {
                    state
                        .get::<AggregateDecimalArrayMovingState<T>>()
                        .add(values.index(row).unwrap());
                }
            }
            ColumnView::Column(validity) => {
                for (row, state) in input.states.iter().enumerate() {
                    let state = state.get::<AggregateDecimalArrayMovingState<T>>();
                    if validity.get(row).unwrap() {
                        state.add(values.index(row).unwrap());
                    } else {
                        state.add_default();
                    }
                }
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateDecimalArrayMovingState<T>>();
        let entry = &input.columns[0];
        if entry.data_type().is_null() {
            state.add_default();
            return Ok(());
        }

        let (not_null, nulls) = entry.clone().split_nullable();
        let values = not_null.downcast::<DecimalType<T>>()?;
        match nulls {
            ColumnView::Const(false, _) => state.add_default(),
            ColumnView::Const(true, _) => state.add(values.index(input.row).unwrap()),
            ColumnView::Column(validity) => {
                if validity.get(input.row).unwrap() {
                    state.add(values.index(input.row).unwrap());
                } else {
                    state.add_default();
                }
            }
        }
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            state
                .get::<AggregateDecimalArrayMovingState<T>>()
                .serialize(&mut input.builders[0]);
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            state
                .get::<AggregateDecimalArrayMovingState<T>>()
                .merge_serialized(super::serialized_scalar_at(input.state, row, 0))?;
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        input
            .state
            .get::<AggregateDecimalArrayMovingState<T>>()
            .append(input.rhs.get::<AggregateDecimalArrayMovingState<T>>());
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateDecimalArrayMovingState<T>>();
        let window_size = self.info.window_size.unwrap_or(state.values.len());
        match self.info.kind {
            ArrayMovingKind::Avg => {
                state.merge_avg_result(input.builder, window_size, self.info.scale_add)?
            }
            ArrayMovingKind::Sum => state.merge_sum_result(input.builder, window_size)?,
        }
        state.values.clear();
        Ok(())
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateDecimalArrayMovingState<T>>();
        let window_size = self.info.window_size.unwrap_or(state.values.len());
        match self.info.kind {
            ArrayMovingKind::Avg => {
                state.merge_avg_result(input.builder, window_size, self.info.scale_add)?
            }
            ArrayMovingKind::Sum => state.merge_sum_result(input.builder, window_size)?,
        }
        Ok(())
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe {
            std::ptr::drop_in_place(state.get::<AggregateDecimalArrayMovingState<T>>());
        }
    }
}

impl ArrayMovingBuilder {
    fn avg_route() -> DirectNameRoute {
        let arguments = Self::array_moving_arguments();
        let features = FunctionFeatures {
            is_decomposable: true,
            sort_policy: SortPolicy::Unsupported,
            distinct_policy: DistinctPolicy::Unsupported,
            category: "Aggregate",
            description: "returns moving average values as an array",
            definition: "group_array_moving_avg([window])(expr)",
            example: "select group_array_moving_avg(2)(number) from numbers(10)",
        };
        DirectNameRoute::new(
            &["group_array_moving_avg"],
            arguments.clone(),
            features,
            NullPolicy::Keep,
        )
        .then(MergeRoute::new(false, ArrayMovingBuilder::create_avg))
        .then(MergeRoute::new(true, ArrayMovingBuilder::create_avg))
        .then(PlainRoute::new(ArrayMovingBuilder::create_avg))
        .then(IfRoute::new(ArrayMovingBuilder::create_avg))
        .then(StateRoute::new(ArrayMovingBuilder::create_avg))
    }

    fn sum_route() -> DirectNameRoute {
        let arguments = Self::array_moving_arguments();
        let features = FunctionFeatures {
            is_decomposable: true,
            sort_policy: SortPolicy::Unsupported,
            distinct_policy: DistinctPolicy::Unsupported,
            category: "Aggregate",
            description: "returns moving sum values as an array",
            definition: "group_array_moving_sum([window])(expr)",
            example: "select group_array_moving_sum(2)(number) from numbers(10)",
        };
        DirectNameRoute::new(
            &["group_array_moving_sum"],
            arguments.clone(),
            features,
            NullPolicy::Keep,
        )
        .then(MergeRoute::new(false, ArrayMovingBuilder::create_sum))
        .then(MergeRoute::new(true, ArrayMovingBuilder::create_sum))
        .then(PlainRoute::new(ArrayMovingBuilder::create_sum))
        .then(IfRoute::new(ArrayMovingBuilder::create_sum))
        .then(StateRoute::new(ArrayMovingBuilder::create_sum))
    }

    fn create_avg(
        build: DirectBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        Self::create(build, ArrayMovingKind::Avg)
    }

    fn create_sum(
        build: DirectBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        Self::create(build, ArrayMovingKind::Sum)
    }

    fn create(
        build: DirectBuildContext<'_, impl CombinatorImpl>,
        kind: ArrayMovingKind,
    ) -> Result<AggregateFunctionRef> {
        if build.params().len() > 1 {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects at most one parameter",
                build.name()
            )));
        }
        let window_size = if let [param] = build.params() {
            Some(extract_number_param::<u64>(param.clone())? as usize)
        } else {
            None
        };

        let data_type = if build.args_type()[0].is_null() {
            NumberType::<i8>::data_type()
        } else {
            build.args_type()[0].remove_nullable()
        };

        with_number_mapped_type!(|NUM| match &data_type {
            DataType::Number(NumberDataType::NUM) => {
                type Sum = <NUM as ResultTypeOfUnary>::Sum;
                Self::create_number::<NumberType<NUM>, NumberType<Sum>>(build, kind, window_size)
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL =>
                        Self::create_decimal::<DECIMAL>(build, kind, *size, window_size,),
                })
            }
            _ => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                build.name(),
                build.args_type()[0]
            ))),
        })
    }

    fn create_number<I, S>(
        build: DirectBuildContext<'_, impl CombinatorImpl>,
        kind: ArrayMovingKind,
        window_size: Option<usize>,
    ) -> Result<AggregateFunctionRef>
    where
        I: ValueType + AccessType + ArgType,
        S: ValueType + AccessType + ArgType,
        I::Scalar: Number + AsPrimitive<S::Scalar>,
        S::Scalar: Number + AsPrimitive<f64> + std::ops::AddAssign + std::ops::SubAssign,
    {
        let return_type = match kind {
            ArrayMovingKind::Avg => DataType::Array(Box::new(Float64Type::data_type())),
            ArrayMovingKind::Sum => DataType::Array(Box::new(S::data_type())),
        };
        let serialized_type = DataType::Array(Box::new(I::data_type()));
        Self::create_instance::<AggregateNumberArrayMovingState<I, S>>(
            build,
            ArrayMovingInfo {
                window_size,
                return_type,
                scale_add: 0,
                kind,
            },
            serialized_type,
        )
    }

    fn create_decimal<T>(
        build: DirectBuildContext<'_, impl CombinatorImpl>,
        kind: ArrayMovingKind,
        input_size: DecimalSize,
        window_size: Option<usize>,
    ) -> Result<AggregateFunctionRef>
    where
        T: Decimal + std::fmt::Debug + std::ops::AddAssign + std::ops::SubAssign,
    {
        let return_size = match kind {
            ArrayMovingKind::Avg => {
                DecimalSize::new_unchecked(T::MAX_PRECISION, input_size.scale().max(4))
            }
            ArrayMovingKind::Sum => {
                DecimalSize::new_unchecked(T::MAX_PRECISION, input_size.scale())
            }
        };
        let scale_add = return_size.scale() - input_size.scale();
        Self::create_instance::<AggregateDecimalArrayMovingState<T>>(
            build,
            ArrayMovingInfo {
                window_size,
                return_type: DataType::Array(Box::new(DataType::Decimal(return_size))),
                scale_add,
                kind,
            },
            DataType::Array(Box::new(DataType::Decimal(T::default_decimal_size()))),
        )
    }

    fn create_instance<State>(
        build: DirectBuildContext<'_, impl CombinatorImpl>,
        info: ArrayMovingInfo,
        serialized_type: DataType,
    ) -> Result<AggregateFunctionRef>
    where
        AggregateArrayMovingImplementation<State>: AggrImpl,
        State: ArrayMovingStateDescription,
    {
        let return_type = info.return_type.clone();
        build.create(
            return_type.clone(),
            <State as ArrayMovingStateDescription>::state_description(serialized_type),
            AggregateArrayMovingImplementation::new(info),
        )
    }
}

trait ArrayMovingStateDescription {
    fn state_description(serialized_type: DataType) -> AggregateStateDescription;
}

impl<I, S> ArrayMovingStateDescription for AggregateNumberArrayMovingState<I, S>
where
    I: ValueType,
    S: ValueType,
{
    fn state_description(serialized_type: DataType) -> AggregateStateDescription {
        AggregateNumberArrayMovingState::<I, S>::state_description(serialized_type)
    }
}

impl<T> ArrayMovingStateDescription for AggregateDecimalArrayMovingState<T>
where T: Decimal
{
    fn state_description(serialized_type: DataType) -> AggregateStateDescription {
        AggregateDecimalArrayMovingState::<T>::state_description(serialized_type)
    }
}
