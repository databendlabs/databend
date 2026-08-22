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

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::array::ArrayColumnBuilderMut;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::i256;
use databend_common_expression::types::number::F64;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use num_traits::AsPrimitive;

use super::super::common::get_levels;
use super::FunctionFactory;
use super::adaptors::*;

struct QuantileContBuilder;

impl QuantileContBuilder {
    fn register(registry: &mut AggregateFunctionRegistry) {
        DirectNameRoute::new(
            &["quantile_cont"],
            QuantileContBuilder::quantile_cont_arguments(),
            QuantileContBuilder::QUANTILE_CONT_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::unary(false, Self::create))
        .then(MergeRoute::unary(true, Self::create))
        .then(PlainRoute::unary(Self::create))
        .then(IfRoute::unary(Self::create))
        .then(StateRoute::unary(Self::create))
        .register(registry);
        DirectNameRoute::new(
            &["median"],
            QuantileContBuilder::quantile_cont_arguments(),
            QuantileContBuilder::MEDIAN_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::unary(false, Self::create_median))
        .then(MergeRoute::unary(true, Self::create_median))
        .then(PlainRoute::unary(Self::create_median))
        .then(IfRoute::unary(Self::create_median))
        .then(StateRoute::unary(Self::create_median))
        .register(registry);
    }
}

inventory::submit! {
    FunctionFactory {
        register: QuantileContBuilder::register,
    }
}

impl QuantileContBuilder {
    fn create_median(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        if !build.params().is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                build.name()
            )));
        }
        Self::create(build)
    }

    fn quantile_cont_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::any_numeric()])
    }

    const QUANTILE_CONT_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns a continuous quantile value",
        definition: "quantile_cont(level)(expr)",
        example: "select quantile_cont(0.5)(number) from numbers(10)",
    };

    const MEDIAN_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the median input value",
        definition: "median(expr)",
        example: "select median(number) from numbers(10)",
    };
}

pub struct QuantileContData {
    levels: Vec<f64>,
}

#[derive(Default)]
pub struct AggregateNumberQuantileContState {
    value: Vec<F64>,
}

impl AggregateNumberQuantileContState {
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(None),
        ])
        .with_manual_drop(true)
    }

    fn compute_result(&mut self, whole: usize, frac: f64, value_len: usize) -> f64 {
        self.value.as_mut_slice().select_nth_unstable(whole);
        let value = self.value.get(whole).unwrap().0;
        let value1 = if whole + 1 >= value_len {
            value
        } else {
            self.value.as_mut_slice().select_nth_unstable(whole + 1);
            self.value.get(whole + 1).unwrap().0
        };

        value + (value1 - value) * frac
    }

    fn merge_state(&mut self, rhs: &Self) {
        self.value.extend(rhs.value.iter());
    }

    fn merge_owned_state(&mut self, rhs: &mut Self) {
        self.value.append(&mut rhs.value);
    }
}

impl BorshSerialize for AggregateNumberQuantileContState {
    fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> borsh::io::Result<()> {
        let values = self.value.iter().map(|value| value.0).collect::<Vec<_>>();
        BorshSerialize::serialize(&values, writer)
    }
}

impl BorshDeserialize for AggregateNumberQuantileContState {
    fn deserialize_reader<R: borsh::io::Read>(reader: &mut R) -> borsh::io::Result<Self> {
        let values = Vec::<f64>::deserialize_reader(reader)?;
        Ok(Self {
            value: values.into_iter().map(Into::into).collect(),
        })
    }
}

impl<T> UnaryState<T, ArrayType<Float64Type>> for AggregateNumberQuantileContState
where
    T: ValueType,
    T::Scalar: Number + AsPrimitive<f64>,
{
    type FunctionInfo = QuantileContData;

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.value.push(T::to_owned_scalar(value).as_().into());
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.merge_state(rhs);
        Ok(())
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge_owned_state(rhs);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: ArrayColumnBuilderMut<'_, Float64Type>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let value_len = self.value.len();
        for level in &function_info.levels {
            let (frac, whole) = libm::modf((value_len - 1) as f64 * (*level));
            let whole = whole as usize;
            if whole >= value_len {
                builder.push_default();
            } else {
                let value = self.compute_result(whole, frac, value_len);
                builder.put_item(value.into());
            }
        }
        builder.commit_row();
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let binary_builder = builder.as_binary_mut().unwrap();
        BorshSerialize::serialize(self, &mut binary_builder.data)?;
        binary_builder.commit_row();
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let ScalarRef::Binary(mut data) = value else {
            unreachable!()
        };
        let rhs = Self::deserialize_reader(&mut data)?;
        self.merge_state(&rhs);
        Ok(())
    }
}

impl<T> UnaryState<T, Float64Type> for AggregateNumberQuantileContState
where
    T: ValueType,
    T::Scalar: Number + AsPrimitive<f64>,
{
    type FunctionInfo = QuantileContData;

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.value.push(T::to_owned_scalar(value).as_().into());
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.merge_state(rhs);
        Ok(())
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge_owned_state(rhs);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: <Float64Type as ValueType>::ColumnBuilderMut<'_>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let value_len = self.value.len();
        let (frac, whole) = libm::modf((value_len - 1) as f64 * function_info.levels[0]);
        let whole = whole as usize;
        if whole >= value_len {
            builder.push_default();
        } else {
            let value = self.compute_result(whole, frac, value_len);
            builder.push_item(value.into());
        }
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let binary_builder = builder.as_binary_mut().unwrap();
        BorshSerialize::serialize(self, &mut binary_builder.data)?;
        binary_builder.commit_row();
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let ScalarRef::Binary(mut data) = value else {
            unreachable!()
        };
        let rhs = Self::deserialize_reader(&mut data)?;
        self.merge_state(&rhs);
        Ok(())
    }
}

pub struct AggregateDecimalQuantileContState<T: ValueType> {
    value: Vec<T::Scalar>,
}

impl<T> Default for AggregateDecimalQuantileContState<T>
where T: ValueType
{
    fn default() -> Self {
        Self { value: vec![] }
    }
}

impl<T> AggregateDecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: Decimal,
{
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(None),
        ])
        .with_manual_drop(true)
    }

    fn compute_result(&mut self, whole: usize, frac: f64, value_len: usize) -> Result<T::Scalar> {
        self.value.as_mut_slice().select_nth_unstable(whole);
        let value = *self.value.get(whole).unwrap();
        let value1 = if whole + 1 >= value_len {
            value
        } else {
            self.value.as_mut_slice().select_nth_unstable(whole + 1);
            *self.value.get(whole + 1).unwrap()
        };

        let result = value1
            .checked_sub(value)
            .and_then(|sub_result| sub_result.checked_mul(Decimal::from_float(frac)))
            .and_then(|mul_result| value.checked_add(mul_result));

        result.ok_or_else(|| ErrorCode::Overflow("Decimal overflow when interpolate"))
    }

    fn merge_state(&mut self, rhs: &Self) {
        self.value.extend(
            rhs.value
                .iter()
                .map(|value| T::to_owned_scalar(T::to_scalar_ref(value))),
        );
    }

    fn merge_owned_state(&mut self, rhs: &mut Self) {
        self.value.append(&mut rhs.value);
    }
}

impl<T> BorshSerialize for AggregateDecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize,
{
    fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> borsh::io::Result<()> {
        BorshSerialize::serialize(&self.value, writer)
    }
}

impl<T> BorshDeserialize for AggregateDecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: BorshDeserialize,
{
    fn deserialize_reader<R: borsh::io::Read>(reader: &mut R) -> borsh::io::Result<Self> {
        Ok(Self {
            value: Vec::<T::Scalar>::deserialize_reader(reader)?,
        })
    }
}

impl<T> UnaryState<T, ArrayType<T>> for AggregateDecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Decimal,
{
    type FunctionInfo = QuantileContData;

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.value.push(T::to_owned_scalar(value));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.merge_state(rhs);
        Ok(())
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge_owned_state(rhs);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: ArrayColumnBuilderMut<'_, T>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let value_len = self.value.len();
        for level in &function_info.levels {
            let (frac, whole) = libm::modf((value_len - 1) as f64 * (*level));
            let whole = whole as usize;
            if whole >= value_len {
                builder.push_default();
            } else {
                let value = self.compute_result(whole, frac, value_len)?;
                builder.put_item(T::to_scalar_ref(&value));
            }
        }
        builder.commit_row();
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let binary_builder = builder.as_binary_mut().unwrap();
        BorshSerialize::serialize(self, &mut binary_builder.data)?;
        binary_builder.commit_row();
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let ScalarRef::Binary(mut data) = value else {
            unreachable!()
        };
        let rhs = Self::deserialize_reader(&mut data)?;
        self.merge_state(&rhs);
        Ok(())
    }
}

impl<T> UnaryState<T, T> for AggregateDecimalQuantileContState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Decimal,
{
    type FunctionInfo = QuantileContData;

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.value.push(T::to_owned_scalar(value));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.merge_state(rhs);
        Ok(())
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge_owned_state(rhs);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: T::ColumnBuilderMut<'_>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let value_len = self.value.len();
        let (frac, whole) = libm::modf((value_len - 1) as f64 * function_info.levels[0]);
        let whole = whole as usize;
        if whole >= value_len {
            builder.push_default();
        } else {
            let value = self.compute_result(whole, frac, value_len)?;
            builder.push_item(T::to_scalar_ref(&value));
        }
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let binary_builder = builder.as_binary_mut().unwrap();
        BorshSerialize::serialize(self, &mut binary_builder.data)?;
        binary_builder.commit_row();
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let ScalarRef::Binary(mut data) = value else {
            unreachable!()
        };
        let rhs = Self::deserialize_reader(&mut data)?;
        self.merge_state(&rhs);
        Ok(())
    }
}

impl QuantileContBuilder {
    fn create(build: UnaryBuildContext<'_, impl CombinatorImpl>) -> Result<AggregateFunctionRef> {
        let data_type = build.arg_type().clone();
        let display_name = build.name().to_string();
        let levels = get_levels(build.params())?;

        with_number_mapped_type!(|NUM| match &data_type {
            DataType::Number(NumberDataType::NUM) => {
                if levels.len() > 1 {
                    Self::create_number_typed::<NumberType<NUM>, ArrayType<Float64Type>>(
                        build,
                        DataType::Array(Box::new(Float64Type::data_type())),
                        levels,
                    )
                } else {
                    Self::create_number_typed::<NumberType<NUM>, Float64Type>(
                        build,
                        Float64Type::data_type(),
                        levels,
                    )
                }
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => {
                        let data_type = DataType::Decimal(*size);
                        if levels.len() > 1 {
                            Self::create_decimal_typed::<
                                DecimalType<DECIMAL>,
                                ArrayType<DecimalType<DECIMAL>>,
                            >(
                                build, DataType::Array(Box::new(data_type)), levels
                            )
                        } else {
                            Self::create_decimal_typed::<DecimalType<DECIMAL>, DecimalType<DECIMAL>>(
                                build, data_type, levels,
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

    fn create_number_typed<I, R>(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
        return_type: DataType,
        levels: Vec<f64>,
    ) -> Result<AggregateFunctionRef>
    where
        I: AccessType + ValueType,
        I::Scalar: Number + AsPrimitive<f64>,
        R: ValueType,
        AggregateNumberQuantileContState: UnaryState<I, R, FunctionInfo = QuantileContData>,
    {
        Self::create_typed::<I, R, AggregateNumberQuantileContState>(
            build,
            AggregateNumberQuantileContState::state_description(),
            return_type,
            levels,
        )
    }

    fn create_decimal_typed<I, R>(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
        return_type: DataType,
        levels: Vec<f64>,
    ) -> Result<AggregateFunctionRef>
    where
        I: AccessType + ValueType,
        I::Scalar: BorshSerialize + BorshDeserialize + Decimal,
        R: ValueType,
        AggregateDecimalQuantileContState<I>: UnaryState<I, R, FunctionInfo = QuantileContData>,
    {
        Self::create_typed::<I, R, AggregateDecimalQuantileContState<I>>(
            build,
            AggregateDecimalQuantileContState::<I>::state_description(),
            return_type,
            levels,
        )
    }

    fn create_typed<I, R, S>(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
        state: AggregateStateDescription,
        return_type: DataType,
        levels: Vec<f64>,
    ) -> Result<AggregateFunctionRef>
    where
        I: AccessType + ValueType,
        R: ValueType,
        S: UnaryState<I, R, FunctionInfo = QuantileContData>,
    {
        build.create_unary_or_null::<S, I, R>(
            return_type.wrap_nullable(),
            state,
            QuantileContData { levels },
        )
    }
}
