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
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalF64View;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::compute_view::NumberConvertView;
use databend_common_expression::types::i256;
use databend_common_expression::types::number::F64;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;

use super::AggregateFunctionV2Factory;
use super::adaptors_v2 as v2;
use super::adaptors_v2::UnaryState;
use super::adaptors_v2::*;

pub const STD_POP: u8 = 0;
pub const STD_SAMP: u8 = 1;
pub const VAR_POP: u8 = 2;
pub const VAR_SAMP: u8 = 3;

struct StddevBuilder;

impl StddevBuilder {
    fn register(registry: &mut v2::AggregateFunctionRegistry) {
        Self::route::<STD_POP>().register(registry);
        Self::route::<STD_SAMP>().register(registry);
    }
}

inventory::submit! {
    AggregateFunctionV2Factory {
        register: StddevBuilder::register,
    }
}

impl StddevBuilder {
    fn route<const TYPE: u8>() -> v2::DirectNameRoute {
        let (names, features) = match TYPE {
            STD_POP => (&["stddev_pop", "std"][..], Self::STDDEV_POP_FEATURES),
            STD_SAMP => (&["stddev_samp", "stddev"][..], Self::STDDEV_SAMP_FEATURES),
            _ => unreachable!(),
        };
        v2::DirectNameRoute::new(
            names,
            Self::stddev_arguments(),
            features,
            v2::NullPolicy::Skip,
        )
        .then(v2::MergeRoute::unary(false, Self::create_for_type::<TYPE>))
        .then(v2::MergeRoute::unary(true, Self::create_for_type::<TYPE>))
        .then(v2::PlainRoute::unary(Self::create_for_type::<TYPE>))
        .then(v2::IfRoute::unary(Self::create_for_type::<TYPE>))
        .then(v2::StateRoute::unary(Self::create_for_type::<TYPE>))
    }

    fn stddev_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::any_numeric()])
    }

    const STDDEV_POP_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "calculates population standard deviation",
        definition: "stddev_pop(expr)",
        example: "select stddev_pop(number) from numbers(10)",
    };

    const STDDEV_SAMP_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "calculates sample standard deviation",
        definition: "stddev_samp(expr)",
        example: "select stddev_samp(number) from numbers(10)",
    };
}

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub struct AggregateStddevState<const TYPE: u8> {
    count: u64,
    mean: f64,
    dsquared: f64,
}

impl<const TYPE: u8> AggregateStddevState<TYPE> {
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(Some(24)),
        ])
    }

    fn add_value(&mut self, value: f64) {
        self.count += 1;
        let mean_differential = (value - self.mean) / self.count as f64;
        let new_mean = self.mean + mean_differential;
        let dsquared_increment = (value - new_mean) * (value - self.mean);
        let new_dsquared = self.dsquared + dsquared_increment;

        self.mean = new_mean;
        self.dsquared = new_dsquared;
    }

    fn merge_state(&mut self, rhs: &Self) {
        if self.count == 0 {
            self.count = rhs.count;
            self.mean = rhs.mean;
            self.dsquared = rhs.dsquared;
            return;
        }

        if rhs.count > 0 {
            let count = self.count + rhs.count;
            let mean = (self.count as f64 * self.mean + rhs.count as f64 * rhs.mean) / count as f64;
            let delta = rhs.mean - self.mean;

            self.dsquared = rhs.dsquared
                + self.dsquared
                + delta * delta * rhs.count as f64 * self.count as f64 / count as f64;

            self.mean = mean;
            self.count = count;
        }
    }

    fn result(&self) -> Option<f64> {
        if self.count <= 1 && (TYPE == VAR_SAMP || TYPE == STD_SAMP) {
            return None;
        }

        Some(match TYPE {
            STD_POP => (self.dsquared / self.count as f64).sqrt(),
            STD_SAMP => (self.dsquared / (self.count - 1) as f64).sqrt(),
            VAR_POP => self.dsquared / self.count as f64,
            VAR_SAMP => self.dsquared / (self.count - 1) as f64,
            _ => unreachable!(),
        })
    }
}

impl<T, const TYPE: u8> UnaryState<T, NullableType<Float64Type>> for AggregateStddevState<TYPE>
where
    T: AccessType,
    T::Scalar: Into<f64>,
{
    type FunctionInfo = ();

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.add_value(T::to_owned_scalar(value).into());
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.merge_state(rhs);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: <NullableType<Float64Type> as ValueType>::ColumnBuilderMut<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        match self.result() {
            Some(value) => builder.push_item(Some(F64::from(value))),
            None => builder.push_item(None),
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

impl StddevBuilder {
    fn create_for_type<const TYPE: u8>(
        build: v2::UnaryBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        let display_name = build.name();
        let data_type = build.arg_type().clone();

        with_number_mapped_type!(|NUM| match &data_type {
            DataType::Number(NumberDataType::NUM) => {
                type Input = NumberConvertView<NUM, F64>;
                Self::create::<TYPE, Input>(build)
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => {
                        type Input = DecimalF64View<DECIMAL>;
                        Self::create::<TYPE, Input>(build)
                    }
                })
            }
            _ => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                display_name, data_type
            ))),
        })
    }

    fn create<const TYPE: u8, I>(
        build: v2::UnaryBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<AggregateFunctionRef>
    where
        AggregateStddevState<TYPE>: UnaryState<I, NullableType<Float64Type>, FunctionInfo = ()>,
        I: AccessType,
    {
        build.create_unary::<AggregateStddevState<TYPE>, I, NullableType<Float64Type>>(
            Float64Type::data_type().wrap_nullable(),
            AggregateStddevState::<TYPE>::state_description(),
            (),
        )
    }
}
