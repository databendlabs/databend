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
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::compute_view::NumberConvertView;
use databend_common_expression::types::i256;
use databend_common_expression::types::number::F64;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use num_traits::AsPrimitive;

use super::FunctionFactory;
use super::adaptors::*;

struct MomentsBuilder;

impl MomentsBuilder {
    fn register(registry: &mut AggregateFunctionRegistry) {
        DirectNameRoute::new(
            &["skewness"],
            MomentsBuilder::moments_arguments(),
            MomentsBuilder::SKEWNESS_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::unary(false, Self::create_skewness))
        .then(MergeRoute::unary(true, Self::create_skewness))
        .then(PlainRoute::unary(Self::create_skewness))
        .then(IfRoute::unary(Self::create_skewness))
        .then(StateRoute::unary(Self::create_skewness))
        .register(registry);
        DirectNameRoute::new(
            &["kurtosis"],
            MomentsBuilder::moments_arguments(),
            MomentsBuilder::KURTOSIS_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::unary(false, Self::create_kurtosis))
        .then(MergeRoute::unary(true, Self::create_kurtosis))
        .then(PlainRoute::unary(Self::create_kurtosis))
        .then(IfRoute::unary(Self::create_kurtosis))
        .then(StateRoute::unary(Self::create_kurtosis))
        .register(registry);
    }
}

inventory::submit! {
    FunctionFactory {
        register: MomentsBuilder::register,
    }
}

impl MomentsBuilder {
    fn moments_arguments() -> ArgumentsPattern {
        ArgumentsPattern::fixed(vec![ArgumentPattern::any_numeric()])
    }

    const SKEWNESS_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "calculates skewness",
        definition: "skewness(expr)",
        example: "select skewness(number) from numbers(10)",
    };

    const KURTOSIS_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "calculates kurtosis",
        definition: "kurtosis(expr)",
        example: "select kurtosis(number) from numbers(10)",
    };
}

#[derive(Default, BorshSerialize, BorshDeserialize)]
pub struct AggregateSkewnessState {
    n: u64,
    sum: F64,
    sum_sqr: F64,
    sum_cub: F64,
}

impl AggregateSkewnessState {
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(Some(32)),
        ])
    }

    fn add_value(&mut self, value: f64) {
        self.n += 1;
        self.sum += value;
        self.sum_sqr += value.powi(2);
        self.sum_cub += value.powi(3);
    }

    fn merge_state(&mut self, rhs: &Self) {
        if rhs.n == 0 {
            return;
        }
        self.n += rhs.n;
        self.sum += rhs.sum;
        self.sum_sqr += rhs.sum_sqr;
        self.sum_cub += rhs.sum_cub;
    }

    fn result_value(&self) -> F64 {
        if self.n <= 2 {
            return F64::from(0_f64);
        }

        let (n, sum, sum_sqr, sum_cub) = (self.n as f64, *self.sum, *self.sum_sqr, *self.sum_cub);
        let temp = 1.0 / n;
        let div = (temp * (sum_sqr - sum * sum * temp)).powi(3).sqrt();
        if div == 0.0 {
            return F64::from(0_f64);
        }
        let temp1 = (n * (n - 1.0)).sqrt() / (n - 2.0);
        let value =
            temp1 * temp * (sum_cub - 3.0 * sum_sqr * sum * temp + 2.0 * sum.powi(3) * temp * temp)
                / div;

        F64::from(if value.is_finite() { value } else { f64::NAN })
    }
}

#[derive(Default, BorshSerialize, BorshDeserialize)]
pub struct AggregateKurtosisState {
    n: u64,
    sum: F64,
    sum_sqr: F64,
    sum_cub: F64,
    sum_four: F64,
}

impl AggregateKurtosisState {
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(Some(40)),
        ])
    }

    fn add_value(&mut self, value: f64) {
        self.n += 1;
        self.sum += value;
        self.sum_sqr += value.powi(2);
        self.sum_cub += value.powi(3);
        self.sum_four += value.powi(4);
    }

    fn merge_state(&mut self, rhs: &Self) {
        if rhs.n == 0 {
            return;
        }
        self.n += rhs.n;
        self.sum += rhs.sum;
        self.sum_sqr += rhs.sum_sqr;
        self.sum_cub += rhs.sum_cub;
        self.sum_four += rhs.sum_four;
    }

    fn result_value(&self) -> F64 {
        if self.n <= 3 {
            return F64::from(0_f64);
        }

        let (n, sum, sum_sqr, sum_cub, sum_four) = (
            self.n as f64,
            *self.sum,
            *self.sum_sqr,
            *self.sum_cub,
            *self.sum_four,
        );

        let temp = 1.0 / n;
        if sum_sqr - sum * sum * temp == 0.0 {
            return F64::from(0_f64);
        }
        let m4 = temp
            * (sum_four - 4.0 * sum_cub * sum * temp + 6.0 * sum_sqr * sum * sum * temp * temp
                - 3.0 * sum.powi(4) * temp.powi(3));
        let m2 = temp * (sum_sqr - sum * sum * temp);
        if m2 <= 0.0 || (n - 2.0) * (n - 3.0) == 0.0 {
            return F64::from(0_f64);
        }
        let value =
            (n - 1.0) * ((n + 1.0) * m4 / (m2 * m2) - 3.0 * (n - 1.0)) / ((n - 2.0) * (n - 3.0));

        F64::from(if value.is_finite() { value } else { f64::NAN })
    }
}

trait MomentState: Send + Default + BorshSerialize + BorshDeserialize + 'static {
    fn state_description() -> AggregateStateDescription;
}

impl MomentState for AggregateSkewnessState {
    fn state_description() -> AggregateStateDescription {
        AggregateSkewnessState::state_description()
    }
}

impl MomentState for AggregateKurtosisState {
    fn state_description() -> AggregateStateDescription {
        AggregateKurtosisState::state_description()
    }
}

macro_rules! impl_moment_unary_state {
    ($state:ty) => {
        impl<T> UnaryState<T, Float64Type> for $state
        where
            T: AccessType,
            T::Scalar: AsPrimitive<f64>,
        {
            type FunctionInfo = ();

            fn init(_function_info: &Self::FunctionInfo) -> Self {
                Self::default()
            }

            fn add(
                &mut self,
                value: T::ScalarRef<'_>,
                _function_info: &Self::FunctionInfo,
            ) -> Result<()> {
                self.add_value(T::to_owned_scalar(value).as_());
                Ok(())
            }

            fn merge(&mut self, rhs: &Self) -> Result<()> {
                self.merge_state(rhs);
                Ok(())
            }

            fn merge_result(
                &mut self,
                mut builder: <Float64Type as ValueType>::ColumnBuilderMut<'_>,
                _function_info: &Self::FunctionInfo,
            ) -> Result<()> {
                builder.push_item(self.result_value());
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
    };
}

impl_moment_unary_state!(AggregateSkewnessState);
impl_moment_unary_state!(AggregateKurtosisState);

macro_rules! create_moment_function {
    ($state:ty, $build:expr) => {{
        let build = $build;
        let display_name = build.name().to_string();
        let data_type = build.arg_type().clone();

        with_number_mapped_type!(|NUM| match &data_type {
            DataType::Number(NumberDataType::NUM) => {
                type Input = NumberConvertView<NUM, F64>;
                MomentsBuilder::create_instance::<$state, Input>(build)
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => {
                        type Input = DecimalF64View<DECIMAL>;
                        MomentsBuilder::create_instance::<$state, Input>(build)
                    }
                })
            }
            _ => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                display_name, data_type
            ))),
        })
    }};
}

impl MomentsBuilder {
    fn create_skewness(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        create_moment_function!(AggregateSkewnessState, build)
    }

    fn create_kurtosis(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        create_moment_function!(AggregateKurtosisState, build)
    }

    fn create_instance<S, I>(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef>
    where
        S: MomentState + UnaryState<I, Float64Type, FunctionInfo = ()>,
        I: AccessType,
    {
        build.create_unary_or_null::<S, I, Float64Type>(
            Float64Type::data_type().wrap_nullable(),
            S::state_description(),
            (),
        )
    }
}
