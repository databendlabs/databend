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

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnView;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::compute_view::NumberConvertView;
use databend_common_expression::types::number::F64;
use databend_common_expression::with_number_mapped_type;
use num_traits::AsPrimitive;

use super::AggregateFunctionDefinition;
use super::AggregateFunctionV2Factory;
use super::adaptors_v2 as v2;
use super::adaptors_v2::*;
use super::serialized_scalar_at;

pub const COVAR_POP: u8 = 0;
pub const COVAR_SAMP: u8 = 1;

struct CovarianceBuilder;

impl CovarianceBuilder {
    fn register(registry: &mut v2::AggregateFunctionRegistry) {
        Self::definition::<COVAR_POP>().register_with_combinators(registry, false);
        Self::definition::<COVAR_SAMP>().register_with_combinators(registry, false);
    }
}

inventory::submit! {
    AggregateFunctionV2Factory {
        register: CovarianceBuilder::register,
    }
}

impl CovarianceBuilder {
    fn definition<const TYPE: u8>() -> AggregateFunctionDefinition {
        match TYPE {
            COVAR_POP => AggregateFunctionDefinition::new(
                "covar_pop",
                Self::covariance_arguments(),
                Self::COVAR_POP_FEATURES,
                Self::try_create::<TYPE>,
            )
            .with_aliases(&["var_pop", "variance_pop"]),
            COVAR_SAMP => AggregateFunctionDefinition::new(
                "covar_samp",
                Self::covariance_arguments(),
                Self::COVAR_SAMP_FEATURES,
                Self::try_create::<TYPE>,
            )
            .with_aliases(&["var_samp", "variance_samp"]),
            _ => unreachable!(),
        }
    }

    fn covariance_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![
            AggregateArgumentPattern::any_number(),
            AggregateArgumentPattern::any_number(),
        ])
    }

    const COVAR_POP_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "calculates population covariance",
        definition: "covar_pop(left, right)",
        example: "select covar_pop(a, b) from t",
    };

    const COVAR_SAMP_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "calculates sample covariance",
        definition: "covar_samp(left, right)",
        example: "select covar_samp(a, b) from t",
    };
}

#[derive(Default, BorshSerialize, BorshDeserialize)]
pub struct AggregateCovarianceState<const TYPE: u8> {
    count: u64,
    co_moments: f64,
    left_mean: f64,
    right_mean: f64,
}

// Source: "Numerically Stable, Single-Pass, Parallel Statistics Algorithms"
// (J. Bennett et al., Sandia National Laboratories,
// 2009 IEEE International Conference on Cluster Computing)
impl<const TYPE: u8> AggregateCovarianceState<TYPE> {
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(None),
        ])
    }

    fn add_value(&mut self, left: f64, right: f64) {
        let left_delta = left - self.left_mean;
        let right_delta = right - self.right_mean;

        self.count += 1;
        let new_left_mean = self.left_mean + left_delta / self.count as f64;
        let new_right_mean = self.right_mean + right_delta / self.count as f64;

        self.co_moments += (left - new_left_mean) * (right - self.right_mean);
        self.left_mean = new_left_mean;
        self.right_mean = new_right_mean;
    }

    fn merge_state(&mut self, rhs: &Self) {
        let total = self.count + rhs.count;
        if total == 0 {
            return;
        }

        let factor = self.count as f64 * rhs.count as f64 / total as f64;
        let left_delta = self.left_mean - rhs.left_mean;
        let right_delta = self.right_mean - rhs.right_mean;

        self.co_moments += rhs.co_moments + left_delta * right_delta * factor;

        if large_and_comparable(self.count, rhs.count) {
            self.left_mean = (self.left_sum() + rhs.left_sum()) / total as f64;
            self.right_mean = (self.right_sum() + rhs.right_sum()) / total as f64;
        } else {
            self.left_mean = rhs.left_mean + left_delta * self.count as f64 / total as f64;
            self.right_mean = rhs.right_mean + right_delta * self.count as f64 / total as f64;
        }

        self.count = total;
    }

    fn left_sum(&self) -> f64 {
        self.count as f64 * self.left_mean
    }

    fn right_sum(&self) -> f64 {
        self.count as f64 * self.right_mean
    }

    fn result_value(&self) -> f64 {
        match TYPE {
            COVAR_POP => {
                if self.count == 0 {
                    f64::INFINITY
                } else if self.count == 1 {
                    0.0
                } else {
                    self.co_moments / self.count as f64
                }
            }
            COVAR_SAMP => {
                if self.count < 2 {
                    f64::INFINITY
                } else {
                    self.co_moments / (self.count - 1) as f64
                }
            }
            _ => unreachable!(),
        }
    }
}

fn large_and_comparable(a: u64, b: u64) -> bool {
    if a == 0 || b == 0 {
        return false;
    }

    let sensitivity = 0.001_f64;
    let threshold = 10000_f64;

    let min = a.min(b) as f64;
    let max = a.max(b) as f64;
    (1.0 - min / max) < sensitivity && min > threshold
}

impl CovarianceBuilder {
    fn try_create<const TYPE: u8>(
        request: AggregateFunctionRequest<'_>,
    ) -> Result<AggregateFunctionRef> {
        Self::definition::<TYPE>().build_with_multi_arg_input(
            request,
            false,
            multi_arg_aggregate_function_build_input_fns!(Self::create::<TYPE>),
        )
    }

    fn create<const TYPE: u8>(
        build: v2::MultiArgBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        let left_type = build.args_type()[0].clone();
        let right_type = build.args_type()[1].clone();
        let display_name = build.name().to_string();

        with_number_mapped_type!(|LEFT| match &left_type {
            DataType::Number(NumberDataType::LEFT) => {
                with_number_mapped_type!(|RIGHT| match &right_type {
                    DataType::Number(NumberDataType::RIGHT) => {
                        type Left = NumberConvertView<LEFT, F64>;
                        type Right = NumberConvertView<RIGHT, F64>;
                        Self::create_instance::<TYPE, Left, Right>(build)
                    }
                    _ => Err(ErrorCode::BadDataValueType(format!(
                        "{} does not support type '{:?}'",
                        display_name,
                        [left_type.clone(), right_type.clone()]
                    ))),
                })
            }
            _ => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                display_name,
                [left_type.clone(), right_type.clone()]
            ))),
        })
    }

    fn create_instance<const TYPE: u8, L, R>(
        build: v2::MultiArgBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<AggregateFunctionRef>
    where
        L: AccessType,
        L::Scalar: AsPrimitive<f64>,
        R: AccessType,
        R::Scalar: AsPrimitive<f64>,
    {
        let implementation = AggregateCovarianceImplementation::<TYPE, L, R>::new();
        let return_type = Float64Type::data_type();
        let state = AggregateCovarianceState::<TYPE>::state_description();

        build.create_multi_arg_or_null(return_type.wrap_nullable(), state, implementation)
    }
}

struct AggregateCovarianceImplementation<const TYPE: u8, L, R>
where
    L: AccessType,
    R: AccessType,
{
    _p: PhantomData<fn(L, R)>,
}

impl<const TYPE: u8, L, R> AggregateCovarianceImplementation<TYPE, L, R>
where
    L: AccessType,
    R: AccessType,
{
    fn new() -> Self {
        Self { _p: PhantomData }
    }
}

impl<const TYPE: u8, L, R> AggrImpl for AggregateCovarianceImplementation<TYPE, L, R>
where
    L: AccessType,
    L::Scalar: AsPrimitive<f64>,
    R: AccessType,
    R::Scalar: AsPrimitive<f64>,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateCovarianceState::<TYPE>::default);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let left = input.columns[0].downcast::<L>().unwrap();
        let right = input.columns[1].downcast::<R>().unwrap();
        let state = input.state.get::<AggregateCovarianceState<TYPE>>();
        add_batch::<TYPE, L, R>(state, left, right, input.validity);
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let left = input.columns[0].downcast::<L>().unwrap();
        let right = input.columns[1].downcast::<R>().unwrap();
        for (row, state) in input.states.iter().enumerate() {
            let left = unsafe { left.index_unchecked(row) };
            let right = unsafe { right.index_unchecked(row) };
            state.get::<AggregateCovarianceState<TYPE>>().add_value(
                L::to_owned_scalar(left).as_(),
                R::to_owned_scalar(right).as_(),
            );
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let left = input.columns[0].downcast::<L>().unwrap();
        let right = input.columns[1].downcast::<R>().unwrap();
        let left = unsafe { left.index_unchecked(input.row) };
        let right = unsafe { right.index_unchecked(input.row) };
        input
            .state
            .get::<AggregateCovarianceState<TYPE>>()
            .add_value(
                L::to_owned_scalar(left).as_(),
                R::to_owned_scalar(right).as_(),
            );
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        let binary_builder = input.builders[0].as_binary_mut().unwrap();
        for state in input.states.iter() {
            let state = state.get::<AggregateCovarianceState<TYPE>>();
            BorshSerialize::serialize(state, &mut binary_builder.data)?;
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            let ScalarRef::Binary(mut data) = serialized_scalar_at(input.state, row, 0) else {
                unreachable!()
            };
            let rhs = AggregateCovarianceState::<TYPE>::deserialize_reader(&mut data)?;
            state
                .get::<AggregateCovarianceState<TYPE>>()
                .merge_state(&rhs);
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateCovarianceState<TYPE>>();
        let rhs = input.rhs.get::<AggregateCovarianceState<TYPE>>();
        state.merge_state(rhs);
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateCovarianceState<TYPE>>();
        let mut builder = Float64Type::downcast_builder(input.builder);
        builder.push_item(F64::from(state.result_value()));
        Ok(())
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        let state = state.get::<AggregateCovarianceState<TYPE>>();
        unsafe { std::ptr::drop_in_place(state) };
    }
}

fn add_batch<const TYPE: u8, L, R>(
    state: &mut AggregateCovarianceState<TYPE>,
    left: ColumnView<L>,
    right: ColumnView<R>,
    validity: Option<&Bitmap>,
) where
    L: AccessType,
    L::Scalar: AsPrimitive<f64>,
    R: AccessType,
    R::Scalar: AsPrimitive<f64>,
{
    match validity {
        Some(validity) => {
            for ((left, right), valid) in left.iter().zip(right.iter()).zip(validity.iter()) {
                if valid {
                    state.add_value(
                        L::to_owned_scalar(left).as_(),
                        R::to_owned_scalar(right).as_(),
                    );
                }
            }
        }
        None => {
            for (left, right) in left.iter().zip(right.iter()) {
                state.add_value(
                    L::to_owned_scalar(left).as_(),
                    R::to_owned_scalar(right).as_(),
                );
            }
        }
    }
}
