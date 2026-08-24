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

use std::marker::PhantomData;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ValueType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::with_unsigned_integer_mapped_type;
use num_traits::AsPrimitive;

use super::super::common::get_levels;
use super::FunctionFactory;
use super::adaptors::*;
use super::quantile_tdigest::AggregateQuantileTDigestState;
use super::serialized_scalar_at;

struct QuantileTDigestWeightedBuilder;

impl QuantileTDigestWeightedBuilder {
    fn register(registry: &mut AggregateFunctionRegistry) {
        DirectNameRoute::new(
            &["quantile_tdigest_weighted"],
            QuantileTDigestWeightedBuilder::quantile_tdigest_weighted_arguments(),
            QuantileTDigestWeightedBuilder::QUANTILE_TDIGEST_WEIGHTED_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::multi_arg(false, Self::create))
        .then(MergeRoute::multi_arg(true, Self::create))
        .then(PlainRoute::multi_arg(Self::create))
        .then(IfRoute::multi_arg(Self::create))
        .then(StateRoute::multi_arg(Self::create))
        .register(registry);
        DirectNameRoute::new(
            &["median_tdigest_weighted"],
            QuantileTDigestWeightedBuilder::quantile_tdigest_weighted_arguments(),
            QuantileTDigestWeightedBuilder::MEDIAN_TDIGEST_WEIGHTED_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::multi_arg(false, Self::create_median))
        .then(MergeRoute::multi_arg(true, Self::create_median))
        .then(PlainRoute::multi_arg(Self::create_median))
        .then(IfRoute::multi_arg(Self::create_median))
        .then(StateRoute::multi_arg(Self::create_median))
        .register(registry);
    }
}

inventory::submit! {
    FunctionFactory {
        register: QuantileTDigestWeightedBuilder::register,
    }
}

impl QuantileTDigestWeightedBuilder {
    fn create_median(
        build: MultiArgBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        if !build.params().is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                build.name()
            )));
        }
        Self::create(build)
    }

    fn quantile_tdigest_weighted_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![
            AggregateArgumentPattern::any_number(),
            AggregateArgumentPattern::any_number(),
        ])
    }

    const QUANTILE_TDIGEST_WEIGHTED_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: false,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns an approximate weighted quantile value using t-digest",
        definition: "quantile_tdigest_weighted(level)(expr, weight)",
        example: "select quantile_tdigest_weighted(0.5)(number, weight) from t",
    };

    const MEDIAN_TDIGEST_WEIGHTED_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: false,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the approximate weighted median input value using t-digest",
        definition: "median_tdigest_weighted(expr, weight)",
        example: "select median_tdigest_weighted(number, weight) from t",
    };
}

pub struct QuantileTDigestWeightedData {
    levels: Vec<f64>,
}

struct AggregateQuantileTDigestWeightedImplementation<V, W, R>
where
    V: AccessType,
    W: AccessType,
    R: ValueType,
{
    function_info: Arc<QuantileTDigestWeightedData>,
    _p: PhantomData<fn(V, W, R)>,
}

impl<V, W, R> AggregateQuantileTDigestWeightedImplementation<V, W, R>
where
    V: AccessType,
    W: AccessType,
    R: ValueType,
{
    fn new(function_info: QuantileTDigestWeightedData) -> Self {
        Self {
            function_info: Arc::new(function_info),
            _p: PhantomData,
        }
    }
}

impl<V, W, R> AggrImpl for AggregateQuantileTDigestWeightedImplementation<V, W, R>
where
    V: AccessType,
    V::Scalar: Number + AsPrimitive<f64>,
    W: AccessType,
    W::Scalar: Number + AsPrimitive<u64>,
    R: ValueType,
    AggregateQuantileTDigestState:
        QuantileTDigestWeightedResult<R, FunctionInfo = QuantileTDigestWeightedData>,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateQuantileTDigestState::new);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let values = input.columns[0].downcast::<V>().unwrap();
        let weights = input.columns[1].downcast::<W>().unwrap();
        let state = input.state.get::<AggregateQuantileTDigestState>();
        match input.validity {
            Some(validity) => {
                for ((value, weight), valid) in
                    values.iter().zip(weights.iter()).zip(validity.iter())
                {
                    if valid {
                        state.add_weighted_value(
                            V::to_owned_scalar(value).as_(),
                            W::to_owned_scalar(weight).as_(),
                        );
                    }
                }
            }
            None => {
                for (value, weight) in values.iter().zip(weights.iter()) {
                    state.add_weighted_value(
                        V::to_owned_scalar(value).as_(),
                        W::to_owned_scalar(weight).as_(),
                    );
                }
            }
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let values = input.columns[0].downcast::<V>().unwrap();
        let weights = input.columns[1].downcast::<W>().unwrap();
        for (row, state) in input.states.iter().enumerate() {
            let value = unsafe { values.index_unchecked(row) };
            let weight = unsafe { weights.index_unchecked(row) };
            state
                .get::<AggregateQuantileTDigestState>()
                .add_weighted_value(
                    V::to_owned_scalar(value).as_(),
                    W::to_owned_scalar(weight).as_(),
                );
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let values = input.columns[0].downcast::<V>().unwrap();
        let weights = input.columns[1].downcast::<W>().unwrap();
        let value = unsafe { values.index_unchecked(input.row) };
        let weight = unsafe { weights.index_unchecked(input.row) };
        input
            .state
            .get::<AggregateQuantileTDigestState>()
            .add_weighted_value(
                V::to_owned_scalar(value).as_(),
                W::to_owned_scalar(weight).as_(),
            );
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        let binary_builder = input.builders[0].as_binary_mut().unwrap();
        for state in input.states.iter() {
            let state = state.get::<AggregateQuantileTDigestState>();
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
            let mut rhs = AggregateQuantileTDigestState::deserialize_reader(&mut data)?;
            state
                .get::<AggregateQuantileTDigestState>()
                .merge_state(&mut rhs)?;
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateQuantileTDigestState>();
        let rhs = input.rhs.get::<AggregateQuantileTDigestState>();
        let mut rhs = rhs.clone_for_merge();
        state.merge_state(&mut rhs)
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateQuantileTDigestState>();
        state.write_result(input.builder, &self.function_info)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        let state = state.get::<AggregateQuantileTDigestState>();
        unsafe { std::ptr::drop_in_place(state) };
    }
}

trait QuantileTDigestWeightedResult<R>: Send + 'static
where R: ValueType
{
    type FunctionInfo: Send + Sync + 'static;

    fn write_result(
        &mut self,
        builder: &mut databend_common_expression::ColumnBuilder,
        function_info: &Self::FunctionInfo,
    ) -> Result<()>;
}

impl QuantileTDigestWeightedResult<Float64Type> for AggregateQuantileTDigestState {
    type FunctionInfo = QuantileTDigestWeightedData;

    fn write_result(
        &mut self,
        builder: &mut databend_common_expression::ColumnBuilder,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let mut builder = Float64Type::downcast_builder(builder);
        builder.push_item(self.quantile(function_info.levels[0]).into());
        Ok(())
    }
}

impl QuantileTDigestWeightedResult<ArrayType<Float64Type>> for AggregateQuantileTDigestState {
    type FunctionInfo = QuantileTDigestWeightedData;

    fn write_result(
        &mut self,
        builder: &mut databend_common_expression::ColumnBuilder,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let mut builder = ArrayType::<Float64Type>::downcast_builder(builder);
        for level in &function_info.levels {
            builder.put_item(self.quantile(*level).into());
        }
        builder.commit_row();
        Ok(())
    }
}

impl QuantileTDigestWeightedBuilder {
    fn create(
        build: MultiArgBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        let value_type = build.args_type()[0].clone();
        let weight_type = build.args_type()[1].clone();
        let display_name = build.name().to_string();
        let levels = get_levels(build.params())?;

        with_number_mapped_type!(|VALUE| match &value_type {
            DataType::Number(NumberDataType::VALUE) => {
                with_unsigned_integer_mapped_type!(|WEIGHT| match &weight_type {
                    DataType::Number(NumberDataType::WEIGHT) => {
                        Self::create_typed::<NumberType<VALUE>, NumberType<WEIGHT>>(build, levels)
                    }
                    _ => Err(ErrorCode::BadDataValueType(format!(
                        "weight just support unsigned integer type, but got '{:?}'",
                        weight_type
                    ))),
                })
            }
            _ => Err(ErrorCode::BadDataValueType(format!(
                "{} just support numeric type, but got '{:?}'",
                display_name, value_type
            ))),
        })
    }

    fn create_typed<V, W>(
        build: MultiArgBuildContext<'_, impl CombinatorImpl>,
        levels: Vec<f64>,
    ) -> Result<AggregateFunctionRef>
    where
        V: AccessType,
        V::Scalar: Number + AsPrimitive<f64>,
        W: AccessType,
        W::Scalar: Number + AsPrimitive<u64>,
    {
        if levels.len() > 1 {
            Self::create_result::<V, W, ArrayType<Float64Type>>(
                build,
                DataType::Array(Box::new(Float64Type::data_type())),
                levels,
            )
        } else {
            Self::create_result::<V, W, Float64Type>(build, Float64Type::data_type(), levels)
        }
    }

    fn create_result<V, W, R>(
        build: MultiArgBuildContext<'_, impl CombinatorImpl>,
        return_type: DataType,
        levels: Vec<f64>,
    ) -> Result<AggregateFunctionRef>
    where
        V: AccessType,
        V::Scalar: Number + AsPrimitive<f64>,
        W: AccessType,
        W::Scalar: Number + AsPrimitive<u64>,
        R: ValueType,
        AggregateQuantileTDigestState:
            QuantileTDigestWeightedResult<R, FunctionInfo = QuantileTDigestWeightedData>,
    {
        let state = AggregateQuantileTDigestState::state_description();
        let implementation = AggregateQuantileTDigestWeightedImplementation::<V, W, R>::new(
            QuantileTDigestWeightedData { levels },
        );
        build.create_multi_arg_or_null(return_type.wrap_nullable(), state, implementation)
    }
}
