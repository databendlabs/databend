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
use std::f64::consts::PI;

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
use databend_common_expression::types::DecimalF64View;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::array::ArrayColumnBuilderMut;
use databend_common_expression::types::compute_view::NumberConvertView;
use databend_common_expression::types::i256;
use databend_common_expression::types::number::F64;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;

use super::super::get_levels;
use super::AggregateFunctionDefinition;
use super::AggregateFunctionV2Factory;
use super::adaptors_v2 as v2;
use super::adaptors_v2::UnaryState;

struct QuantileTDigestBuilder;

impl QuantileTDigestBuilder {
    fn register(registry: &mut v2::AggregateFunctionRegistry) {
        let quantile_tdigest = AggregateFunctionDefinition::new(
            "quantile_tdigest",
            QuantileTDigestBuilder::quantile_tdigest_arguments(),
            QuantileTDigestBuilder::QUANTILE_TDIGEST_FEATURES,
            QuantileTDigestBuilder::try_create_quantile_tdigest,
        );
        quantile_tdigest.register_with_combinators(registry, false);
        let median_tdigest = AggregateFunctionDefinition::new(
            "median_tdigest",
            QuantileTDigestBuilder::quantile_tdigest_arguments(),
            QuantileTDigestBuilder::MEDIAN_TDIGEST_FEATURES,
            QuantileTDigestBuilder::try_create_median_tdigest,
        );
        median_tdigest.register_with_combinators(registry, false);
    }
}

inventory::submit! {
    AggregateFunctionV2Factory {
        register: QuantileTDigestBuilder::register,
    }
}

impl QuantileTDigestBuilder {
    fn quantile_tdigest_arguments() -> v2::AggregateArgumentsPattern {
        v2::AggregateArgumentsPattern::fixed(vec![v2::AggregateArgumentPattern::any_numeric()])
    }

    const QUANTILE_TDIGEST_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: false,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns an approximate quantile value using t-digest",
        definition: "quantile_tdigest(level)(expr)",
        example: "select quantile_tdigest(0.5)(number) from numbers(10)",
    };

    const MEDIAN_TDIGEST_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: false,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the approximate median input value using t-digest",
        definition: "median_tdigest(expr)",
        example: "select median_tdigest(number) from numbers(10)",
    };
}

pub struct QuantileTDigestData {
    levels: Vec<f64>,
}

#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub enum AggregateQuantileTDigestState {
    Normal(TDigestData),
    Nan,
}

#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub struct TDigestData {
    total_weight: f64,
    centroids: Vec<Centroid>,
    unmerged_total_weight: f64,
    unmerged: Vec<Centroid>,

    min: f64,
    max: f64,
}

#[derive(Clone, Copy, BorshSerialize, BorshDeserialize)]
struct Centroid {
    mean: f64,
    weight: f64,
}

impl AggregateQuantileTDigestState {
    pub fn state_description() -> v2::AggregateStateDescription {
        v2::AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<Self>())],
            vec![StateSerdeItem::Binary(None)],
        )
        .with_manual_drop(true)
    }

    pub(super) fn new() -> Self {
        Self::Normal(TDigestData {
            total_weight: 0.0,
            centroids: vec![],
            unmerged_total_weight: 0.0,
            unmerged: vec![],
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        })
    }

    pub(super) fn add_value(&mut self, value: f64) {
        self.add_weighted_value(value, 1);
    }

    pub(super) fn add_weighted_value(&mut self, value: f64, weight: u64) {
        if weight == 0 {
            return;
        }
        if value.is_nan() {
            *self = Self::Nan;
            return;
        }

        let Self::Normal(state) = self else {
            return;
        };
        state.add_finite(value, weight as f64);
    }

    pub(super) fn merge_state(&mut self, rhs: &mut Self) -> Result<()> {
        match (&mut *self, rhs) {
            (Self::Nan, _) | (_, Self::Nan) => {
                *self = Self::Nan;
            }
            (Self::Normal(state), Self::Normal(rhs)) => state.merge(rhs)?,
        }
        Ok(())
    }

    pub(super) fn quantile(&mut self, level: f64) -> f64 {
        match self {
            Self::Normal(state) => state.quantile(level),
            Self::Nan => f64::NAN,
        }
    }

    pub(super) fn clone_for_merge(&self) -> Self {
        self.clone()
    }
}

impl TDigestData {
    const EPSILON: f64 = 100.0;
    const MAX_CENTROIDS: usize = 2048;

    fn add_finite(&mut self, value: f64, weight: f64) {
        if self.unmerged.len() + self.centroids.len() >= Self::MAX_CENTROIDS - 1 {
            self.compress();
        }

        self.unmerged.push(Centroid {
            mean: value,
            weight,
        });
        self.unmerged_total_weight += weight;
    }

    fn merge(&mut self, rhs: &mut Self) -> Result<()> {
        if rhs.len() == 0 {
            return Ok(());
        }

        rhs.compress();

        self.unmerged.extend_from_slice(&rhs.centroids);
        self.unmerged_total_weight += rhs
            .centroids
            .iter()
            .map(|centroid| centroid.weight)
            .sum::<f64>();
        self.compress();

        Ok(())
    }

    fn quantile(&mut self, level: f64) -> f64 {
        self.compress();
        let (first, last) = match self.centroids.as_slice() {
            [] => return 0.0,
            [Centroid { mean, .. }] => return *mean,
            [first, .., last] => (*first, *last),
        };

        let index = level * self.total_weight;
        if index < 1.0 {
            return self.min;
        }
        if first.weight > 1.0 && index < first.weight / 2.0 {
            return self.min + (index - 1.0) / (first.weight / 2.0 - 1.0) * (first.mean - self.min);
        }
        if index > self.total_weight - 1.0 {
            return self.max;
        }
        if last.weight > 1.0 && self.total_weight - index <= last.weight / 2.0 {
            if last.weight / 2.0 <= 1.0 {
                return self.max;
            }
            return self.max
                - (self.total_weight - index - 1.0) / (last.weight / 2.0 - 1.0)
                    * (self.max - last.mean);
        }

        let mut weight_so_far = first.weight / 2.0;
        for (left, right) in self
            .centroids
            .windows(2)
            .map(|centroids| (centroids[0], centroids[1]))
        {
            let dw = (left.weight + right.weight) / 2.0;
            if weight_so_far + dw > index {
                let mut left_unit = 0.0;
                if left.weight == 1.0 {
                    if index - weight_so_far < 0.5 {
                        return left.mean;
                    }
                    left_unit = 0.5;
                }

                let mut right_unit = 0.0;
                if right.weight == 1.0 {
                    if weight_so_far + dw - index <= 0.5 {
                        return right.mean;
                    }
                    right_unit = 0.5;
                }

                let z1 = index - weight_so_far - left_unit;
                let z2 = weight_so_far + dw - index - right_unit;
                return Self::weighted_average(left.mean, z2, right.mean, z1);
            }
            weight_so_far += dw;
        }

        debug_assert!(index <= self.total_weight);
        debug_assert!(index >= self.total_weight - last.weight / 2.0);

        let z1 = index - self.total_weight - last.weight / 2.0;
        let z2 = last.weight / 2.0 - z1;

        Self::weighted_average(last.mean, z1, self.max, z2)
    }

    fn len(&self) -> usize {
        (self.total_weight + self.unmerged_total_weight) as usize
    }

    fn weighted_average(m1: f64, w1: f64, m2: f64, w2: f64) -> f64 {
        let a = f64::min(m1, m2);
        let b = f64::max(m1, m2);
        let x = (m1 * w1 + m2 * w2) / (w1 + w2);

        f64::max(a, f64::min(b, x))
    }

    fn compress(&mut self) {
        if self.unmerged_total_weight > 0.0 {
            self.merge_centroid(self.unmerged.clone());
            self.unmerged.clear();
            self.unmerged_total_weight = 0.0;
        }
    }

    fn merge_centroid(&mut self, mut incoming: Vec<Centroid>) {
        incoming.extend_from_slice(&self.centroids);
        incoming.sort_by(|a, b| a.mean.total_cmp(&b.mean));

        self.total_weight += self.unmerged_total_weight;

        let normalizer = Self::EPSILON / (PI * self.total_weight);

        let mut incoming = incoming.into_iter();
        let mut current = incoming.next().unwrap();
        let first_mean = current.mean;
        let mut centroids = vec![];
        let mut weight_so_far = 0.0;

        for centroid in incoming {
            let proposed_weight = current.weight + centroid.weight;
            let z = normalizer * proposed_weight;
            let q0 = weight_so_far / self.total_weight;
            let q2 = (weight_so_far + proposed_weight) / self.total_weight;
            if z * z <= q0 * (1.0 - q0) && z * z <= q2 * (1.0 - q2) {
                current.weight = proposed_weight;
                current.mean += (centroid.mean - current.mean) * centroid.weight / current.weight;
            } else {
                weight_so_far += current.weight;
                centroids.push(current);
                current = centroid;
            }
        }

        if self.total_weight > 0.0 {
            self.min = f64::min(self.min, first_mean);
            self.max = f64::max(self.max, current.mean);
        }

        centroids.push(current);
        self.centroids = centroids;
    }
}

impl<I> UnaryState<I, ArrayType<Float64Type>> for AggregateQuantileTDigestState
where for<'a> I: AccessType<Scalar = F64, ScalarRef<'a> = F64>
{
    type FunctionInfo = QuantileTDigestData;

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::new()
    }

    fn add(&mut self, value: I::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.add_value(value.into());
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        let mut rhs = rhs.clone_for_merge();
        self.merge_state(&mut rhs)
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge_state(rhs)
    }

    fn merge_result(
        &mut self,
        mut builder: ArrayColumnBuilderMut<'_, Float64Type>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        for level in &function_info.levels {
            builder.put_item(self.quantile(*level).into());
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
        let mut rhs = Self::deserialize_reader(&mut data)?;
        self.merge_state(&mut rhs)
    }
}

impl<I> UnaryState<I, Float64Type> for AggregateQuantileTDigestState
where for<'a> I: AccessType<Scalar = F64, ScalarRef<'a> = F64>
{
    type FunctionInfo = QuantileTDigestData;

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::new()
    }

    fn add(&mut self, value: I::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.add_value(value.into());
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        let mut rhs = rhs.clone_for_merge();
        self.merge_state(&mut rhs)
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge_state(rhs)
    }

    fn merge_result(
        &mut self,
        mut builder: <Float64Type as ValueType>::ColumnBuilderMut<'_>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        builder.push_item(self.quantile(function_info.levels[0]).into());
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
        let mut rhs = Self::deserialize_reader(&mut data)?;
        self.merge_state(&mut rhs)
    }
}

impl QuantileTDigestBuilder {
    fn try_create_quantile_tdigest(
        request: v2::AggregateFunctionRequest<'_>,
    ) -> Result<v2::AggregateFunctionRef> {
        v2::build_default_name_route_with_unary_input(
            request,
            &["quantile_tdigest"],
            Self::QUANTILE_TDIGEST_FEATURES,
            false,
            unary_aggregate_function_build_input_fns!(Self::create),
        )
    }

    fn try_create_median_tdigest(
        request: v2::AggregateFunctionRequest<'_>,
    ) -> Result<v2::AggregateFunctionRef> {
        if !request.params.is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                request.name
            )));
        }
        v2::build_default_name_route_with_unary_input(
            request,
            &["median_tdigest"],
            Self::MEDIAN_TDIGEST_FEATURES,
            false,
            unary_aggregate_function_build_input_fns!(Self::create),
        )
    }

    fn create(
        build: v2::UnaryBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<v2::AggregateFunctionRef> {
        let data_type = build.arg_type().clone();
        let display_name = build.name().to_string();
        let levels = get_levels(build.params())?;

        with_number_mapped_type!(|NUM| match &data_type {
            DataType::Number(NumberDataType::NUM) => {
                type Input = NumberConvertView<NUM, F64>;
                Self::create_typed::<Input>(build, levels)
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => {
                        type Input = DecimalF64View<DECIMAL>;
                        Self::create_typed::<Input>(build, levels)
                    }
                })
            }
            _ => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                display_name, data_type
            ))),
        })
    }

    fn create_typed<I>(
        build: v2::UnaryBuildContext<'_, impl v2::CombinatorImpl>,
        levels: Vec<f64>,
    ) -> Result<v2::AggregateFunctionRef>
    where
        for<'a> I: AccessType<Scalar = F64, ScalarRef<'a> = F64>,
    {
        if levels.len() > 1 {
            Self::create_result::<I, ArrayType<Float64Type>>(
                build,
                DataType::Array(Box::new(Float64Type::data_type())),
                levels,
            )
        } else {
            Self::create_result::<I, Float64Type>(build, Float64Type::data_type(), levels)
        }
    }

    fn create_result<I, R>(
        build: v2::UnaryBuildContext<'_, impl v2::CombinatorImpl>,
        return_type: DataType,
        levels: Vec<f64>,
    ) -> Result<v2::AggregateFunctionRef>
    where
        for<'a> I: AccessType<Scalar = F64, ScalarRef<'a> = F64>,
        R: ValueType,
        AggregateQuantileTDigestState: UnaryState<I, R, FunctionInfo = QuantileTDigestData>,
    {
        let state = AggregateQuantileTDigestState::state_description();

        build.create_unary_or_null::<AggregateQuantileTDigestState, I, R>(
            return_type.wrap_nullable(),
            state,
            QuantileTDigestData { levels },
        )
    }
}
