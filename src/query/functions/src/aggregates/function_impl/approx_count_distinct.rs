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
use std::hash::Hash;
use std::marker::PhantomData;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::with_number_mapped_type;
use simple_hll::HyperLogLog;

use super::AggregateRegistration;
use super::adaptors::*;
use crate::aggregates::common::extract_number_param;

struct ApproxCountDistinctBuilder;

impl ApproxCountDistinctBuilder {
    fn register(registry: &mut AggregateRegistry) {
        NameRoute::new(
            &["approx_count_distinct"],
            ArgumentsPattern::fixed(vec![ArgumentPattern::any()]),
            Self::APPROX_COUNT_DISTINCT_FEATURES,
            NullPolicy::ReturnsDefaultWhenOnlyNull,
        )
        .then(MergeRoute::unary(false, Self::create))
        .then(MergeRoute::unary(true, Self::create))
        .then(PlainRoute::unary(Self::create))
        .then(IfRoute::unary(Self::create))
        .then(StateRoute::unary(Self::create))
        .register(registry);
    }
}

inventory::submit! {
    AggregateRegistration {
        register: ApproxCountDistinctBuilder::register,
    }
}

impl ApproxCountDistinctBuilder {
    const APPROX_COUNT_DISTINCT_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: false,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "estimates the number of distinct input values",
        definition: "approx_count_distinct(expr)",
        example: "select approx_count_distinct(number) from numbers(10)",
    };
}

pub struct ApproxCountDistinctState<const P: usize, T>
where T: ValueType
{
    hll: HyperLogLog<P>,
    _p: PhantomData<fn(T)>,
}

impl<const P: usize, T> Default for ApproxCountDistinctState<P, T>
where T: ValueType
{
    fn default() -> Self {
        Self {
            hll: HyperLogLog::default(),
            _p: PhantomData,
        }
    }
}

impl<const P: usize, T> ApproxCountDistinctState<P, T>
where
    T: ValueType,
    T::Scalar: Hash,
{
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(None),
        ])
        .with_manual_drop(true)
    }

    fn add(&mut self, value: T::ScalarRef<'_>) {
        self.hll.add_object(&T::to_owned_scalar(value));
    }

    fn merge_state(&mut self, rhs: &Self) {
        self.hll.merge(&rhs.hll);
    }
}

impl<const P: usize, T> UnaryState<T, UInt64Type> for ApproxCountDistinctState<P, T>
where
    T: ValueType,
    T::Scalar: Hash,
{
    type FunctionInfo = ();

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.add(value);
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.merge_state(rhs);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: <UInt64Type as ValueType>::ColumnBuilderMut<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        builder.push_item(self.hll.count() as u64);
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let binary_builder = builder.as_binary_mut().unwrap();
        BorshSerialize::serialize(&self.hll, &mut binary_builder.data)?;
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
        let rhs = HyperLogLog::<P>::deserialize_reader(&mut data)?;
        self.hll.merge(&rhs);
        Ok(())
    }
}

impl ApproxCountDistinctBuilder {
    fn create(build: UnaryBuildContext<'_, impl Combinator>) -> Result<AggregateCallRef> {
        let mut p = 14;
        if !build.params().is_empty() {
            let error_rate = Self::extract_f64_param(build.params()[0].clone())?;
            p = ((1.04f64 / error_rate).log2() * 2.0).ceil() as u64;
            p = p.clamp(4, 14);
        }

        let data_type = build.arg_type().clone();
        match p {
            4 => Self::create_templated::<4>(build, &data_type),
            5 => Self::create_templated::<5>(build, &data_type),
            6 => Self::create_templated::<6>(build, &data_type),
            7 => Self::create_templated::<7>(build, &data_type),
            8 => Self::create_templated::<8>(build, &data_type),
            9 => Self::create_templated::<9>(build, &data_type),
            10 => Self::create_templated::<10>(build, &data_type),
            11 => Self::create_templated::<11>(build, &data_type),
            12 => Self::create_templated::<12>(build, &data_type),
            13 => Self::create_templated::<13>(build, &data_type),
            14 => Self::create_templated::<14>(build, &data_type),
            _ => unreachable!(),
        }
    }

    fn create_templated<const P: usize>(
        build: UnaryBuildContext<'_, impl Combinator>,
        data_type: &DataType,
    ) -> Result<AggregateCallRef> {
        with_number_mapped_type!(|NUM_TYPE| match data_type {
            DataType::Number(NumberDataType::NUM_TYPE) => {
                Self::create_instance::<P, NumberType<NUM_TYPE>>(build)
            }
            DataType::String => Self::create_instance::<P, StringType>(build),
            DataType::Date => Self::create_instance::<P, DateType>(build),
            DataType::Timestamp => Self::create_instance::<P, TimestampType>(build),
            _ => Self::create_instance::<P, AnyType>(build),
        })
    }

    fn create_instance<const P: usize, T>(
        build: UnaryBuildContext<'_, impl Combinator>,
    ) -> Result<AggregateCallRef>
    where
        T: ValueType,
        T::Scalar: Hash,
    {
        build.create_unary::<ApproxCountDistinctState<P, T>, T, UInt64Type>(
            UInt64Type::data_type(),
            ApproxCountDistinctState::<P, T>::state_description(),
            (),
        )
    }

    fn extract_f64_param(param: Scalar) -> Result<f64> {
        Ok(extract_number_param::<databend_common_expression::types::F64>(param)?.0)
    }
}
