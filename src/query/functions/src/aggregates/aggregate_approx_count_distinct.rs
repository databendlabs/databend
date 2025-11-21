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

use std::hash::Hash;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::Result;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use databend_common_expression::StateSerdeItem;
use simple_hll::HyperLogLog;

use super::assert_unary_arguments;
use super::batch_merge1;
use super::extract_number_param;
use super::AggrState;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionSortDesc;
use super::AggregateUnaryFunction;
use super::SerializeInfo;
use super::StateSerde;
use super::UnaryState;

/// Use Hyperloglog to estimate distinct of values
type AggregateApproxCountDistinctState<const HLL_P: usize> = HyperLogLog<HLL_P>;

impl<const HLL_P: usize, T> UnaryState<T, UInt64Type> for AggregateApproxCountDistinctState<HLL_P>
where
    T: ValueType,
    T::Scalar: Hash,
{
    fn add(&mut self, other: T::ScalarRef<'_>, _: &Self::FunctionInfo) -> Result<()> {
        self.add_object(&T::to_owned_scalar(other));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.merge(rhs);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: BuilderMut<'_, UInt64Type>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        builder.push(self.count() as u64);
        Ok(())
    }
}

impl<const HLL_P: usize> StateSerde for AggregateApproxCountDistinctState<HLL_P> {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        for place in places {
            let state: &mut Self = AggrState::new(*place, loc).get();
            state.serialize(&mut binary_builder.data)?;
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<BinaryType, Self, _>(places, loc, state, filter, |state, mut data| {
            let rhs = Self::deserialize_reader(&mut data)?;
            state.merge(&rhs);
            Ok(())
        })
    }
}

pub fn try_create_aggregate_approx_count_distinct_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;

    let mut p = 14;

    if !params.is_empty() {
        let error_rate = extract_number_param::<F64>(params[0].clone())?.0;
        p = ((1.04f64 / error_rate).log2() * 2.0).ceil() as u64;
        p = p.clamp(4, 14);
    }

    match p {
        4 => create_templated::<4>(display_name, arguments),
        5 => create_templated::<5>(display_name, arguments),
        6 => create_templated::<6>(display_name, arguments),
        7 => create_templated::<7>(display_name, arguments),
        8 => create_templated::<8>(display_name, arguments),
        9 => create_templated::<9>(display_name, arguments),
        10 => create_templated::<10>(display_name, arguments),
        11 => create_templated::<11>(display_name, arguments),
        12 => create_templated::<12>(display_name, arguments),
        13 => create_templated::<13>(display_name, arguments),
        14 => create_templated::<14>(display_name, arguments),
        _ => unreachable!(),
    }
}

fn create_templated<const P: usize>(
    display_name: &str,
    arguments: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    let return_type = DataType::Number(NumberDataType::UInt64);
    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            AggregateUnaryFunction::<HyperLogLog<P>, NumberType<NUM_TYPE>, UInt64Type>::new(
                display_name,
                return_type,
            )
            .with_need_drop(true)
            .finish()
        }
        DataType::String => {
            AggregateUnaryFunction::<HyperLogLog<P>, StringType, UInt64Type>::new(
                display_name,
                return_type,
            )
            .with_need_drop(true)
            .finish()
        }
        DataType::Date => {
            AggregateUnaryFunction::<HyperLogLog<P>, DateType, UInt64Type>::new(
                display_name,
                return_type,
            )
            .with_need_drop(true)
            .finish()
        }
        DataType::Timestamp => {
            AggregateUnaryFunction::<HyperLogLog<P>, TimestampType, UInt64Type>::new(
                display_name,
                return_type,
            )
            .with_need_drop(true)
            .finish()
        }
        _ => {
            AggregateUnaryFunction::<HyperLogLog<P>, AnyType, UInt64Type>::new(
                display_name,
                return_type,
            )
            .with_need_drop(true)
            .finish()
        }
    })
}

pub fn aggregate_approx_count_distinct_function_desc() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        returns_default_when_only_null: true,
        ..Default::default()
    };

    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_approx_count_distinct_function),
        features,
    )
}
