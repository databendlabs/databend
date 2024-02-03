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

use databend_common_base::containers::HyperLogLog;
use databend_common_exception::Result;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Scalar;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::borsh_deserialize_state;
use super::borsh_serialize_state;
use super::AggregateUnaryFunction;
use super::FunctionData;
use super::UnaryState;
use crate::aggregates::aggregator_common::assert_unary_arguments;

const HLL_P: usize = 14;
/// Use Hyperloglog to estimate distinct of values
struct AggregateApproxCountDistinctState {
    hll: HyperLogLog<HLL_P>,
}

impl Default for AggregateApproxCountDistinctState {
    fn default() -> Self {
        Self {
            hll: HyperLogLog::<HLL_P>::new(),
        }
    }
}

impl<T> UnaryState<T, UInt64Type> for AggregateApproxCountDistinctState
where
    T: ValueType + Send + Sync,
    T::Scalar: Hash,
{
    fn add(&mut self, other: T::ScalarRef<'_>) -> Result<()> {
        self.hll.add_object(&T::to_owned_scalar(other));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.hll.merge(&rhs.hll);
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut Vec<u64>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        builder.push(self.hll.count() as u64);
        Ok(())
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        borsh_serialize_state(writer, &self.hll)
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where Self: Sized {
        let hll = borsh_deserialize_state(reader)?;
        Ok(Self { hll })
    }
}

pub fn try_create_aggregate_approx_count_distinct_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;

    let return_type = DataType::Number(NumberDataType::UInt64);

    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            let func = AggregateUnaryFunction::<
                AggregateApproxCountDistinctState,
                NumberType<NUM_TYPE>,
                UInt64Type,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_need_drop(true);

            Ok(Arc::new(func))
        }
        DataType::String => {
            let func = AggregateUnaryFunction::<
                AggregateApproxCountDistinctState,
                StringType,
                UInt64Type,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_need_drop(true);

            Ok(Arc::new(func))
        }
        DataType::Date => {
            let func = AggregateUnaryFunction::<
                AggregateApproxCountDistinctState,
                DateType,
                UInt64Type,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_need_drop(true);

            Ok(Arc::new(func))
        }
        DataType::Timestamp => {
            let func = AggregateUnaryFunction::<
                AggregateApproxCountDistinctState,
                TimestampType,
                UInt64Type,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_need_drop(true);

            Ok(Arc::new(func))
        }
        _ => {
            let func = AggregateUnaryFunction::<
                AggregateApproxCountDistinctState,
                AnyType,
                UInt64Type,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_need_drop(true);

            Ok(Arc::new(func))
        }
    })
}

pub fn aggregate_approx_count_distinct_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        returns_default_when_only_null: true,
        ..Default::default()
    };

    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_approx_count_distinct_function),
        features,
    )
}
