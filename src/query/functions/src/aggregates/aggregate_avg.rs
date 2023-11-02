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
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::decimal::*;
use common_expression::types::*;
use common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use ethnum::i256;
use serde::Deserialize;
use serde::Serialize;

use super::aggregate_sum::DecimalSumState;
use super::aggregate_sum::NumberSumState;
use super::aggregate_sum::SumState;
use super::deserialize_state;
use super::serialize_state;
use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;

#[derive(Serialize, Deserialize)]
struct AvgState<T> {
    pub value: T,
    pub count: u64,
}

#[derive(Clone)]
pub struct AggregateAvgFunction<T> {
    display_name: String,
    _arguments: Vec<DataType>,
    t: PhantomData<T>,
    return_type: DataType,

    // only for decimals
    // AVG：AVG(DECIMAL(a, b)) -> DECIMAL(38 or 76, max(b, 4))。
    scale_add: u8,
}

impl<T> AggregateFunction for AggregateAvgFunction<T>
where T: SumState
{
    fn name(&self) -> &str {
        "AggregateAvgFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AvgState::<T> {
            value: T::default(),
            count: 0,
        });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AvgState<T>>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AvgState<T>>();
        state.count += validity.map_or(input_rows as u64, |v| (v.len() - v.unset_bits()) as u64);
        state.value.accumulate(&columns[0], validity)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        for place in places {
            let state = place.next(offset).get::<AvgState<T>>();
            state.count += 1;
        }
        T::accumulate_keys(places, offset, &columns[0])
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let state = place.get::<AvgState<T>>();
        state.value.accumulate_row(&columns[0], row)?;
        state.count += 1;
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<AvgState<T>>();

        serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AvgState<T>>();
        let rhs: AvgState<T> = deserialize_state(reader)?;

        state.count += rhs.count;
        state.value.merge(&rhs.value)
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AvgState<T>>();
        let rhs = rhs.get::<AvgState<T>>();
        state.count += rhs.count;
        state.value.merge(&rhs.value)
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<AvgState<T>>();
        state
            .value
            .merge_avg_result(builder, state.count, self.scale_add, &None)
    }
}

impl<T> fmt::Display for AggregateAvgFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T> AggregateAvgFunction<T>
where T: SumState
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataType>,
        return_type: DataType,
        scale_add: u8,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_string(),
            _arguments: arguments,
            t: PhantomData,
            return_type,
            scale_add,
        }))
    }
}

pub fn try_create_aggregate_avg_function(
    display_name: &str,
    _params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;

    // null use dummy func, it's already covered in `AggregateNullResultFunction`
    let data_type = if arguments[0].is_null() {
        Int8Type::data_type()
    } else {
        arguments[0].clone()
    };
    with_number_mapped_type!(|NUM_TYPE| match &data_type {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            type TSum = <NUM_TYPE as ResultTypeOfUnary>::Sum;
            AggregateAvgFunction::<NumberSumState<NUM_TYPE, TSum>>::try_create(
                display_name,
                arguments,
                Float64Type::data_type(),
                0,
            )
        }
        DataType::Decimal(DecimalDataType::Decimal128(s)) => {
            let p = MAX_DECIMAL128_PRECISION;
            let decimal_size = DecimalSize {
                precision: p,
                scale: s.scale.max(4),
            };

            let overflow = s.precision > 18;

            if overflow {
                AggregateAvgFunction::<DecimalSumState<true, i128>>::try_create(
                    display_name,
                    arguments,
                    DataType::Decimal(DecimalDataType::from_size(decimal_size)?),
                    decimal_size.scale - s.scale,
                )
            } else {
                AggregateAvgFunction::<DecimalSumState<false, i128>>::try_create(
                    display_name,
                    arguments,
                    DataType::Decimal(DecimalDataType::from_size(decimal_size)?),
                    decimal_size.scale - s.scale,
                )
            }
        }
        DataType::Decimal(DecimalDataType::Decimal256(s)) => {
            let p = MAX_DECIMAL256_PRECISION;

            let decimal_size = DecimalSize {
                precision: p,
                scale: s.scale.max(4),
            };

            let overflow = s.precision > 18;

            if overflow {
                AggregateAvgFunction::<DecimalSumState<true, i256>>::try_create(
                    display_name,
                    arguments,
                    DataType::Decimal(DecimalDataType::from_size(decimal_size)?),
                    decimal_size.scale - s.scale,
                )
            } else {
                AggregateAvgFunction::<DecimalSumState<false, i256>>::try_create(
                    display_name,
                    arguments,
                    DataType::Decimal(DecimalDataType::from_size(decimal_size)?),
                    decimal_size.scale - s.scale,
                )
            }
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "AggregateAvgFunction does not support type '{:?}'",
            arguments[0]
        ))),
    })
}

pub fn aggregate_avg_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_avg_function),
        features,
    )
}
