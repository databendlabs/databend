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

use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::compute_view::NumberConvertView;
use databend_common_expression::types::i256;
use databend_common_expression::types::nullable::NullableColumnBuilderMut;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalF64View;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::F64;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use databend_common_expression::StateSerdeItem;

use super::aggregator_common::assert_params;
use super::aggregator_common::assert_unary_arguments;
use super::batch_merge1;
use super::AggrState;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionSortDesc;
use super::AggregateUnaryFunction;
use super::SerializeInfo;
use super::StateSerde;
use super::UnaryState;

const STD_POP: u8 = 0;
const STD_SAMP: u8 = 1;
const VAR_POP: u8 = 2;
const VAR_SAMP: u8 = 3;

// Streaming approximate standard deviation using Welford's
// method, DOI: 10.2307/1266577
#[derive(BorshSerialize, BorshDeserialize, Default)]
struct StddevState<const TYPE: u8> {
    count: u64,    // n
    mean: f64,     // M1
    dsquared: f64, // M2
}

impl<const TYPE: u8> StddevState<TYPE> {
    fn state_add(&mut self, value: f64) -> Result<()> {
        self.count += 1;
        let mean_differential = (value - self.mean) / self.count as f64;
        let new_mean = self.mean + mean_differential;
        let dsquared_increment = (value - new_mean) * (value - self.mean);
        let new_dsquared = self.dsquared + dsquared_increment;

        self.mean = new_mean;
        self.dsquared = new_dsquared;
        Ok(())
    }

    fn state_merge(&mut self, other: &Self) -> Result<()> {
        if self.count == 0 {
            self.count = other.count;
            self.mean = other.mean;
            self.dsquared = other.dsquared;
            return Ok(());
        }

        if other.count > 0 {
            let count = self.count + other.count;
            let mean =
                (self.count as f64 * self.mean + other.count as f64 * other.mean) / count as f64;
            let delta = other.mean - self.mean;

            self.dsquared = other.dsquared
                + self.dsquared
                + delta * delta * other.count as f64 * self.count as f64 / count as f64;

            self.mean = mean;
            self.count = count;
        }

        Ok(())
    }

    fn state_merge_result(
        &mut self,
        mut builder: NullableColumnBuilderMut<'_, Float64Type>,
    ) -> Result<()> {
        // For single-record inputs, VAR_SAMP and STDDEV_SAMP should return NULL
        if self.count <= 1 && (TYPE == VAR_SAMP || TYPE == STD_SAMP) {
            builder.push_null();
        } else {
            let value = match TYPE {
                STD_POP => (self.dsquared / self.count as f64).sqrt(),
                STD_SAMP => (self.dsquared / (self.count - 1) as f64).sqrt(),
                VAR_POP => self.dsquared / self.count as f64,
                VAR_SAMP => self.dsquared / (self.count - 1) as f64,
                _ => unreachable!(),
            };
            builder.push(value.into());
        };
        Ok(())
    }
}

impl<T, const TYPE: u8> UnaryState<T, NullableType<Float64Type>> for StddevState<TYPE>
where
    T: AccessType,
    T::Scalar: Into<f64>,
{
    fn add(&mut self, other: T::ScalarRef<'_>, _: &Self::FunctionInfo) -> Result<()> {
        let value = T::to_owned_scalar(other).into();
        self.state_add(value)
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.state_merge(other)
    }

    fn merge_result(
        &mut self,
        builder: NullableColumnBuilderMut<'_, Float64Type>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        self.state_merge_result(builder)
    }
}

impl<const TYPE: u8> StateSerde for StddevState<TYPE> {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(Some(24))]
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
            <Self as UnaryState<Float64Type, NullableType<Float64Type>>>::merge(state, &rhs)
        })
    }
}

pub fn try_create_aggregate_stddev_pop_function<const TYPE: u8>(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_params(display_name, params.len(), 0)?;
    assert_unary_arguments(display_name, arguments.len())?;

    let return_type = DataType::Number(NumberDataType::Float64).wrap_nullable();
    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            AggregateUnaryFunction::<
                StddevState<TYPE>,
                NumberConvertView<NUM_TYPE, F64>,
                NullableType<Float64Type>,
            >::create(display_name, return_type)
        }
        DataType::Decimal(s) => {
            with_decimal_mapped_type!(|DECIMAL| match s.data_kind() {
                DecimalDataKind::DECIMAL => {
                    AggregateUnaryFunction::<
                        StddevState<TYPE>,
                        DecimalF64View<DECIMAL>,
                        NullableType<Float64Type>,
                    >::create(display_name, return_type)
                }
            })
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, arguments[0]
        ))),
    })
}

pub fn aggregate_stddev_pop_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_stddev_pop_function::<STD_POP>,
    ))
}

pub fn aggregate_stddev_samp_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_stddev_pop_function::<STD_SAMP>,
    ))
}
