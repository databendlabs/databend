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

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use databend_common_expression::types::array::ArrayColumnBuilderMut;
use databend_common_expression::types::*;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;

use super::AggrState;
use super::AggregateFunctionDescription;
use super::AggregateFunctionRef;
use super::AggregateFunctionSortDesc;
use super::AggregateUnaryFunction;
use super::QuantileData;
use super::SerializeInfo;
use super::StateSerde;
use super::StateSerdeItem;
use super::UnaryState;
use super::assert_unary_arguments;
use super::batch_merge1;
use super::get_levels;
use crate::with_simple_no_number_mapped_type;

#[derive(BorshSerialize, BorshDeserialize)]
struct QuantileState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    pub value: Vec<T::Scalar>,
}

impl<T> Default for QuantileState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self { value: vec![] }
    }
}

impl<T> QuantileState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value.extend(
            rhs.value
                .iter()
                .map(|v| T::to_owned_scalar(T::to_scalar_ref(v))),
        );
        Ok(())
    }
}

impl<T> UnaryState<T, ArrayType<T>> for QuantileState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Ord,
{
    type FunctionInfo = QuantileData;

    fn add(&mut self, other: T::ScalarRef<'_>, _: &Self::FunctionInfo) -> Result<()> {
        self.value.push(T::to_owned_scalar(other));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.merge(rhs)
    }

    fn merge_result(
        &mut self,
        mut builder: ArrayColumnBuilderMut<'_, T>,
        quantile_disc_data: &QuantileData,
    ) -> Result<()> {
        let value_len = self.value.len();
        if quantile_disc_data.levels.len() > 1 {
            let indices = quantile_disc_data
                .levels
                .iter()
                .map(|level| ((value_len - 1) as f64 * (*level)).floor() as usize)
                .collect::<Vec<usize>>();
            for idx in indices {
                if idx < value_len {
                    self.value.as_mut_slice().select_nth_unstable(idx);
                    let value = self.value.get(idx).unwrap();
                    builder.put_item(T::to_scalar_ref(value));
                } else {
                    builder.push_default();
                }
            }
            builder.commit_row();
        }
        Ok(())
    }
}

impl<T> StateSerde for QuantileState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
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
            state.merge(&rhs)
        })
    }
}

impl<T> UnaryState<T, T> for QuantileState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Ord,
{
    type FunctionInfo = QuantileData;

    fn add(&mut self, other: T::ScalarRef<'_>, _: &Self::FunctionInfo) -> Result<()> {
        self.value.push(T::to_owned_scalar(other));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value.extend(
            rhs.value
                .iter()
                .map(|v| T::to_owned_scalar(T::to_scalar_ref(v))),
        );
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: T::ColumnBuilderMut<'_>,
        quantile_disc_data: &QuantileData,
    ) -> Result<()> {
        let value_len = self.value.len();

        let idx = ((value_len - 1) as f64 * quantile_disc_data.levels[0]).floor() as usize;
        if idx >= value_len {
            builder.push_default();
        } else {
            self.value.as_mut_slice().select_nth_unstable(idx);
            let value = self.value.get(idx).unwrap();
            builder.push_item(T::to_scalar_ref(value));
        }

        Ok(())
    }
}

pub fn try_create_aggregate_quantile_disc_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;
    let data_type = arguments[0].clone();
    let levels = get_levels(&params)?;
    with_simple_no_number_mapped_type!(|T| match data_type {
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM_TYPE| match num_type {
                NumberDataType::NUM_TYPE => {
                    if params.len() > 1 {
                        AggregateUnaryFunction::<
                            QuantileState<NumberType<NUM_TYPE>>,
                            NumberType<NUM_TYPE>,
                            ArrayType<NumberType<NUM_TYPE>>,
                        >::with_function_info(
                            display_name,
                            DataType::Array(Box::new(data_type)),
                            QuantileData { levels },
                        )
                        .with_need_drop(true)
                        .finish()
                    } else {
                        AggregateUnaryFunction::<
                            QuantileState<NumberType<NUM_TYPE>>,
                            NumberType<NUM_TYPE>,
                            NumberType<NUM_TYPE>,
                        >::with_function_info(
                            display_name, data_type, QuantileData { levels }
                        )
                        .with_need_drop(true)
                        .finish()
                    }
                }
            })
        }
        DataType::Decimal(size) => {
            with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                DecimalDataKind::DECIMAL => {
                    let data_type = DataType::Decimal(size);
                    if params.len() > 1 {
                        AggregateUnaryFunction::<
                            QuantileState<DecimalType<DECIMAL>>,
                            DecimalType<DECIMAL>,
                            ArrayType<DecimalType<DECIMAL>>,
                        >::with_function_info(
                            display_name,
                            DataType::Array(Box::new(data_type)),
                            QuantileData { levels },
                        )
                        .with_need_drop(true)
                        .finish()
                    } else {
                        AggregateUnaryFunction::<
                            QuantileState<DecimalType<DECIMAL>>,
                            DecimalType<DECIMAL>,
                            DecimalType<DECIMAL>,
                        >::with_function_info(
                            display_name, data_type, QuantileData { levels }
                        )
                        .with_need_drop(true)
                        .finish()
                    }
                }
            })
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, data_type
        ))),
    })
}

pub fn aggregate_quantile_disc_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_quantile_disc_function))
}
