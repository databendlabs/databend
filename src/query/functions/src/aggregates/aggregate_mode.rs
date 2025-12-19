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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::ops::AddAssign;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use databend_common_expression::types::*;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;

use super::AggregateFunctionDescription;
use super::AggregateFunctionSortDesc;
use super::AggregateUnaryFunction;
use super::SerializeInfo;
use super::StateSerde;
use super::StateSerdeItem;
use super::UnaryState;
use super::assert_params;
use super::assert_unary_arguments;
use super::batch_merge1;

#[derive(BorshSerialize, BorshDeserialize)]
pub struct ModeState<T>
where
    T: ValueType,
    T::Scalar: Ord + Hash + BorshSerialize + BorshDeserialize,
{
    pub frequency_map: HashMap<T::Scalar, u64>,
}

impl<T> Default for ModeState<T>
where
    T: ValueType,
    T::Scalar: Ord + Hash + BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        ModeState::<T> {
            frequency_map: HashMap::new(),
        }
    }
}

impl<T> UnaryState<T, T> for ModeState<T>
where
    T: ValueType,
    T::Scalar: Ord + Hash + BorshSerialize + BorshDeserialize,
{
    fn add(&mut self, other: T::ScalarRef<'_>, _: &Self::FunctionInfo) -> Result<()> {
        let other = T::to_owned_scalar(other);
        match self.frequency_map.entry(other) {
            Entry::Occupied(o) => *o.into_mut() += 1,
            Entry::Vacant(v) => {
                v.insert(1);
            }
        };

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        for (key, value) in rhs.frequency_map.iter() {
            match self.frequency_map.get_mut(key) {
                Some(entry) => entry.add_assign(value),
                None => {
                    self.frequency_map.insert(key.clone(), *value);
                }
            }
        }

        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: T::ColumnBuilderMut<'_>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        if self.frequency_map.is_empty() {
            builder.push_default();
        } else {
            let (key, _) = self
                .frequency_map
                .iter()
                .max_by_key(|&(_, value)| value)
                .unwrap();
            builder.push_item(T::to_scalar_ref(key));
        }

        Ok(())
    }
}

impl<T> StateSerde for ModeState<T>
where
    T: ValueType,
    T::Scalar: Ord + Hash + BorshSerialize + BorshDeserialize,
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

pub fn try_create_aggregate_mode_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    assert_params(display_name, params.len(), 0)?;
    assert_unary_arguments(display_name, arguments.len())?;

    let data_type = arguments[0].clone();
    with_number_mapped_type!(|NUM| match &data_type {
        DataType::Number(NumberDataType::NUM) => {
            AggregateUnaryFunction::<
                ModeState<NumberType<NUM>>,
                NumberType<NUM>,
                NumberType<NUM>,
            >::new(display_name, data_type.clone())
            .with_need_drop(true)
            .finish()
        }
        DataType::Decimal(size) => {
            with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                DecimalDataKind::DECIMAL => {
                    AggregateUnaryFunction::<
                        ModeState<DecimalType<DECIMAL>>,
                        DecimalType<DECIMAL>,
                        DecimalType<DECIMAL>,
                    >::new(display_name, data_type.clone())
                    .with_need_drop(true)
                    .finish()
                }
            })
        }
        _ => {
            AggregateUnaryFunction::<ModeState<AnyType>, AnyType, AnyType>::new(
                display_name,
                data_type.clone(),
            )
            .with_need_drop(true)
            .finish()
        }
    })
}

pub fn aggregate_mode_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_mode_function))
}
