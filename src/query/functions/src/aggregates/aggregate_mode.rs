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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::AddAssign;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::Result;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::Scalar;

use super::FunctionData;
use super::UnaryState;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregate_function_factory::AggregateFunctionSortDesc;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateUnaryFunction;

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
    T: ValueType + Sync + Send,
    T::Scalar: Ord + Hash + Sync + Send + BorshSerialize + BorshDeserialize,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
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
        builder: &mut T::ColumnBuilder,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        if self.frequency_map.is_empty() {
            T::push_default(builder);
        } else {
            let (key, _) = self
                .frequency_map
                .iter()
                .max_by_key(|&(_, value)| value)
                .unwrap();
            T::push_item(builder, T::to_scalar_ref(key));
        }

        Ok(())
    }
}

pub fn try_create_aggregate_mode_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;

    let data_type = arguments[0].clone();
    with_number_mapped_type!(|NUM| match &data_type {
        DataType::Number(NumberDataType::NUM) => {
            let func = AggregateUnaryFunction::<
                ModeState<NumberType<NUM>>,
                NumberType<NUM>,
                NumberType<NUM>,
            >::try_create(
                display_name, data_type.clone(), params, data_type.clone()
            )
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        DataType::Decimal(DecimalDataType::Decimal128(_)) => {
            let func = AggregateUnaryFunction::<
                ModeState<Decimal128Type>,
                Decimal128Type,
                Decimal128Type,
            >::try_create(
                display_name, data_type.clone(), params, data_type.clone()
            )
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        DataType::Decimal(DecimalDataType::Decimal256(_)) => {
            let func = AggregateUnaryFunction::<
                ModeState<Decimal256Type>,
                Decimal256Type,
                Decimal256Type,
            >::try_create(
                display_name, data_type.clone(), params, data_type.clone()
            )
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        _ => {
            let func = AggregateUnaryFunction::<ModeState<AnyType>, AnyType, AnyType>::try_create(
                display_name,
                data_type.clone(),
                params,
                data_type.clone(),
            )
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
    })
}

pub fn aggregate_mode_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_mode_function))
}
