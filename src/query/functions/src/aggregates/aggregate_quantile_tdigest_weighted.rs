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
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::Arc;

use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::number::*;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::with_unsigned_integer_mapped_type;
use num_traits::AsPrimitive;

use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionRef;
use super::AggregateFunctionSortDesc;
use super::StateAddr;
use super::aggregate_quantile_tdigest::MEDIAN;
use super::aggregate_quantile_tdigest::QUANTILE;
use super::aggregate_quantile_tdigest::QuantileTDigestState;
use super::assert_binary_arguments;
use super::assert_params;
use super::borsh_partial_deserialize;
use super::get_levels;

#[derive(Clone)]
pub struct AggregateQuantileTDigestWeightedFunction<T0, T1> {
    display_name: String,
    return_type: DataType,
    levels: Vec<f64>,
    _arguments: Vec<DataType>,
    _p: PhantomData<fn(T0, T1)>,
}

impl<T0, T1> Display for AggregateQuantileTDigestWeightedFunction<T0, T1>
where
    T0: Number + AsPrimitive<f64>,
    T1: Number + AsPrimitive<u64>,
{
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T0, T1> AggregateFunction for AggregateQuantileTDigestWeightedFunction<T0, T1>
where
    T0: Number + AsPrimitive<f64>,
    T1: Number + AsPrimitive<u64>,
{
    fn name(&self) -> &str {
        "AggregateQuantileDiscFunction"
    }
    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn init_state(&self, place: AggrState) {
        place.write(QuantileTDigestState::new)
    }
    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<QuantileTDigestState>()));
    }
    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = NumberType::<T0>::try_downcast_column(&columns[0].to_column()).unwrap();
        let weighted = NumberType::<T1>::try_downcast_column(&columns[1].to_column()).unwrap();
        let state = place.get::<QuantileTDigestState>();
        match validity {
            Some(bitmap) => {
                for ((value, weight), is_valid) in
                    column.iter().zip(weighted.iter()).zip(bitmap.iter())
                {
                    if is_valid {
                        state.add(value.as_(), Some(weight.as_()));
                    }
                }
            }
            None => {
                for (value, weight) in column.iter().zip(weighted.iter()) {
                    state.add(value.as_(), Some(weight.as_()));
                }
            }
        }

        Ok(())
    }
    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let column = NumberType::<T0>::try_downcast_column(&columns[0].to_column()).unwrap();
        let weighted = NumberType::<T1>::try_downcast_column(&columns[1].to_column()).unwrap();
        let value = unsafe { column.get_unchecked(row) };
        let weight = unsafe { weighted.get_unchecked(row) };

        let state = place.get::<QuantileTDigestState>();
        state.add(value.as_(), Some(weight.as_()));
        Ok(())
    }
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        let column = NumberType::<T0>::try_downcast_column(&columns[0].to_column()).unwrap();
        let weighted = NumberType::<T1>::try_downcast_column(&columns[1].to_column()).unwrap();
        column
            .iter()
            .zip(weighted.iter())
            .zip(places.iter())
            .for_each(|((value, weight), place)| {
                let state = AggrState::new(*place, loc).get::<QuantileTDigestState>();
                state.add(value.as_(), Some(weight.as_()))
            });
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        for place in places {
            let state = AggrState::new(*place, loc).get::<QuantileTDigestState>();
            state.serialize(&mut binary_builder.data)?;
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        let view = state.downcast::<UnaryType<BinaryType>>().unwrap();
        let iter = places.iter().zip(view.iter());

        if let Some(filter) = filter {
            for (place, mut data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v)) {
                let state = AggrState::new(*place, loc).get::<QuantileTDigestState>();
                let mut rhs: QuantileTDigestState = borsh_partial_deserialize(&mut data)?;
                state.merge(&mut rhs)?;
            }
        } else {
            for (place, mut data) in iter {
                let state = AggrState::new(*place, loc).get::<QuantileTDigestState>();
                let mut rhs: QuantileTDigestState = borsh_partial_deserialize(&mut data)?;
                state.merge(&mut rhs)?;
            }
        }
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<QuantileTDigestState>();
        let other = rhs.get::<QuantileTDigestState>();
        state.merge(other)
    }

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let state = place.get::<QuantileTDigestState>();
        state.merge_result(builder, self.levels.clone())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<QuantileTDigestState>();
        std::ptr::drop_in_place(state);
    }
}

impl<T0, T1> AggregateQuantileTDigestWeightedFunction<T0, T1>
where
    T0: Number + AsPrimitive<f64>,
    T1: Number + AsPrimitive<u64>,
{
    fn try_create(
        display_name: &str,
        return_type: DataType,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let levels = get_levels(&params)?;
        let func = AggregateQuantileTDigestWeightedFunction::<T0, T1> {
            display_name: display_name.to_string(),
            return_type,
            levels,
            _arguments: arguments,
            _p: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

pub fn try_create_aggregate_quantile_tdigest_weighted_function<const TYPE: u8>(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    if TYPE == MEDIAN {
        assert_params(display_name, params.len(), 0)?;
    }

    assert_binary_arguments(display_name, arguments.len())?;
    with_number_mapped_type!(|NUM_TYPE_0| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE_0) => {
            let return_type = if params.len() > 1 {
                DataType::Array(Box::new(DataType::Number(NumberDataType::Float64)))
            } else {
                DataType::Number(NumberDataType::Float64)
            };

            with_unsigned_integer_mapped_type!(|NUM_TYPE_1| match &arguments[1] {
                DataType::Number(NumberDataType::NUM_TYPE_1) => {
                    AggregateQuantileTDigestWeightedFunction::<NUM_TYPE_0, NUM_TYPE_1>::try_create(
                        display_name,
                        return_type,
                        params,
                        arguments,
                    )
                }
                _ => Err(ErrorCode::BadDataValueType(format!(
                    "weight just support unsigned integer type, but got '{:?}'",
                    arguments[1]
                ))),
            })
        }

        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} just support numeric type, but got '{:?}'",
            display_name, arguments[0]
        ))),
    })
}

pub fn aggregate_quantile_tdigest_weighted_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_quantile_tdigest_weighted_function::<QUANTILE>,
    ))
}

pub fn aggregate_median_tdigest_weighted_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_quantile_tdigest_weighted_function::<MEDIAN>,
    ))
}
