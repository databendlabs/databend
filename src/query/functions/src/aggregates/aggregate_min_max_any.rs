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
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use ethnum::i256;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_scalar_state::need_manual_drop_state;
use super::aggregate_scalar_state::ChangeIf;
use super::aggregate_scalar_state::CmpAny;
use super::aggregate_scalar_state::CmpMax;
use super::aggregate_scalar_state::CmpMin;
use super::aggregate_scalar_state::ScalarState;
use super::aggregate_scalar_state::ScalarStateFunc;
use super::aggregate_scalar_state::TYPE_ANY;
use super::aggregate_scalar_state::TYPE_MAX;
use super::aggregate_scalar_state::TYPE_MIN;
use super::deserialize_state;
use super::serialize_state;
use super::StateAddr;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::with_compare_mapped_type;
use crate::with_simple_no_number_mapped_type;

#[derive(Clone)]
pub struct AggregateMinMaxAnyFunction<T, C, State> {
    display_name: String,
    data_type: DataType,
    need_drop: bool,
    _t: PhantomData<T>,
    _c: PhantomData<C>,
    _state: PhantomData<State>,
}

impl<T, C, State> AggregateFunction for AggregateMinMaxAnyFunction<T, C, State>
where
    T: ValueType + Send + Sync,
    C: ChangeIf<T> + Default,
    State: ScalarStateFunc<T>,
{
    fn name(&self) -> &str {
        "AggregateMinMaxAnyFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| State::new());
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<State>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<State>();
        state.add_batch(&column, validity)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        let column_iter = T::iter_column(&column);
        column_iter.zip(places.iter()).for_each(|(v, place)| {
            let addr = place.next(offset);
            let state = addr.get::<State>();
            state.add(Some(v.clone()))
        });
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        let v = T::index_column(&column, row);
        let state = place.get::<State>();
        state.add(v);
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<State>();
        serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<State>();

        let rhs: State = deserialize_state(reader)?;
        state.merge(&rhs)
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<State>();
        let other = rhs.get::<State>();
        state.merge(other)
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<State>();
        state.merge_result(builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        self.need_drop
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<State>();
        std::ptr::drop_in_place(state);
    }
}

impl<T, C, State> fmt::Display for AggregateMinMaxAnyFunction<T, C, State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, C, State> AggregateMinMaxAnyFunction<T, C, State>
where
    T: ValueType + Send + Sync,
    C: ChangeIf<T> + Default,
    State: ScalarStateFunc<T>,
{
    pub fn try_create(
        display_name: &str,
        return_type: DataType,
        need_drop: bool,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateMinMaxAnyFunction::<T, C, State> {
            display_name: display_name.to_string(),
            data_type: return_type,
            need_drop,
            _t: PhantomData,
            _c: PhantomData,
            _state: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

pub fn try_create_aggregate_min_max_any_function<const CMP_TYPE: u8>(
    display_name: &str,
    _params: Vec<Scalar>,
    argument_types: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, argument_types.len())?;
    let mut data_type = argument_types[0].clone();
    let need_drop = need_manual_drop_state(&data_type);

    // null use dummy func, it's already covered in `AggregateNullResultFunction`
    if data_type.is_null() {
        data_type = DataType::String;
    }

    with_compare_mapped_type!(|CMP| match CMP_TYPE {
        CMP => {
            with_simple_no_number_mapped_type!(|T| match data_type {
                DataType::T => {
                    type State = ScalarState<T, CMP>;
                    AggregateMinMaxAnyFunction::<T, CMP, State>::try_create(
                        display_name,
                        data_type,
                        need_drop,
                    )
                }
                DataType::Number(num_type) => {
                    with_number_mapped_type!(|NUM| match num_type {
                        NumberDataType::NUM => {
                            type State = ScalarState<NumberType<NUM>, CMP>;
                            AggregateMinMaxAnyFunction::<NumberType<NUM>, CMP, State>::try_create(
                                display_name,
                                data_type,
                                need_drop,
                            )
                        }
                    })
                }
                DataType::Decimal(DecimalDataType::Decimal128(s)) => {
                    let decimal_size = DecimalSize {
                        precision: s.precision,
                        scale: s.scale,
                    };
                    type State = ScalarState<DecimalType<i128>, CMP>;
                    AggregateMinMaxAnyFunction::<DecimalType<i128>, CMP, State>::try_create(
                        display_name,
                        DataType::Decimal(DecimalDataType::from_size(decimal_size)?),
                        need_drop,
                    )
                }
                DataType::Decimal(DecimalDataType::Decimal256(s)) => {
                    let decimal_size = DecimalSize {
                        precision: s.precision,
                        scale: s.scale,
                    };
                    type State = ScalarState<DecimalType<i256>, CMP>;
                    AggregateMinMaxAnyFunction::<DecimalType<i256>, CMP, State>::try_create(
                        display_name,
                        DataType::Decimal(DecimalDataType::from_size(decimal_size)?),
                        need_drop,
                    )
                }
                _ => {
                    type State = ScalarState<AnyType, CMP>;
                    AggregateMinMaxAnyFunction::<AnyType, CMP, State>::try_create(
                        display_name,
                        data_type,
                        need_drop,
                    )
                }
            })
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare type for aggregate function {} (type number: {})",
            display_name, CMP_TYPE
        ))),
    })
}

pub fn aggregate_min_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_min_max_any_function::<TYPE_MIN>),
        features,
    )
}

pub fn aggregate_max_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_min_max_any_function::<TYPE_MAX>),
        features,
    )
}

pub fn aggregate_any_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_min_max_any_function::<TYPE_ANY>,
    ))
}
