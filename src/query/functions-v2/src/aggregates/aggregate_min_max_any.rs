// Copyright 2022 Datafuse Labs.
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

use bytes::BytesMut;
use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_expression::types::AnyType;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::ValueType;
use common_expression::with_basic_no_number_mapped_type;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_scalar_state::need_manual_drop_state;
use super::aggregate_scalar_state::ChangeIf;
use super::aggregate_scalar_state::CmpAny;
use super::aggregate_scalar_state::CmpMax;
use super::aggregate_scalar_state::CmpMin;
use super::aggregate_scalar_state::ScalarState;
use super::aggregate_scalar_state::ScalarStateFunc;
use super::StateAddr;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;

const TYPE_ANY: u8 = 0;
const TYPE_MIN: u8 = 1;
const TYPE_MAX: u8 = 2;

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
            state.add(v.clone())
        });
        Ok(())
    }
    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        let v = T::index_column(&column, row);
        if let Some(v) = v {
            let state = place.get::<State>();
            state.add(v)
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<State>();
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<State>();
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = rhs.get::<State>();
        let state = place.get::<State>();
        state.merge(rhs)
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

pub fn try_create_aggregate_min_max_any_function<const TYPE: u8>(
    display_name: &str,
    _params: Vec<Scalar>,
    argument_types: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, argument_types.len())?;
    let data_type = argument_types[0].clone();
    let need_drop = need_manual_drop_state(&data_type);

    with_basic_no_number_mapped_type!(|T| match data_type {
        DataType::T => {
            if TYPE == TYPE_MIN {
                type State = ScalarState<T, CmpMin>;
                AggregateMinMaxAnyFunction::<T, CmpMin, State>::try_create(
                    display_name,
                    data_type,
                    need_drop,
                )
            } else if TYPE == TYPE_MAX {
                type State = ScalarState<T, CmpMax>;
                AggregateMinMaxAnyFunction::<T, CmpMax, State>::try_create(
                    display_name,
                    data_type,
                    need_drop,
                )
            } else {
                type State = ScalarState<T, CmpAny>;
                AggregateMinMaxAnyFunction::<T, CmpAny, State>::try_create(
                    display_name,
                    data_type,
                    need_drop,
                )
            }
        }
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM| match num_type {
                NumberDataType::NUM => {
                    if TYPE == TYPE_MIN {
                        type State = ScalarState<NumberType<NUM>, CmpMin>;
                        AggregateMinMaxAnyFunction::<NumberType<NUM>, CmpMin, State>::try_create(
                            display_name,
                            data_type,
                            need_drop,
                        )
                    } else if TYPE == TYPE_MAX {
                        type State = ScalarState<NumberType<NUM>, CmpMax>;
                        AggregateMinMaxAnyFunction::<NumberType<NUM>, CmpMax, State>::try_create(
                            display_name,
                            data_type,
                            need_drop,
                        )
                    } else {
                        type State = ScalarState<NumberType<NUM>, CmpAny>;
                        AggregateMinMaxAnyFunction::<NumberType<NUM>, CmpAny, State>::try_create(
                            display_name,
                            data_type,
                            need_drop,
                        )
                    }
                }
            })
        }
        _ => {
            if TYPE == TYPE_MIN {
                type State = ScalarState<AnyType, CmpMin>;
                AggregateMinMaxAnyFunction::<AnyType, CmpMin, State>::try_create(
                    display_name,
                    data_type,
                    need_drop,
                )
            } else if TYPE == TYPE_MAX {
                type State = ScalarState<AnyType, CmpMax>;
                AggregateMinMaxAnyFunction::<AnyType, CmpMax, State>::try_create(
                    display_name,
                    data_type,
                    need_drop,
                )
            } else {
                type State = ScalarState<AnyType, CmpAny>;
                AggregateMinMaxAnyFunction::<AnyType, CmpAny, State>::try_create(
                    display_name,
                    data_type,
                    need_drop,
                )
            }
        }
    })
}

pub fn aggregate_min_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_min_max_any_function::<TYPE_MIN>,
    ))
}

pub fn aggregate_max_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_min_max_any_function::<TYPE_MAX>,
    ))
}

pub fn aggregate_any_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_min_max_any_function::<TYPE_ANY>,
    ))
}
