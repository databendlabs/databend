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
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_scalar_state::ChangeIf;
use super::aggregate_scalar_state::CmpAny;
use super::aggregate_scalar_state::CmpMax;
use super::aggregate_scalar_state::CmpMin;
use super::aggregate_scalar_state::TYPE_ANY;
use super::aggregate_scalar_state::TYPE_MAX;
use super::aggregate_scalar_state::TYPE_MIN;
use super::deserialize_state;
use super::serialize_state;
use super::AggregateFunctionRef;
use super::StateAddr;
use crate::aggregates::assert_binary_arguments;
use crate::aggregates::AggregateFunction;
use crate::with_compare_mapped_type;
use crate::with_simple_no_number_mapped_type;

// State for arg_min(arg, val) and arg_max(arg, val)
// A: ValueType for arg.
// V: ValueType for val.
pub trait AggregateArgMinMaxState<A: ValueType, V: ValueType>:
    Serialize + DeserializeOwned + Send + Sync + 'static
{
    fn new() -> Self;
    fn add(&mut self, value: V::ScalarRef<'_>, data: Scalar);
    fn add_batch(
        &mut self,
        data_column: &A::Column,
        column: &V::Column,
        validity: Option<&Bitmap>,
    ) -> Result<()>;

    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn merge_result(&mut self, column: &mut ColumnBuilder) -> Result<()>;
}

#[derive(Serialize, Deserialize)]
struct ArgMinMaxState<A, V, C>
where
    V: ValueType,
    V::Scalar: Serialize + DeserializeOwned,
{
    #[serde(bound(deserialize = "V::Scalar: DeserializeOwned"))]
    pub value: Option<V::Scalar>,
    pub data: Scalar,
    #[serde(skip)]
    _a: PhantomData<A>,
    #[serde(skip)]
    _c: PhantomData<C>,
}

impl<A, V, C> AggregateArgMinMaxState<A, V> for ArgMinMaxState<A, V, C>
where
    A: ValueType + Send + Sync,
    V: ValueType,
    V::Scalar: Send + Sync + Serialize + DeserializeOwned,
    C: ChangeIf<V> + Default,
{
    fn new() -> Self {
        Self {
            value: None,
            data: Scalar::Null,
            _a: PhantomData,
            _c: PhantomData,
        }
    }

    fn add(&mut self, other: V::ScalarRef<'_>, data: Scalar) {
        match &self.value {
            Some(v) => {
                if C::change_if(V::to_scalar_ref(v), other.clone()) {
                    self.value = Some(V::to_owned_scalar(other));
                    self.data = data;
                }
            }
            None => {
                self.value = Some(V::to_owned_scalar(other));
                self.data = data;
            }
        }
    }

    fn add_batch(
        &mut self,
        arg_col: &A::Column,
        val_col: &V::Column,
        validity: Option<&Bitmap>,
    ) -> Result<()> {
        let column_len = V::column_len(val_col);
        if column_len == 0 {
            return Ok(());
        }
        let val_col_iter = V::iter_column(val_col);

        if let Some(bit) = validity {
            if bit.unset_bits() == column_len {
                return Ok(());
            }
            // V::ScalarRef doesn't derive Default, so take the first value as default.
            let mut v = unsafe { V::index_column_unchecked(val_col, 0) };
            let mut has_v = bit.get_bit(0);
            let mut data_value = if has_v {
                let arg = unsafe { A::index_column_unchecked(arg_col, 0) };
                A::upcast_scalar(A::to_owned_scalar(arg))
            } else {
                Scalar::Null
            };

            for ((row, val), valid) in val_col_iter.enumerate().skip(1).zip(bit.iter().skip(1)) {
                if !valid {
                    continue;
                }
                if !has_v {
                    has_v = true;
                    v = val.clone();
                    let arg = unsafe { A::index_column_unchecked(arg_col, row) };
                    data_value = A::upcast_scalar(A::to_owned_scalar(arg));
                } else if C::change_if(v.clone(), val.clone()) {
                    v = val.clone();
                    let arg = unsafe { A::index_column_unchecked(arg_col, row) };
                    data_value = A::upcast_scalar(A::to_owned_scalar(arg));
                }
            }

            if has_v {
                self.add(v, data_value);
            }
        } else {
            let v = val_col_iter.enumerate().reduce(|acc, (row, val)| {
                if C::change_if(acc.1.clone(), val.clone()) {
                    (row, val)
                } else {
                    acc
                }
            });

            if let Some((row, val)) = v {
                let arg = unsafe { A::index_column_unchecked(arg_col, row) };
                self.add(val, A::upcast_scalar(A::to_owned_scalar(arg)));
            }
        };
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(v) = &rhs.value {
            self.add(V::to_scalar_ref(v), rhs.data.clone());
        }
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        if self.value.is_some() {
            if let Some(inner) = A::try_downcast_builder(builder) {
                A::push_item(inner, A::try_downcast_scalar(&self.data.as_ref()).unwrap());
            } else {
                builder.push(self.data.as_ref());
            }
        } else if let Some(inner) = A::try_downcast_builder(builder) {
            A::push_default(inner);
        } else {
            builder.push_default();
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateArgMinMaxFunction<A, V, C, State> {
    display_name: String,
    return_data_type: DataType,
    _a: PhantomData<A>,
    _v: PhantomData<V>,
    _c: PhantomData<C>,
    _state: PhantomData<State>,
}

impl<A, V, C, State> AggregateFunction for AggregateArgMinMaxFunction<A, V, C, State>
where
    A: ValueType + Send + Sync,
    V: ValueType + Send + Sync,
    C: ChangeIf<V> + Default,
    State: AggregateArgMinMaxState<A, V>,
{
    fn name(&self) -> &str {
        "AggregateArgMinMaxFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_data_type.clone())
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
        let state: &mut State = place.get();
        let arg_col = A::try_downcast_column(&columns[0]).unwrap();
        let val_col = V::try_downcast_column(&columns[1]).unwrap();
        state.add_batch(&arg_col, &val_col, validity)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let arg_col = A::try_downcast_column(&columns[0]).unwrap();
        let val_col = V::try_downcast_column(&columns[1]).unwrap();
        let arg_col_iter = A::iter_column(&arg_col);
        let val_col_iter = V::iter_column(&val_col);

        val_col_iter
            .zip(arg_col_iter)
            .zip(places.iter())
            .for_each(|((val, arg), place)| {
                let addr = place.next(offset);
                let state = addr.get::<State>();
                state.add(
                    val.clone(),
                    A::upcast_scalar(A::to_owned_scalar(arg.clone())),
                );
            });
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let arg_col = A::try_downcast_column(&columns[0]).unwrap();
        let val_col = V::try_downcast_column(&columns[1]).unwrap();
        let state = place.get::<State>();

        let arg = unsafe { A::index_column_unchecked(&arg_col, row) };
        let val = unsafe { V::index_column_unchecked(&val_col, row) };
        state.add(val, A::upcast_scalar(A::to_owned_scalar(arg.clone())));
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
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<State>();
        std::ptr::drop_in_place(state);
    }
}

impl<A, V, C, State> fmt::Display for AggregateArgMinMaxFunction<A, V, C, State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<A, V, C, State> AggregateArgMinMaxFunction<A, V, C, State>
where
    A: ValueType + Send + Sync,
    V: ValueType + Send + Sync,
    C: ChangeIf<V> + Default,
    State: AggregateArgMinMaxState<A, V>,
{
    pub fn try_create(
        display_name: &str,
        return_data_type: DataType,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(AggregateArgMinMaxFunction::<A, V, C, State> {
            display_name: display_name.to_owned(),
            return_data_type,
            _a: PhantomData,
            _v: PhantomData,
            _c: PhantomData,
            _state: PhantomData,
        }))
    }
}

pub fn try_create_aggregate_arg_minmax_function<const CMP_TYPE: u8>(
    display_name: &str,
    _params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_binary_arguments(display_name, arguments.len())?;
    let arg_type = arguments[0].clone();
    let val_type = arguments[1].clone();

    with_compare_mapped_type!(|CMP| match CMP_TYPE {
        CMP => {
            with_simple_no_number_mapped_type!(|ARG_TYPE| match arg_type {
                DataType::ARG_TYPE => {
                    with_simple_no_number_mapped_type!(|VAL_TYPE| match val_type {
                        DataType::VAL_TYPE => {
                            type State = ArgMinMaxState<ARG_TYPE, VAL_TYPE, CMP>;
                            AggregateArgMinMaxFunction::<ARG_TYPE, VAL_TYPE, CMP, State>::try_create(
                                display_name,
                                arg_type,
                            )
                        }
                        DataType::Number(num_type) => {
                            with_number_mapped_type!(|NUM| match num_type {
                                NumberDataType::NUM => {
                                    type State = ArgMinMaxState<ARG_TYPE, NumberType<NUM>, CMP>;
                                    AggregateArgMinMaxFunction::<
                                        ARG_TYPE,
                                        NumberType<NUM>,
                                        CMP,
                                        State,
                                    >::try_create(
                                        display_name, arg_type
                                    )
                                }
                            })
                        }
                        _ => {
                            type State = ArgMinMaxState<ARG_TYPE, AnyType, CMP>;
                            AggregateArgMinMaxFunction::<ARG_TYPE, AnyType, CMP, State>::try_create(
                                display_name,
                                arg_type,
                            )
                        }
                    })
                }
                DataType::Number(arg_num) => {
                    with_number_mapped_type!(|ARG_NUM| match arg_num {
                        NumberDataType::ARG_NUM => {
                            with_simple_no_number_mapped_type!(|VAL_TYPE| match val_type {
                                DataType::VAL_TYPE => {
                                    type State = ArgMinMaxState<NumberType<ARG_NUM>, VAL_TYPE, CMP>;
                                    AggregateArgMinMaxFunction::<
                                        NumberType<ARG_NUM>,
                                        VAL_TYPE,
                                        CMP,
                                        State,
                                    >::try_create(
                                        display_name, arg_type
                                    )
                                }
                                DataType::Number(val_num) => {
                                    with_number_mapped_type!(|VAL_NUM| match val_num {
                                        NumberDataType::VAL_NUM => {
                                            type State = ArgMinMaxState<
                                                NumberType<ARG_NUM>,
                                                NumberType<VAL_NUM>,
                                                CMP,
                                            >;
                                            AggregateArgMinMaxFunction::<
                                                NumberType<ARG_NUM>,
                                                NumberType<VAL_NUM>,
                                                CMP,
                                                State,
                                            >::try_create(
                                                display_name, arg_type
                                            )
                                        }
                                    })
                                }
                                _ => {
                                    type State = ArgMinMaxState<NumberType<ARG_NUM>, AnyType, CMP>;
                                    AggregateArgMinMaxFunction::<
                                        NumberType<ARG_NUM>,
                                        AnyType,
                                        CMP,
                                        State,
                                    >::try_create(
                                        display_name, arg_type
                                    )
                                }
                            })
                        }
                    })
                }
                _ => {
                    with_simple_no_number_mapped_type!(|VAL_TYPE| match val_type {
                        DataType::VAL_TYPE => {
                            type State = ArgMinMaxState<AnyType, VAL_TYPE, CMP>;
                            AggregateArgMinMaxFunction::<AnyType, VAL_TYPE, CMP, State>::try_create(
                                display_name,
                                arg_type,
                            )
                        }
                        DataType::Number(num_type) => {
                            with_number_mapped_type!(|NUM| match num_type {
                                NumberDataType::NUM => {
                                    type State = ArgMinMaxState<AnyType, NumberType<NUM>, CMP>;
                                    AggregateArgMinMaxFunction::<
                                        AnyType,
                                        NumberType<NUM>,
                                        CMP,
                                        State,
                                    >::try_create(
                                        display_name, arg_type
                                    )
                                }
                            })
                        }
                        _ => {
                            type State = ArgMinMaxState<AnyType, AnyType, CMP>;
                            AggregateArgMinMaxFunction::<AnyType, AnyType, CMP, State>::try_create(
                                display_name,
                                arg_type,
                            )
                        }
                    })
                }
            })
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare type for aggregate function {} (type number: {})",
            display_name, CMP_TYPE
        ))),
    })
}

pub fn aggregate_arg_min_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_arg_minmax_function::<TYPE_MIN>,
    ))
}

pub fn aggregate_arg_max_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_arg_minmax_function::<TYPE_MAX>,
    ))
}
