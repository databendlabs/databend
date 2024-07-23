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

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::number::*;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_scalar_state::ChangeIf;
use super::aggregate_scalar_state::CmpAny;
use super::aggregate_scalar_state::CmpMax;
use super::aggregate_scalar_state::CmpMin;
use super::aggregate_scalar_state::TYPE_ANY;
use super::aggregate_scalar_state::TYPE_MAX;
use super::aggregate_scalar_state::TYPE_MIN;
use super::borsh_deserialize_state;
use super::borsh_serialize_state;
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
    BorshSerialize + BorshDeserialize + Send + Sync + 'static
{
    fn new() -> Self;
    fn change(&self, other: V::ScalarRef<'_>) -> bool;
    fn update(&mut self, other: V::ScalarRef<'_>, arg: A::ScalarRef<'_>);
    fn add_batch(
        &mut self,
        data_column: &A::Column,
        column: &V::Column,
        validity: Option<&Bitmap>,
    ) -> Result<()>;

    fn merge_from(&mut self, rhs: Self) -> Result<()>;
    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn merge_result(&self, column: &mut ColumnBuilder) -> Result<()>;
}

#[derive(BorshSerialize, BorshDeserialize)]
struct ArgMinMaxState<A, V, C>
where
    V: ValueType,
    V::Scalar: BorshSerialize + BorshDeserialize,
    A: ValueType,
    A::Scalar: BorshSerialize + BorshDeserialize,
{
    pub data: Option<(V::Scalar, A::Scalar)>,
    #[borsh(skip)]
    _c: PhantomData<C>,
}

impl<A, V, C> AggregateArgMinMaxState<A, V> for ArgMinMaxState<A, V, C>
where
    A: ValueType,
    A::Scalar: Send + Sync + BorshSerialize + BorshDeserialize,
    V: ValueType,
    V::Scalar: Send + Sync + BorshSerialize + BorshDeserialize,
    C: ChangeIf<V> + Default,
{
    fn new() -> Self {
        Self {
            data: None,
            _c: PhantomData,
        }
    }

    fn change(&self, other: V::ScalarRef<'_>) -> bool {
        match &self.data {
            Some((val, _)) => C::change_if(V::to_scalar_ref(val), other),
            None => true,
        }
    }

    fn update(&mut self, other: V::ScalarRef<'_>, arg: A::ScalarRef<'_>) {
        self.data = Some((V::to_owned_scalar(other), A::to_owned_scalar(arg)));
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
        let acc = if let Some(bit) = validity {
            if bit.unset_bits() == column_len {
                return Ok(());
            }

            V::iter_column(val_col)
                .enumerate()
                .zip(bit.iter())
                .filter_map(|(item, valid)| if valid { Some(item) } else { None })
                .reduce(|acc, (row, val)| {
                    if C::change_if(acc.1.clone(), val.clone()) {
                        (row, val)
                    } else {
                        acc
                    }
                })
        } else {
            V::iter_column(val_col)
                .enumerate()
                .reduce(|acc, (row, val)| {
                    if C::change_if(acc.1.clone(), val.clone()) {
                        (row, val)
                    } else {
                        acc
                    }
                })
        };

        if let Some((row, val)) = acc {
            if self.change(val.clone()) {
                self.update(val, A::index_column(arg_col, row).unwrap())
            }
        }
        Ok(())
    }

    fn merge_from(&mut self, rhs: Self) -> Result<()> {
        if let Some((r_val, r_arg)) = rhs.data {
            if self.change(V::to_scalar_ref(&r_val)) {
                self.data = Some((r_val, r_arg));
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some((r_val, r_arg)) = &rhs.data {
            if self.change(V::to_scalar_ref(r_val)) {
                self.data = Some((r_val.to_owned(), r_arg.to_owned()));
            }
        }
        Ok(())
    }

    fn merge_result(&self, builder: &mut ColumnBuilder) -> Result<()> {
        match &self.data {
            Some((_, arg)) => {
                if let Some(inner) = A::try_downcast_builder(builder) {
                    A::push_item(inner, A::to_scalar_ref(arg));
                } else {
                    builder.push(A::upcast_scalar(arg.clone()).as_ref());
                }
            }
            None => {
                if let Some(inner) = A::try_downcast_builder(builder) {
                    A::push_default(inner);
                } else {
                    builder.push_default();
                }
            }
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
        columns: InputColumns,
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
        columns: InputColumns,
        _input_rows: usize,
    ) -> Result<()> {
        let arg_col = A::try_downcast_column(&columns[0]).unwrap();
        let val_col = V::try_downcast_column(&columns[1]).unwrap();
        let val_col_iter = V::iter_column(&val_col);

        val_col_iter
            .enumerate()
            .zip(places.iter())
            .for_each(|((row, val), place)| {
                let addr = place.next(offset);
                let state = addr.get::<State>();
                if state.change(val.clone()) {
                    state.update(val, A::index_column(&arg_col, row).unwrap())
                }
            });
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: InputColumns, row: usize) -> Result<()> {
        let arg_col = A::try_downcast_column(&columns[0]).unwrap();
        let val_col = V::try_downcast_column(&columns[1]).unwrap();
        let state = place.get::<State>();

        let val = unsafe { V::index_column_unchecked(&val_col, row) };
        if state.change(val.clone()) {
            state.update(val, A::index_column(&arg_col, row).unwrap())
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<State>();
        borsh_serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<State>();
        let rhs: State = borsh_deserialize_state(reader)?;
        state.merge_from(rhs)
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
