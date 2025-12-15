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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::number::*;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;

use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionRef;
use super::AggregateFunctionSortDesc;
use super::StateAddr;
use super::aggregate_scalar_state::ChangeIf;
use super::aggregate_scalar_state::CmpAny;
use super::aggregate_scalar_state::CmpMax;
use super::aggregate_scalar_state::CmpMin;
use super::aggregate_scalar_state::TYPE_ANY;
use super::aggregate_scalar_state::TYPE_MAX;
use super::aggregate_scalar_state::TYPE_MIN;
use super::assert_binary_arguments;
use super::assert_params;
use super::batch_merge3;
use super::batch_serialize3;
use crate::with_compare_mapped_type;
use crate::with_simple_no_number_mapped_type;

// State for arg_min(arg, val) and arg_max(arg, val)
// A: ValueType for arg.
// V: ValueType for val.
struct ArgMinMaxState<A, V, C>
where
    V: ValueType,
    A: ValueType,
{
    pub data: Option<(V::Scalar, A::Scalar)>,
    _c: PhantomData<C>,
}

impl<A, V, C> ArgMinMaxState<A, V, C>
where
    A: ValueType,
    V: ValueType,
    C: ChangeIf<V>,
{
    fn new() -> Self {
        Self {
            data: None,
            _c: PhantomData,
        }
    }

    fn change(&self, other: &V::ScalarRef<'_>) -> bool {
        match &self.data {
            Some((val, _)) => C::change_if(&V::to_scalar_ref(val), other),
            None => true,
        }
    }

    fn update(&mut self, other: V::ScalarRef<'_>, arg: A::ScalarRef<'_>) {
        self.data = Some((V::to_owned_scalar(other), A::to_owned_scalar(arg)));
    }

    fn add_batch(
        &mut self,
        arg_col: &ColumnView<A>,
        val_col: &ColumnView<V>,
        validity: Option<&Bitmap>,
    ) -> Result<()> {
        let column_len = val_col.len();
        if column_len == 0 {
            return Ok(());
        }
        let acc = if let Some(bit) = validity {
            if bit.null_count() == column_len {
                return Ok(());
            }

            val_col
                .iter()
                .enumerate()
                .zip(bit.iter())
                .filter_map(|(item, valid)| if valid { Some(item) } else { None })
                .reduce(|acc, (row, val)| {
                    if C::change_if(&acc.1, &val) {
                        (row, val)
                    } else {
                        acc
                    }
                })
        } else {
            val_col.iter().enumerate().reduce(|acc, (row, val)| {
                if C::change_if(&acc.1, &val) {
                    (row, val)
                } else {
                    acc
                }
            })
        };

        if let Some((row, val)) = acc {
            if self.change(&val) {
                self.update(val, arg_col.index(row).unwrap())
            }
        }
        Ok(())
    }

    fn merge_from(&mut self, rhs: Self) -> Result<()> {
        if let Some((r_val, r_arg)) = rhs.data {
            if self.change(&V::to_scalar_ref(&r_val)) {
                self.data = Some((r_val, r_arg));
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some((r_val, r_arg)) = &rhs.data {
            if self.change(&V::to_scalar_ref(r_val)) {
                self.data = Some((r_val.to_owned(), r_arg.to_owned()));
            }
        }
        Ok(())
    }

    fn merge_result(&self, builder: &mut ColumnBuilder) -> Result<()> {
        match &self.data {
            Some((_, arg)) => {
                let mut inner = A::downcast_builder(builder);
                inner.push_item(A::to_scalar_ref(arg));
            }
            None => {
                let mut inner = A::downcast_builder(builder);
                inner.push_default();
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateArgMinMaxFunction<A, V, C> {
    display_name: String,
    arg: DataType,
    value: DataType,
    _p: PhantomData<fn(A, V, C)>,
}

impl<A, V, C> AggregateFunction for AggregateArgMinMaxFunction<A, V, C>
where
    A: ValueType,
    V: ValueType,
    C: ChangeIf<V>,
{
    fn name(&self) -> &str {
        "AggregateArgMinMaxFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.arg.clone())
    }

    fn init_state(&self, place: AggrState) {
        place.write(ArgMinMaxState::<A, V, C>::new);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(
            Layout::new::<ArgMinMaxState<A, V, C>>(),
        ));
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state: &mut ArgMinMaxState<A, V, C> = place.get();
        let arg_col = columns[0].downcast::<A>().unwrap();
        let val_col = columns[1].downcast::<V>().unwrap();
        state.add_batch(&arg_col, &val_col, validity)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        let arg_col = columns[0].downcast::<A>().unwrap();
        let val_col = &columns[1].downcast::<V>().unwrap();

        val_col
            .iter()
            .enumerate()
            .zip(places.iter().cloned())
            .for_each(|((row, val), addr)| {
                let state = AggrState::new(addr, loc).get::<ArgMinMaxState<A, V, C>>();
                if state.change(&val) {
                    state.update(val, arg_col.index(row).unwrap())
                }
            });
        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let arg_col = columns[0].downcast::<A>().unwrap();
        let val_col = &columns[1].downcast::<V>().unwrap();
        let state = place.get::<ArgMinMaxState<A, V, C>>();

        let val = unsafe { val_col.index_unchecked(row) };
        if state.change(&val) {
            state.update(val, arg_col.index(row).unwrap())
        }
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        vec![
            DataType::Boolean.into(),
            self.value.clone().into(),
            self.arg.clone().into(),
        ]
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize3::<BooleanType, V, A, ArgMinMaxState<A, V, C>, _>(
            places,
            loc,
            builders,
            |state, (flag_builder, value_builder, arg_builder)| {
                match &state.data {
                    Some((value, arg)) => {
                        flag_builder.push_item(true);
                        value_builder.push_item(V::to_scalar_ref(value));
                        arg_builder.push_item(A::to_scalar_ref(arg));
                    }
                    None => {
                        flag_builder.push_item(false);
                        value_builder.push_default();
                        arg_builder.push_default();
                    }
                }
                Ok(())
            },
        )
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge3::<BooleanType, V, A, ArgMinMaxState<A, V, C>, _>(
            places,
            loc,
            state,
            filter,
            |state, (flag, value, arg)| {
                state.merge_from(ArgMinMaxState::<A, V, C> {
                    data: flag.then_some((V::to_owned_scalar(value), A::to_owned_scalar(arg))),
                    _c: PhantomData,
                })
            },
        )
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<ArgMinMaxState<A, V, C>>();
        let other = rhs.get::<ArgMinMaxState<A, V, C>>();
        state.merge(other)
    }

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let state = place.get::<ArgMinMaxState<A, V, C>>();
        state.merge_result(builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<ArgMinMaxState<A, V, C>>();
        unsafe { std::ptr::drop_in_place(state) };
    }
}

impl<A, V, C> fmt::Display for AggregateArgMinMaxFunction<A, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<A, V, C> AggregateArgMinMaxFunction<A, V, C>
where
    A: ValueType,
    V: ValueType,
    C: ChangeIf<V>,
{
    pub fn try_create(
        display_name: &str,
        arg: DataType,
        value: DataType,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(AggregateArgMinMaxFunction::<A, V, C> {
            display_name: display_name.to_owned(),
            arg,
            value,
            _p: PhantomData,
        }))
    }
}

pub fn try_create_aggregate_arg_minmax_function<const CMP_TYPE: u8>(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    assert_params(display_name, params.len(), 0)?;
    assert_binary_arguments(display_name, arguments.len())?;
    let arg_type = arguments[0].clone();
    let val_type = arguments[1].clone();

    with_compare_mapped_type!(|CMP| match CMP_TYPE {
        CMP => {
            with_simple_no_number_mapped_type!(|ARG_TYPE| match arg_type {
                DataType::ARG_TYPE => {
                    with_simple_no_number_mapped_type!(|VAL_TYPE| match val_type {
                        DataType::VAL_TYPE => {
                            AggregateArgMinMaxFunction::<ARG_TYPE, VAL_TYPE, CMP>::try_create(
                                display_name,
                                arg_type,
                                val_type,
                            )
                        }
                        DataType::Number(num_type) => {
                            with_number_mapped_type!(|NUM| match num_type {
                                NumberDataType::NUM => {
                                    AggregateArgMinMaxFunction::<
                                        ARG_TYPE,
                                        NumberType<NUM>,
                                        CMP,
                                    >::try_create(
                                        display_name,
                                        arg_type,
                                        val_type,
                                    )
                                }
                            })
                        }
                        _ => {
                            AggregateArgMinMaxFunction::<ARG_TYPE, AnyType, CMP>::try_create(
                                display_name,
                                arg_type,
                                val_type,
                            )
                        }
                    })
                }
                DataType::Number(arg_num) => {
                    with_number_mapped_type!(|ARG_NUM| match arg_num {
                        NumberDataType::ARG_NUM => {
                            with_simple_no_number_mapped_type!(|VAL_TYPE| match val_type {
                                DataType::VAL_TYPE => {
                                    AggregateArgMinMaxFunction::<
                                        NumberType<ARG_NUM>,
                                        VAL_TYPE,
                                        CMP,
                                    >::try_create(
                                        display_name,
                                        arg_type,
                                        val_type,
                                    )
                                }
                                DataType::Number(val_num) => {
                                    with_number_mapped_type!(|VAL_NUM| match val_num {
                                        NumberDataType::VAL_NUM => {
                                            AggregateArgMinMaxFunction::<
                                                NumberType<ARG_NUM>,
                                                NumberType<VAL_NUM>,
                                                CMP,
                                            >::try_create(
                                                display_name, arg_type, val_type
                                            )
                                        }
                                    })
                                }
                                _ => {
                                    AggregateArgMinMaxFunction::<
                                        NumberType<ARG_NUM>,
                                        AnyType,
                                        CMP,
                                    >::try_create(
                                        display_name, arg_type,
                                val_type

                                    )
                                }
                            })
                        }
                    })
                }
                _ => {
                    with_simple_no_number_mapped_type!(|VAL_TYPE| match val_type {
                        DataType::VAL_TYPE => {
                            AggregateArgMinMaxFunction::<AnyType, VAL_TYPE, CMP>::try_create(
                                display_name,
                                arg_type,
                                val_type,
                            )
                        }
                        DataType::Number(num_type) => {
                            with_number_mapped_type!(|NUM| match num_type {
                                NumberDataType::NUM => {
                                    AggregateArgMinMaxFunction::<
                                        AnyType,
                                        NumberType<NUM>,
                                        CMP,
                                    >::try_create(
                                        display_name,
                                        arg_type,
                                        val_type,
                                    )
                                }
                            })
                        }
                        _ => {
                            AggregateArgMinMaxFunction::<AnyType, AnyType, CMP>::try_create(
                                display_name,
                                arg_type,
                                val_type,
                            )
                        }
                    })
                }
            })
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare type for aggregate function {display_name} (type number: {})",
            CMP_TYPE
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
