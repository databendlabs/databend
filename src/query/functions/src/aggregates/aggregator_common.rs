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

use std::fmt::Display;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use bumpalo::Bump;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;

use super::get_states_layout;
use super::AggrState;
use super::AggregateFunctionFactory;
use super::AggregateFunctionRef;
use crate::aggregates::StatesLayout;

pub fn assert_unary_params<D: Display>(name: D, actual: usize) -> Result<()> {
    if actual != 1 {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have single parameters, but got {}",
            name, actual
        )));
    }
    Ok(())
}

pub fn assert_params<D: Display>(name: D, actual: usize, expected: usize) -> Result<()> {
    if actual != expected {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have {} params, but got {}",
            name, expected, actual
        )));
    }
    Ok(())
}

pub fn assert_variadic_params<D: Display>(
    name: D,
    actual: usize,
    expected: (usize, usize),
) -> Result<()> {
    if actual < expected.0 || actual > expected.1 {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have [{}, {}] params, but got {}",
            name, expected.0, expected.1, actual
        )));
    }
    Ok(())
}

pub fn assert_unary_arguments<D: Display>(name: D, actual: usize) -> Result<()> {
    if actual != 1 {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have single arguments, but got {}",
            name, actual
        )));
    }
    Ok(())
}

pub fn assert_binary_arguments<D: Display>(name: D, actual: usize) -> Result<()> {
    if actual != 2 {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have two arguments, but got {}",
            name, actual
        )));
    }
    Ok(())
}

pub fn assert_arguments<D: Display>(name: D, actual: usize, expected: usize) -> Result<()> {
    if actual != expected {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have {} arguments, but got {}",
            name, expected, actual
        )));
    }
    Ok(())
}

pub fn assert_variadic_arguments<D: Display>(
    name: D,
    actual: usize,
    expected: (usize, usize),
) -> Result<()> {
    if actual < expected.0 || actual > expected.1 {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have [{}, {}] arguments, but got {}",
            name, expected.0, expected.1, actual
        )));
    }
    Ok(())
}

struct EvalAggr {
    addr: StateAddr,
    state_layout: StatesLayout,
    _arena: Bump,
    func: AggregateFunctionRef,
}

impl EvalAggr {
    fn new(func: AggregateFunctionRef) -> Self {
        let funcs = [func];
        let state_layout = get_states_layout(&funcs).unwrap();
        let [func] = funcs;

        let _arena = Bump::new();
        let addr = _arena.alloc_layout(state_layout.layout).into();

        let state = AggrState::new(addr, &state_layout.states_loc[0]);
        func.init_state(&state);

        Self {
            addr,
            state_layout,
            _arena,
            func,
        }
    }
}

impl Drop for EvalAggr {
    fn drop(&mut self) {
        drop_guard(move || {
            if self.func.need_manual_drop_state() {
                unsafe {
                    self.func
                        .drop_state(&AggrState::new(self.addr, &self.state_layout.states_loc[0]));
                }
            }
        })
    }
}

pub fn eval_aggr(
    name: &str,
    params: Vec<Scalar>,
    columns: &[Column],
    rows: usize,
) -> Result<(Column, DataType)> {
    let factory = AggregateFunctionFactory::instance();
    let arguments = columns.iter().map(|x| x.data_type()).collect();

    let func = factory.get(name, params, arguments)?;
    let data_type = func.return_type()?;

    let eval = EvalAggr::new(func.clone());
    let state = AggrState::new(eval.addr, &eval.state_layout.states_loc[0]);
    func.accumulate(&state, columns.into(), None, rows)?;
    let mut builder = ColumnBuilder::with_capacity(&data_type, 1024);
    func.merge_result(&state, &mut builder)?;
    Ok((builder.build(), data_type))
}

#[inline]
pub fn borsh_serialize_state<W: std::io::Write, T: BorshSerialize>(
    writer: &mut W,
    value: &T,
) -> Result<()> {
    borsh::to_writer(writer, value)?;
    Ok(())
}

#[inline]
pub fn borsh_deserialize_state<T: BorshDeserialize>(slice: &mut &[u8]) -> Result<T> {
    Ok(T::deserialize(slice)?)
}
