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
use bumpalo::Bump;
use databend_common_base::runtime::drop_guard;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Number;
use databend_common_expression::types::PairType;
use databend_common_expression::types::TernaryType;
use databend_common_expression::types::UnaryType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::F64;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Constant;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;

use super::get_states_layout;
use super::AggrState;
use super::AggregateFunctionFactory;
use super::AggregateFunctionRef;
use super::AggregateFunctionSortDesc;
use super::StatesLayout;
use crate::BUILTIN_FUNCTIONS;

pub(super) fn assert_unary_params<D: Display>(name: D, actual: usize) -> Result<()> {
    if actual != 1 {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have single parameters, but got {}",
            name, actual
        )));
    }
    Ok(())
}

pub(super) fn assert_params<D: Display>(name: D, actual: usize, expected: usize) -> Result<()> {
    if actual != expected {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have {} params, but got {}",
            name, expected, actual
        )));
    }
    Ok(())
}

pub(super) fn assert_variadic_params<D: Display>(
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

pub(super) fn assert_unary_arguments<D: Display>(name: D, actual: usize) -> Result<()> {
    if actual != 1 {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have single arguments, but got {}",
            name, actual
        )));
    }
    Ok(())
}

pub(super) fn assert_binary_arguments<D: Display>(name: D, actual: usize) -> Result<()> {
    if actual != 2 {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have two arguments, but got {}",
            name, actual
        )));
    }
    Ok(())
}

pub(super) fn assert_arguments<D: Display>(name: D, actual: usize, expected: usize) -> Result<()> {
    if actual != expected {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have {} arguments, but got {}",
            name, expected, actual
        )));
    }
    Ok(())
}

pub(super) fn assert_variadic_arguments<D: Display>(
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
        func.init_state(state);

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
                        .drop_state(AggrState::new(self.addr, &self.state_layout.states_loc[0]));
                }
            }
        })
    }
}

pub fn eval_aggr(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    let factory = AggregateFunctionFactory::instance();
    let arguments = entries.iter().map(BlockEntry::data_type).collect();

    let func = factory.get(name, params, arguments, sort_descs)?;
    let data_type = func.return_type()?;

    let eval = EvalAggr::new(func.clone());
    let state = AggrState::new(eval.addr, &eval.state_layout.states_loc[0]);
    func.accumulate(state, entries.into(), None, rows)?;
    let mut builder = ColumnBuilder::with_capacity(&data_type, 1024);
    func.merge_result(state, false, &mut builder)?;
    Ok((builder.build(), data_type))
}

#[inline]
pub(super) fn borsh_partial_deserialize<T: BorshDeserialize>(slice: &mut &[u8]) -> Result<T> {
    Ok(T::deserialize(slice)?)
}

pub(super) fn extract_number_param<T: Number>(param: Scalar) -> Result<T> {
    check_number::<T, usize>(
        None,
        &FunctionContext::default(),
        &Constant {
            span: None,
            data_type: param.as_ref().infer_data_type(),
            scalar: param,
        }
        .into(),
        &BUILTIN_FUNCTIONS,
    )
}

pub(super) fn get_levels(params: &[Scalar]) -> Result<Vec<f64>> {
    let levels = match params {
        [] => vec![0.5f64],
        [param] => {
            let level = extract_number_param::<F64>(param.clone())?.0;
            if !(0.0..=1.0).contains(&level) {
                return Err(ErrorCode::BadDataValueType(format!(
                    "level range between [0, 1], got: {:?}",
                    level
                )));
            }
            vec![level]
        }
        params => {
            let mut levels = Vec::with_capacity(params.len());
            for param in params {
                let level = extract_number_param::<F64>(param.clone())?.0;
                if !(0.0..=1.0).contains(&level) {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "level range between [0, 1], got: {:?} in levels",
                        level
                    )));
                }
                levels.push(level);
            }
            levels
        }
    };
    Ok(levels)
}

pub(super) fn batch_serialize1<A, S, F>(
    places: &[StateAddr],
    loc: &[AggrStateLoc],
    builders: &mut [ColumnBuilder],
    serialize: F,
) -> Result<()>
where
    A: ValueType,
    S: Send + 'static,
    for<'a> F: Fn(&S, &mut A::ColumnBuilderMut<'a>) -> Result<()>,
{
    let [a] = builders else { unreachable!() };
    let mut a = A::downcast_builder(a);

    for place in places {
        let state = AggrState::new(*place, loc).get::<S>();
        serialize(state, &mut a)?;
    }
    Ok(())
}

pub(super) fn batch_serialize2<A, B, S, F>(
    places: &[StateAddr],
    loc: &[AggrStateLoc],
    builders: &mut [ColumnBuilder],
    serialize: F,
) -> Result<()>
where
    A: ValueType,
    B: ValueType,
    S: Send + 'static,
    for<'a> F: Fn(&S, (&mut A::ColumnBuilderMut<'a>, &mut B::ColumnBuilderMut<'a>)) -> Result<()>,
{
    let [a, b] = builders else { unreachable!() };
    let mut a = A::downcast_builder(a);
    let mut b = B::downcast_builder(b);

    for place in places {
        let state = AggrState::new(*place, loc).get::<S>();
        serialize(state, (&mut a, &mut b))?;
    }
    debug_assert_eq!(a.len(), b.len());
    Ok(())
}

pub(super) fn batch_serialize3<A, B, C, S, F>(
    places: &[StateAddr],
    loc: &[AggrStateLoc],
    builders: &mut [ColumnBuilder],
    serialize: F,
) -> Result<()>
where
    A: ValueType,
    B: ValueType,
    C: ValueType,
    S: Send + 'static,
    for<'a> F: Fn(
        &S,
        (
            &mut A::ColumnBuilderMut<'a>,
            &mut B::ColumnBuilderMut<'a>,
            &mut C::ColumnBuilderMut<'a>,
        ),
    ) -> Result<()>,
{
    let [a, b, c] = builders else { unreachable!() };
    let mut a = A::downcast_builder(a);
    let mut b = B::downcast_builder(b);
    let mut c = C::downcast_builder(c);

    for place in places {
        let state = AggrState::new(*place, loc).get::<S>();
        serialize(state, (&mut a, &mut b, &mut c))?;
    }
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(b.len(), c.len());
    Ok(())
}

pub(super) fn batch_merge1<A, S, F>(
    places: &[StateAddr],
    loc: &[AggrStateLoc],
    state: &BlockEntry,
    filter: Option<&Bitmap>,
    merge: F,
) -> Result<()>
where
    A: AccessType,
    S: Send + 'static,
    for<'a> F: Fn(&mut S, A::ScalarRef<'a>) -> Result<()>,
{
    let view = state.downcast::<UnaryType<A>>().unwrap();
    let iter = places.iter().zip(view.iter());
    if let Some(filter) = filter {
        for (place, data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v)) {
            let state = AggrState::new(*place, loc).get::<S>();
            merge(state, data)?;
        }
    } else {
        for (place, data) in iter {
            let state = AggrState::new(*place, loc).get::<S>();
            merge(state, data)?;
        }
    }
    Ok(())
}

pub(super) fn batch_merge2<A, B, S, F>(
    places: &[StateAddr],
    loc: &[AggrStateLoc],
    state: &BlockEntry,
    filter: Option<&Bitmap>,
    merge: F,
) -> Result<()>
where
    A: AccessType,
    B: AccessType,
    S: Send + 'static,
    for<'a> F: Fn(&mut S, (A::ScalarRef<'a>, B::ScalarRef<'a>)) -> Result<()>,
{
    let view = state.downcast::<PairType<A, B>>().unwrap();
    let iter = places.iter().zip(view.iter());
    if let Some(filter) = filter {
        for (place, data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v)) {
            let state = AggrState::new(*place, loc).get::<S>();
            merge(state, data)?;
        }
    } else {
        for (place, data) in iter {
            let state = AggrState::new(*place, loc).get::<S>();
            merge(state, data)?;
        }
    }
    Ok(())
}

pub(super) fn batch_merge3<A, B, C, S, F>(
    places: &[StateAddr],
    loc: &[AggrStateLoc],
    state: &BlockEntry,
    filter: Option<&Bitmap>,
    merge: F,
) -> Result<()>
where
    A: AccessType,
    B: AccessType,
    C: AccessType,
    S: Send + 'static,
    for<'a> F: Fn(&mut S, (A::ScalarRef<'a>, B::ScalarRef<'a>, C::ScalarRef<'a>)) -> Result<()>,
{
    let view = state.downcast::<TernaryType<A, B, C>>().unwrap();
    let iter = places.iter().zip(view.iter());
    if let Some(filter) = filter {
        for (place, data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v)) {
            let state = AggrState::new(*place, loc).get::<S>();
            merge(state, data)?;
        }
    } else {
        for (place, data) in iter {
            let state = AggrState::new(*place, loc).get::<S>();
            merge(state, data)?;
        }
    }
    Ok(())
}
