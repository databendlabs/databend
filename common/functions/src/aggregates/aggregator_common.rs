// Copyright 2021 Datafuse Labs.
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

use bumpalo::Bump;
use common_datavalues::ColumnRef;
use common_datavalues::ColumnWithField;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;

use super::AggregateFunctionFactory;

pub fn assert_unary_params<D: Display>(name: D, actual: usize) -> Result<()> {
    if actual != 1 {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have single parameters, but got {}",
            name, actual
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

pub fn eval_aggr(
    name: &str,
    params: Vec<DataValue>,
    columns: &[ColumnWithField],
    rows: usize,
) -> Result<ColumnRef> {
    let factory = AggregateFunctionFactory::instance();
    let arguments = columns.iter().map(|c| c.field().clone()).collect();
    let cols: Vec<ColumnRef> = columns.iter().map(|c| c.column().clone()).collect();

    let func = factory.get(name, params, arguments)?;

    let arena = Bump::new();
    let place = arena.alloc_layout(func.state_layout());
    let addr = place.into();
    func.init_state(addr);
    func.accumulate(addr, &cols, None, rows)?;

    let data_type = func.return_type()?;
    let mut builder = data_type.create_mutable(1024);
    func.merge_result(addr, builder.as_mut())?;

    Ok(builder.to_column())
}
