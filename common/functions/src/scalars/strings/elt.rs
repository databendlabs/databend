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

use std::fmt;

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
use common_exception::ErrorCode;
use common_exception::Result;
use num_traits::AsPrimitive;

use crate::scalars::assert_numeric;
use crate::scalars::assert_string;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[derive(Clone)]
pub struct EltFunction {
    display_name: String,
}

//MySQL ELT() returns the string at the index number specified in the list of arguments. The first argument indicates the index of the string to be retrieved from the list of arguments.
// Note: According to Wikipedia ELT stands for Extract, Load, Transform (ELT), a data manipulation process

impl EltFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(EltFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(2, usize::MAX - 1),
        )
    }
}

impl Function for EltFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if args[0].is_null() {
            return Ok(NullType::arc());
        }

        let arg = remove_nullable(args[0]);
        assert_numeric(&arg)?;

        for arg in args[1..].iter() {
            let arg = remove_nullable(*arg);
            if !arg.is_null() {
                assert_string(&arg)?;
            }
        }

        let dt = Vu8::to_data_type();
        match args.iter().any(|f| f.is_nullable()) {
            true => Ok(wrap_nullable(&dt)),
            false => Ok(dt),
        }
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        if columns[0].data_type().is_null() {
            return Ok(NullColumn::new(input_rows).arc());
        }
        let nullable = columns.iter().any(|c| c.data_type().is_nullable());
        with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
             execute_impl::<$S>(columns, input_rows, nullable)
        },{
            unreachable!()
        })
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}

fn check_range(index: usize, cols: usize) -> Result<()> {
    if index > cols || index == 0 {
        return Err(ErrorCode::IndexOutOfBounds(format!(
            "Index out of bounds, expect: [1, {}], index: {}",
            cols, index
        )));
    }
    Ok(())
}

fn execute_impl<S>(
    columns: &ColumnsWithField,
    input_rows: usize,
    nullable: bool,
) -> Result<ColumnRef>
where
    S: Scalar + AsPrimitive<usize>,
    for<'a> S: Scalar<RefType<'a> = S>,
{
    let viewer = S::try_create_viewer(columns[0].column())?;
    let cols = columns.len() - 1;

    if columns[0].column().is_const() {
        if viewer.null_at(0) {
            return Ok(NullColumn::new(input_rows).arc());
        }
        let index: usize = viewer.value_at(0).as_();
        check_range(index, cols)?;
        let dest_column = columns[index].column();

        return match (nullable, dest_column.data_type().can_inside_nullable()) {
            (true, true) => Ok(NullableColumn::new(
                dest_column.clone(),
                const_validitiess(input_rows, true),
            )
            .arc()),
            _ => Ok(dest_column.clone()),
        };
    }

    let viewers = columns[1..]
        .iter()
        .map(|column| Vu8::try_create_viewer(column.column()))
        .collect::<Result<Vec<_>>>()?;
    match nullable {
        false => {
            let index_c = Series::check_get_scalar::<S>(columns[0].column())?;
            let mut builder = MutableStringColumn::with_capacity(input_rows);
            for (row, index) in index_c.scalar_iter().enumerate() {
                let index: usize = index.as_();
                check_range(index, cols)?;
                builder.append_value(viewers[index - 1].value_at(row));
            }
            Ok(builder.to_column())
        }

        true => {
            let index_viewer = u8::try_create_viewer(columns[0].column())?;
            let mut builder = NullableColumnBuilder::<Vu8>::with_capacity(input_rows);

            for row in 0..input_rows {
                if index_viewer.null_at(row) {
                    builder.append_null();
                } else {
                    let index: usize = index_viewer.value_at(row).as_();
                    check_range(index, cols)?;
                    builder.append(viewers[index].value_at(row), viewers[index].valid_at(row));
                }
            }
            Ok(builder.build(input_rows))
        }
    }
}

impl fmt::Display for EltFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
