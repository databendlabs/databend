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
use common_exception::Result;

use crate::scalars::assert_string;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[derive(Clone)]
pub struct FieldFunction {
    display_name: String,
}

impl FieldFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(FieldFunction {
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

impl Function for FieldFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        for arg in args {
            assert_string(*arg)?;
        }
        Ok(u64::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let basic_viewer = Vu8::try_create_viewer(columns[0].column())?;

        let viewers = columns
            .iter()
            .skip(1)
            .map(|c| Vu8::try_create_viewer(c.column()))
            .collect::<Result<Vec<_>>>()?;

        let mut values = Vec::with_capacity(input_rows);
        let mut flag: bool;
        for row in 0..input_rows {
            flag = false;
            let value = basic_viewer.value_at(row);
            for (i, viewer) in viewers.iter().enumerate() {
                if viewer.value_at(row) == value {
                    flag = true;
                    values.push((i + 1) as u64);
                    break;
                }
            }
            if !flag {
                values.push(0);
            }
        }

        Ok(UInt64Column::from_vecs(values).arc())
    }
}

impl fmt::Display for FieldFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
