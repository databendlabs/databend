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

#[inline]
fn apply<'a>(str: &'a [u8], from: &'a [u8], to: &'a [u8], buf: &mut Vec<u8>) {
    if from.is_empty() || from == to {
        buf.extend_from_slice(str);
        return;
    }
    let mut skip = 0;
    for (p, w) in str.windows(from.len()).enumerate() {
        if w == from {
            buf.extend_from_slice(to);
            skip = from.len();
        } else if p + w.len() == str.len() {
            buf.extend_from_slice(w);
        } else if skip > 1 {
            skip -= 1;
        } else {
            buf.extend_from_slice(&w[0..1]);
        }
    }
}

#[derive(Clone)]
pub struct ReplaceFunction {
    display_name: String,
}

impl ReplaceFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(3))
    }
}

impl Function for ReplaceFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        for arg in args {
            assert_string(*arg)?;
        }
        Ok(Vu8::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let view0 = Vu8::try_create_viewer(columns[0].column())?;
        let view1 = Vu8::try_create_viewer(columns[1].column())?;
        let view2 = Vu8::try_create_viewer(columns[2].column())?;

        let mut values = Vec::with_capacity(view0.value_at(0).len() * input_rows);
        let mut offsets = Vec::with_capacity(input_rows + 1);
        offsets.push(0i64);

        for row in 0..input_rows {
            apply(
                view0.value_at(row),
                view1.value_at(row),
                view2.value_at(row),
                &mut values,
            );
            offsets.push(values.len() as i64);
        }
        let mut builder = MutableStringColumn::from_data(values, offsets);
        Ok(builder.to_column())
    }
}

impl fmt::Display for ReplaceFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
