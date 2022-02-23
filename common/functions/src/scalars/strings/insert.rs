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
use common_exception::Result;
use num_traits::AsPrimitive;

use crate::scalars::assert_numeric;
use crate::scalars::assert_string;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[inline]
fn apply_insert<'a, S: AsPrimitive<i64>, T: AsPrimitive<i64>>(
    srcstr: &'a [u8],
    pos: S,
    len: T,
    substr: &'a [u8],
    values: &mut Vec<u8>,
) {
    let pos = pos.as_();
    let len = len.as_();

    let sl = srcstr.len() as i64;
    if pos < 1 || pos > sl {
        values.extend_from_slice(srcstr);
    } else {
        let p = pos as usize - 1;
        values.extend_from_slice(&srcstr[0..p]);
        values.extend_from_slice(substr);
        if len >= 0 && pos + len < sl {
            let l = len as usize;
            values.extend_from_slice(&srcstr[p + l..]);
        }
    }
}

#[derive(Clone)]
pub struct InsertFunction {
    display_name: String,
}

impl InsertFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(4))
    }
}

impl Function for InsertFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        assert_string(args[0])?;
        assert_numeric(args[1])?;
        assert_numeric(args[2])?;
        assert_string(args[3])?;

        Ok(Vu8::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let s_viewer = Vu8::try_create_viewer(columns[0].column())?;
        let ss_viewer = Vu8::try_create_viewer(columns[3].column())?;

        let mut values = Vec::with_capacity(s_viewer.value_at(0).len() * input_rows);
        let mut offsets = Vec::with_capacity(input_rows + 1);
        offsets.push(0i64);

        with_match_primitive_type_id!(columns[1].data_type().data_type_id(), |$S| {
            with_match_primitive_type_id!(columns[2].data_type().data_type_id(), |$T| {
                let p_viewer = $S::try_create_viewer(columns[1].column())?;
                let l_viewer = $T::try_create_viewer(columns[2].column())?;

                for row in 0..input_rows {
                    apply_insert(
                        s_viewer.value_at(row),
                        p_viewer.value_at(row),
                        l_viewer.value_at(row),
                        ss_viewer.value_at(row),
                        &mut values,
                    );
                    offsets.push(values.len() as i64);
                }
                let mut builder = MutableStringColumn::from_data(values, offsets);
                Ok(builder.to_column())

            },{
                unreachable!()
            })
        },{
            unreachable!()
        })
    }
}

impl fmt::Display for InsertFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
