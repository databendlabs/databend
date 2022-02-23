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
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
use common_exception::Result;
use num_traits::AsPrimitive;

use crate::scalars::assert_numeric;
use crate::scalars::assert_string;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

pub type LeftPadFunction = PadFunction<LeftPad>;
pub type RightPadFunction = PadFunction<RightPad>;

pub trait PadOperator: Send + Sync + Clone + Default + 'static {
    fn apply<'a>(&'a mut self, str: &'a [u8], l: usize, pad: &'a [u8]) -> &'a [u8];
}

#[derive(Clone, Default)]
pub struct LeftPad {
    buff: Vec<u8>,
}

impl PadOperator for LeftPad {
    #[inline]
    fn apply<'a>(&'a mut self, str: &'a [u8], l: usize, pad: &'a [u8]) -> &'a [u8] {
        self.buff.clear();
        if l != 0 {
            if l > str.len() {
                let l = l - str.len();
                while self.buff.len() < l {
                    if self.buff.len() + pad.len() <= l {
                        self.buff.extend_from_slice(pad);
                    } else {
                        self.buff.extend_from_slice(&pad[0..l - self.buff.len()])
                    }
                }
                self.buff.extend_from_slice(str);
            } else {
                self.buff.extend_from_slice(&str[0..l]);
            }
        }
        &self.buff
    }
}

#[derive(Clone, Default)]
pub struct RightPad {
    buff: Vec<u8>,
}

impl PadOperator for RightPad {
    #[inline]
    fn apply<'a>(&'a mut self, str: &'a [u8], l: usize, pad: &'a [u8]) -> &'a [u8] {
        self.buff.clear();
        if l != 0 {
            if l > str.len() {
                self.buff.extend_from_slice(str);
                while self.buff.len() < l {
                    if self.buff.len() + pad.len() <= l {
                        self.buff.extend_from_slice(pad);
                    } else {
                        self.buff.extend_from_slice(&pad[0..l - self.buff.len()])
                    }
                }
            } else {
                self.buff.extend_from_slice(&str[0..l]);
            }
        }
        &self.buff
    }
}

#[derive(Clone)]
pub struct PadFunction<T> {
    display_name: String,
    _marker: PhantomData<T>,
}

impl<T: PadOperator> PadFunction<T> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            _marker: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(3))
    }
}

impl<T: PadOperator> Function for PadFunction<T> {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        assert_string(args[0])?;
        assert_numeric(args[1])?;
        assert_string(args[2])?;
        Ok(Vu8::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let col1 = Vu8::try_create_viewer(columns[0].column())?;
        let col3 = Vu8::try_create_viewer(columns[2].column())?;
        let mut t = T::default();
        let mut builder = MutableStringColumn::with_capacity(input_rows);

        with_match_primitive_type_id!(columns[1].data_type().data_type_id(), |$S| {
            let col2 = $S::try_create_viewer(columns[1].column())?;
            col1.iter()
                .zip(col2.iter())
                .zip(col3.iter())
                .for_each(|((str, l), pad)| builder.append_value(t.apply(str, l.as_(), pad)));
            Ok(builder.to_column())
        },{
            unreachable!()
        })
    }
}

impl<F> fmt::Display for PadFunction<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.display_name)
    }
}
