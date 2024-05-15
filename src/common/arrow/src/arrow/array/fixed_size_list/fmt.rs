// Copyright 2020-2022 Jorge C. Leitão
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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result;
use std::fmt::Write;

use super::super::fmt::get_display;
use super::super::fmt::write_vec;
use super::FixedSizeListArray;

pub fn write_value<W: Write>(
    array: &FixedSizeListArray,
    index: usize,
    null: &'static str,
    f: &mut W,
) -> Result {
    let values = array.value(index);
    let writer = |f: &mut W, index| get_display(values.as_ref(), null)(f, index);
    write_vec(f, writer, None, values.len(), null, false)
}

impl Debug for FixedSizeListArray {
    fn fmt(&self, f: &mut Formatter) -> Result {
        let writer = |f: &mut Formatter, index| write_value(self, index, "None", f);

        write!(f, "FixedSizeListArray")?;
        write_vec(f, writer, self.validity(), self.len(), "None", false)
    }
}
