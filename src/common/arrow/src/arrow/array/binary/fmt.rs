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

use super::super::fmt::write_vec;
use super::BinaryArray;
use crate::arrow::offset::Offset;

pub fn write_value<O: Offset, W: Write>(array: &BinaryArray<O>, index: usize, f: &mut W) -> Result {
    let bytes = array.value(index);
    let writer = |f: &mut W, index| write!(f, "{}", bytes[index]);

    write_vec(f, writer, None, bytes.len(), "None", false)
}

impl<O: Offset> Debug for BinaryArray<O> {
    fn fmt(&self, f: &mut Formatter) -> Result {
        let writer = |f: &mut Formatter, index| write_value(self, index, f);

        let head = if O::IS_LARGE {
            "LargeBinaryArray"
        } else {
            "BinaryArray"
        };
        write!(f, "{head}")?;
        write_vec(f, writer, self.validity(), self.len(), "None", false)
    }
}
