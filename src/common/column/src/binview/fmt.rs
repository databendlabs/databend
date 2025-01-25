// Copyright (c) 2020 Ritchie Vink
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

use crate::binview::BinaryViewColumn;
use crate::binview::BinaryViewColumnGeneric;
use crate::binview::Utf8ViewColumn;
use crate::binview::ViewType;
use crate::fmt::write_vec;

pub fn write_value<'a, T: ViewType + ?Sized, W: Write>(
    array: &'a BinaryViewColumnGeneric<T>,
    index: usize,
    f: &mut W,
) -> Result
where
    &'a T: Debug,
{
    let bytes = array.value(index).to_bytes();
    let writer = |f: &mut W, index| write!(f, "{}", bytes[index]);

    write_vec(f, writer, None, bytes.len(), "None", false)
}

impl Debug for BinaryViewColumn {
    fn fmt(&self, f: &mut Formatter) -> Result {
        let writer = |f: &mut Formatter, index| write_value(self, index, f);
        write!(f, "BinaryViewColumn")?;
        write_vec(f, writer, None, self.len(), "None", false)
    }
}

impl Debug for Utf8ViewColumn {
    fn fmt(&self, f: &mut Formatter) -> Result {
        let writer = |f: &mut Formatter, index| write!(f, "{}", self.value(index));
        write!(f, "StringColumn")?;
        write_vec(f, writer, None, self.len(), "None", false)
    }
}
