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

use super::BinaryColumn;
use crate::fmt::write_vec;

pub fn write_value<W: Write>(array: &BinaryColumn, index: usize, f: &mut W) -> Result {
    let bytes = array.value(index);
    let writer = |f: &mut W, index| write!(f, "{}", bytes[index]);

    write_vec(f, writer, None, bytes.len(), "None", false)
}

impl Debug for BinaryColumn {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("BinaryColumn")
            .field(
                "data",
                &format_args!("0x{}", &hex::encode(self.data().as_slice())),
            )
            .field("offsets", &self.offsets())
            .finish()
    }
}
