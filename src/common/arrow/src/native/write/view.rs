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

use std::io::Write;

use super::WriteOptions;
use crate::arrow::array::BinaryViewArray;
use crate::arrow::error::Result;

pub(crate) fn write_view<W: Write>(
    w: &mut W,
    array: &BinaryViewArray,
    _write_options: WriteOptions,
    _scratch: &mut Vec<u8>,
) -> Result<()> {
    // TODO: adaptive gc and dict by stats
    let array = array.clone().gc();
    let input_buf: &[u8] = bytemuck::cast_slice(array.views().as_slice());
    w.write_all(input_buf)?;

    w.write_all(&(array.data_buffers().len() as u32).to_le_bytes())?;

    for buffer in array.data_buffers().iter() {
        w.write_all(&(buffer.len() as u32).to_le_bytes())?;
        w.write_all(buffer.as_slice())?;
    }
    Ok(())
}
