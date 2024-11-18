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

use databend_common_column::binview::BinaryViewColumn;
use databend_common_column::binview::View;

use super::WriteOptions;
use crate::error::Result;

pub(crate) fn write_view<W: Write>(
    w: &mut W,
    array: &BinaryViewColumn,
    write_options: WriteOptions,
    buf: &mut Vec<u8>,
) -> Result<()> {
    // TODO: adaptive gc and dict by stats
    let array = array.clone().gc();
    let c = write_options.default_compression;
    let codec = c.to_compression();

    let total_size = array.len() * std::mem::size_of::<View>()
        + array.data_buffers().iter().map(|x| x.len()).sum::<usize>();
    w.write_all(&[codec as u8])?;
    w.write_all(&(total_size as u32).to_le_bytes())?;
    w.write_all(&(total_size as u32).to_le_bytes())?;

    let input_buf: &[u8] = bytemuck::cast_slice(array.views().as_slice());
    w.write_all(input_buf)?;
    w.write_all(&(array.data_buffers().len() as u32).to_le_bytes())?;

    for buffer in array.data_buffers().iter() {
        buf.clear();
        let pos = buf.len();
        w.write_all(&[codec as u8])?;
        buf.extend_from_slice(&[0u8; 8]);

        let compressed_size = c.compress(buffer.as_slice(), buf)?;
        buf[pos..pos + 4].copy_from_slice(&(compressed_size as u32).to_le_bytes());
        buf[pos + 4..pos + 8].copy_from_slice(&(buffer.len() as u32).to_le_bytes());
        w.write_all(buf.as_slice())?;
        buf.clear();
    }
    Ok(())
}
