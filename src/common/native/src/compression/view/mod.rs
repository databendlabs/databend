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

use databend_common_column::binview::BinaryViewColumn;
use databend_common_column::bitmap::Bitmap;
use databend_common_expression::types::Buffer;

use super::basic::CommonCompression;
use crate::compression::integer::compress_integer;
use crate::error::Result;
use crate::write::WriteOptions;

pub fn compress_view(
    col: &BinaryViewColumn,
    validity: Option<Bitmap>,
    buf: &mut Vec<u8>,
    write_options: WriteOptions,
) -> Result<()> {
    // choose compressor
    let col = col.gc_with_dict(validity.clone());
    let view_array: Buffer<i128> = col.views().iter().map(|x| x.as_i128()).collect();
    compress_integer(&view_array, validity, write_options.clone(), buf)?;
    compress_buffers(&col, buf, write_options.default_compression)
}

fn compress_buffers(
    array: &BinaryViewColumn,
    buf: &mut Vec<u8>,
    c: CommonCompression,
) -> Result<()> {
    let codec = c.to_compression() as u8;
    buf.extend_from_slice(&(array.data_buffers().len() as u32).to_le_bytes());
    // let's encode the buffers
    for buffer in array.data_buffers().iter() {
        buf.extend_from_slice(&[codec as u8]);
        let pos = buf.len();
        buf.extend_from_slice(&[0u8; 8]);

        let compressed_size = c.compress(buffer.as_slice(), buf)?;
        buf[pos..pos + 4].copy_from_slice(&(compressed_size as u32).to_le_bytes());
        buf[pos + 4..pos + 8].copy_from_slice(&(buffer.len() as u32).to_le_bytes());
    }
    Ok(())
}
