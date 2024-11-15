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

use databend_common_column::binary::BinaryColumn;

use super::WriteOptions;
use crate::compression::binary::compress_binary;
use crate::error::Result;

pub(crate) fn write_binary<W: Write>(
    w: &mut W,
    array: &BinaryColumn,
    validity: Option<Bitmap>,
    write_options: WriteOptions,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    scratch.clear();
    compress_binary(array, validity, scratch, write_options)?;
    w.write_all(scratch.as_slice())?;
    Ok(())
}
