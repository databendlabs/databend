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

use std::sync::Arc;

use bstr::ByteSlice;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use futures_util::AsyncReadExt;
use log::debug;

use crate::input_formats::InputContext;
use crate::input_formats::SplitInfo;

pub struct BeyondEndReader {
    pub ctx: Arc<InputContext>,
    pub split_info: Arc<SplitInfo>,
    pub path: String,
    pub record_delimiter_end: u8,
}

impl BeyondEndReader {
    #[async_backtrace::framed]
    pub async fn read(self) -> Result<Vec<u8>> {
        let split_info = &self.split_info;
        if split_info.num_file_splits > 1 && split_info.seq_in_file < split_info.num_file_splits - 1
        {
            debug!("reading beyond end of split {}", split_info);
            // todo(youngsofun): use the avg and max row size
            let mut res = Vec::new();
            let batch_size = 1024;
            let mut buf = vec![0u8; batch_size];
            let operator = self.ctx.source.get_operator()?;
            let offset = split_info.offset as u64;
            let size = split_info.size as u64;
            let offset = offset + size;
            let limit = size as usize;
            let mut reader = operator.reader_with(&self.path).range(offset..).await?;
            let mut num_read_total = 0;
            loop {
                let num_read = reader.read(&mut buf[..]).await?;
                if num_read == 0 {
                    break;
                }
                num_read_total += num_read;

                if let Some(idx) = buf[..num_read].find_byte(self.record_delimiter_end) {
                    if res.is_empty() {
                        buf.truncate(idx);
                        return Ok(buf);
                    } else {
                        res.extend_from_slice(&buf[..idx]);
                        break;
                    }
                } else {
                    if num_read_total > limit {
                        return Err(ErrorCode::BadBytes(format!(
                            "no record delimiter '{}' find in {}[{}..{}]",
                            self.record_delimiter_end,
                            self.path,
                            offset,
                            offset + size,
                        )));
                    }
                    res.extend_from_slice(&buf[..num_read])
                }
            }
            return Ok(res);
        }
        Ok(vec![])
    }
}
