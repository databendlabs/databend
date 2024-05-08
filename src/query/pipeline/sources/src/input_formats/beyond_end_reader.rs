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
use futures_util::StreamExt;
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
            let operator = self.ctx.source.get_operator()?;
            let offset = split_info.offset as u64;
            let size = split_info.size as u64;

            // question(xuanwo): Why we need to add size to offset?
            let offset = offset + size;
            let limit = size as usize;
            let mut stream = operator
                .reader(&self.path)
                .await?
                .into_bytes_stream(offset..);

            let mut num_read_total = 0;
            loop {
                let mut bs = stream.next().await.transpose()?.unwrap_or_default();
                if bs.is_empty() {
                    break;
                }
                num_read_total += bs.len();

                if let Some(idx) = bs.find_byte(self.record_delimiter_end) {
                    if res.is_empty() {
                        bs.truncate(idx);
                        return Ok(bs.to_vec());
                    } else {
                        res.extend_from_slice(&bs[..idx]);
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
                    res.extend_from_slice(&bs)
                }
            }
            return Ok(res);
        }
        Ok(vec![])
    }
}
