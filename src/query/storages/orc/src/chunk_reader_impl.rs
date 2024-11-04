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

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::AsyncRead;
use futures_util::AsyncReadExt;
use futures_util::FutureExt;
use opendal::Operator;
use orc_rust::reader::AsyncChunkReader;

pub struct OrcChunkReader {
    pub operator: Operator,
    pub path: String,
    pub size: u64,
}

impl AsyncChunkReader for OrcChunkReader {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        async move { Ok(self.size) }.boxed()
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        let range = offset_from_start..(offset_from_start + length);
        async move {
            let buf = self
                .operator
                .read_with(&self.path)
                .range(range)
                .chunk(8 << 20)
                .concurrent(4)
                .await?;
            Ok(buf.to_bytes())
        }
        .boxed()
    }
}

#[async_backtrace::framed]
pub async fn read_full<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
) -> std::io::Result<usize> {
    let mut buf = &mut buf[0..];
    let mut n = 0;
    while !buf.is_empty() {
        let read = reader.read(buf).await?;
        if read == 0 {
            break;
        }
        n += read;
        buf = &mut buf[read..]
    }
    Ok(n)
}
