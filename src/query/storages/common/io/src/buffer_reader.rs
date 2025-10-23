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

use bytes::Buf;
use opendal::Buffer;
use parquet::errors;
use parquet::file::reader::ChunkReader;
use parquet::file::reader::Length;

pub struct BufferReader(pub Buffer);

impl Length for BufferReader {
    fn len(&self) -> u64 {
        self.0.len() as u64
    }
}

impl ChunkReader for BufferReader {
    type T = bytes::buf::Reader<Buffer>;

    fn get_read(&self, start: u64) -> errors::Result<Self::T> {
        let start = start as usize;
        if start > self.0.remaining() {
            return Err(errors::ParquetError::IndexOutOfBound(
                start,
                self.0.remaining(),
            ));
        }
        let mut r = self.0.clone();
        r.advance(start);
        Ok(r.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> errors::Result<bytes::Bytes> {
        let start = start as usize;
        Ok(self.0.slice(start..start + length).to_bytes())
    }
}
