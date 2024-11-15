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

use super::AsBytes;

pub struct ByteWriter<const EMPTY: bool> {
    buffer: Vec<u8>,
    index: usize,
}

impl<const EMPTY: bool> ByteWriter<EMPTY> {
    pub fn with_capacity(capacity: usize) -> Self {
        ByteWriter {
            buffer: vec![0u8; capacity],
            index: 0,
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.buffer[..self.index]
    }

    pub fn write_value<T: AsBytes>(&mut self, value: T) {
        let data = value.as_bytes();
        if !EMPTY {
            self.buffer[self.index..self.index + data.len()].copy_from_slice(value.as_bytes());
        }
        self.index += data.len();
    }

    pub fn write_value_bytes<T: AsBytes>(&mut self, value: T, bytes: usize) {
        if !EMPTY {
            self.buffer[self.index..self.index + bytes].copy_from_slice(&value.as_bytes()[..bytes]);
        }
        self.index += bytes;
    }
}
