// Copyright 2021 Datafuse Labs.
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

// put_uvarint encodes a uint64 into buf and returns the number of bytes written.
// If the buffer is too small, put_uvarint will panic.
pub fn put_uvarint(mut buffer: impl AsMut<[u8]>, x: u64) -> usize {
    let mut i = 0;
    let mut mx = x;
    let buf = buffer.as_mut();
    while mx >= 0x80 {
        buf[i] = mx as u8 | 0x80;
        mx >>= 7;
        i += 1;
    }
    buf[i] = mx as u8;
    i + 1
}
