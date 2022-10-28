// Copyright 2022 Datafuse Labs.
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

pub fn write_csv_string(bytes: &[u8], buf: &mut Vec<u8>, quote: u8) {
    buf.push(quote);
    let mut start = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        if byte == quote {
            if start < i {
                buf.extend_from_slice(&bytes[start..i]);
            }
            buf.push(quote);
            buf.push(quote);
            start = i + 1;
        }
    }

    if start != bytes.len() {
        buf.extend_from_slice(&bytes[start..]);
    }
    buf.push(quote);
}
