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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use roaring::RoaringTreemap;

pub fn parse_bitmap(buf: &[u8]) -> Result<RoaringTreemap> {
    std::str::from_utf8(buf)
        .map_err(|e| e.to_string())
        .and_then(|s| {
            let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
            let result: Result<Vec<u64>, String> = s
                .split(',')
                .map(|v| v.parse::<u64>().map_err(|e| e.to_string()))
                .collect();
            result
        })
        .map_or_else(
            |_| {
                Err(ErrorCode::BadBytes(format!(
                    "Invalid Bitmap value: {:?}",
                    String::from_utf8_lossy(buf)
                )))
            },
            |v| {
                let rb = RoaringTreemap::from_iter(v.iter());
                Ok(rb)
            },
        )
}

pub fn deserialize_bitmap(buf: &[u8]) -> databend_common_exception::Result<RoaringTreemap> {
    if buf.is_empty() {
        Ok(RoaringTreemap::new())
    } else {
        RoaringTreemap::deserialize_from(buf).map_err(|e| {
            let len = buf.len();
            let msg = format!("fail to decode bitmap from buffer of size {len}: {e}");
            ErrorCode::BadBytes(msg)
        })
    }
}
