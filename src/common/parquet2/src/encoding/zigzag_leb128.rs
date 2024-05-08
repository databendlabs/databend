// Copyright [2021] [Jorge C Leitao]
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

use super::uleb128;
use crate::error::Error;

pub fn decode(values: &[u8]) -> Result<(i64, usize), Error> {
    let (u, consumed) = uleb128::decode(values)?;
    Ok(((u >> 1) as i64 ^ -((u & 1) as i64), consumed))
}

pub fn encode(value: i64) -> ([u8; 10], usize) {
    let value = ((value << 1) ^ (value >> (64 - 1))) as u64;
    let mut a = [0u8; 10];
    let produced = uleb128::encode(value, &mut a);
    (a, produced)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode() {
        // see e.g. https://stackoverflow.com/a/2211086/931303
        let cases = vec![
            (0u8, 0i64),
            (1, -1),
            (2, 1),
            (3, -2),
            (4, 2),
            (5, -3),
            (6, 3),
            (7, -4),
            (8, 4),
            (9, -5),
        ];
        for (data, expected) in cases {
            let (result, _) = decode(&[data]).unwrap();
            assert_eq!(result, expected)
        }
    }

    #[test]
    fn test_encode() {
        let cases = vec![
            (0u8, 0i64),
            (1, -1),
            (2, 1),
            (3, -2),
            (4, 2),
            (5, -3),
            (6, 3),
            (7, -4),
            (8, 4),
            (9, -5),
        ];
        for (expected, data) in cases {
            let (result, size) = encode(data);
            assert_eq!(size, 1);
            assert_eq!(result[0], expected)
        }
    }

    #[test]
    fn test_roundtrip() {
        let value = -1001212312;
        let (data, size) = encode(value);
        let (result, _) = decode(&data[..size]).unwrap();
        assert_eq!(value, result);
    }
}
