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

use std::string::FromUtf8Error;

pub fn escape_for_key(key: &str) -> Result<String, FromUtf8Error> {
    let mut new_key = Vec::with_capacity(key.len());

    fn hex(num: u8) -> u8 {
        match num {
            0..=9 => b'0' + num,
            10..=15 => b'a' + (num - 10),
            unreachable => unreachable!("Unreachable branch num = {}", unreachable),
        }
    }

    for char in key.as_bytes() {
        match char {
            b'0'..=b'9' => new_key.push(*char),
            b'_' | b'a'..=b'z' | b'A'..=b'Z' => new_key.push(*char),
            _other => {
                new_key.push(b'%');
                new_key.push(hex(*char / 16));
                new_key.push(hex(*char % 16));
            }
        }
    }

    Ok(String::from_utf8(new_key)?)
}

pub fn unescape_for_key(key: &str) -> Result<String, FromUtf8Error> {
    let mut new_key = Vec::with_capacity(key.len());

    fn unhex(num: u8) -> u8 {
        match num {
            b'0'..=b'9' => num - b'0',
            b'a'..=b'f' => num - b'a',
            unreachable => unreachable!("Unreachable branch num = {}", unreachable),
        }
    }

    let bytes = key.as_bytes();

    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                let mut num = unhex(bytes[index + 1]) * 16;
                num += unhex(bytes[index + 2]);
                new_key.push(num);
                index += 3;
            }
            other => {
                new_key.push(other);
                index += 1;
            }
        }
    }

    Ok(String::from_utf8(new_key)?)
}
