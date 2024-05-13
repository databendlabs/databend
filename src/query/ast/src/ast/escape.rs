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

/// Escape only the specified `chars` in a string.
pub(crate) fn escape_specified(key: &str, chars: &[u8]) -> String {
    let mut new_key = Vec::with_capacity(key.len());

    for char in key.as_bytes() {
        if chars.contains(char) {
            new_key.push(b'%');
            new_key.push(hex(*char / 16));
            new_key.push(hex(*char % 16));
        } else {
            new_key.push(*char);
        }
    }

    // Safe unwrap(): there are no invalid utf char in it.
    String::from_utf8(new_key).unwrap()
}

// Encode 4bit number to [0-9a-f]
fn hex(num: u8) -> u8 {
    match num {
        0..=9 => b'0' + num,
        10..=15 => b'a' + (num - 10),
        unreachable => unreachable!("Unreachable branch num = {}", unreachable),
    }
}
