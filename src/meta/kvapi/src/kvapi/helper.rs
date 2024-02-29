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

use std::string::FromUtf8Error;

use crate::kvapi::KeyError;

/// Function that escapes special characters in a string.
///
/// All characters except digit, alphabet and '_' are treated as special characters.
/// A special character will be converted into "%num" where num is the hexadecimal form of the character.
///
/// # Example
/// ```
/// let key = "data_bend!!";
/// let new_key = escape(&key);
/// assert_eq!("data_bend%21%21".to_string(), new_key);
/// ```
pub(crate) fn escape(key: &str) -> String {
    let mut new_key = Vec::with_capacity(key.len());

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

    // Safe unwrap(): there are no invalid utf char in it.
    String::from_utf8(new_key).unwrap()
}

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

/// The reverse function of escape_for_key.
///
/// # Example
/// ```
/// let key = "data_bend%21%21";
/// let original_key = unescape(&key);
/// assert_eq!(Ok("data_bend!!".to_string()), original_key);
/// ```
pub(crate) fn unescape(key: &str) -> Result<String, FromUtf8Error> {
    let mut new_key = Vec::with_capacity(key.len());

    let bytes = key.as_bytes();

    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                // The last byte of the string won't be '%'
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

    String::from_utf8(new_key)
}

/// Unescape only the specified `chars` in a string.
pub(crate) fn unescape_specified(key: &str, chars: &[u8]) -> Result<String, FromUtf8Error> {
    let mut new_key = Vec::with_capacity(key.len());

    let bytes = key.as_bytes();

    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                if index + 2 < bytes.len() {
                    let mut num = unhex(bytes[index + 1]) * 16;
                    num += unhex(bytes[index + 2]);

                    if chars.contains(&num) {
                        new_key.push(num);
                    } else {
                        // Not specified char, do not unescape
                        new_key.push(bytes[index]);
                        new_key.push(bytes[index + 1]);
                        new_key.push(bytes[index + 2]);
                    }

                    index += 3;
                } else {
                    // Incomplete
                    new_key.push(b'%');
                    index += 1;
                }
            }
            other => {
                new_key.push(other);
                index += 1;
            }
        }
    }

    String::from_utf8(new_key)
}

/// Decode a string into u64 id.
pub(crate) fn decode_id(s: &str) -> Result<u64, KeyError> {
    let id = s.parse::<u64>().map_err(|e| KeyError::InvalidId {
        s: s.to_string(),
        reason: e.to_string(),
    })?;

    Ok(id)
}

/// Encode 4bit number to [0-9a-f]
fn hex(num: u8) -> u8 {
    match num {
        0..=9 => b'0' + num,
        10..=15 => b'a' + (num - 10),
        unreachable => unreachable!("Unreachable branch num = {}", unreachable),
    }
}

/// Convert a hexadecimal char to a 4bit word.
fn unhex(num: u8) -> u8 {
    match num {
        b'0'..=b'9' => num - b'0',
        b'a'..=b'f' => num - b'a' + 10,
        unreachable => unreachable!("Unreachable branch num = {}", unreachable),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_escape_specified() {
        let inp = "a/'b'/%";

        let out = super::escape_specified(inp, &[b'%', b'\'']);
        assert_eq!("a/%27b%27/%25", out);

        let out = super::escape_specified(inp, &[b'\'']);
        assert_eq!("a/%27b%27/%", out);

        let out = super::escape_specified(inp, &[]);
        assert_eq!("a/'b'/%", out);
    }

    #[test]
    fn test_unescape_specified() {
        let inp = "a/%27b%27/%25";

        let out = super::unescape_specified(inp, &[b'%', b'\'']).unwrap();
        assert_eq!("a/'b'/%", out);

        let out = super::unescape_specified(inp, &[b'\'']).unwrap();
        assert_eq!("a/'b'/%25", out);

        let out = super::unescape_specified(inp, &[b'%']).unwrap();
        assert_eq!("a/%27b%27/%", out);

        let out = super::unescape_specified(inp, &[]).unwrap();
        assert_eq!("a/%27b%27/%25", out);

        let out = super::unescape_specified("a/%25/%2", &[b'%']).unwrap();
        assert_eq!("a/%/%2", out, "incomplete input wont be unescaped");
    }
}
