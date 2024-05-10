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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use regex::Regex;

/// Function that escapes special characters in a string.
///
/// All characters except digit, alphabet and '_' are treated as special characters.
/// A special character will be converted into "%num" where num is the hexadecimal form of the character.
///
/// # Example
/// ```
/// let key = "data_bend!!";
/// let new_key = escape_for_key(&key);
/// assert_eq!(Ok("data_bend%21%21".to_string()), new_key);
/// ```
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

    String::from_utf8(new_key)
}

/// The reverse function of escape_for_key.
///
/// # Example
/// ```
/// let key = "data_bend%21%21";
/// let original_key = unescape_for_key(&key);
/// assert_eq!(Ok("data_bend!!".to_string()), original_key);
/// ```
pub fn unescape_for_key(key: &str) -> Result<String, FromUtf8Error> {
    let mut new_key = Vec::with_capacity(key.len());

    fn unhex(num: u8) -> u8 {
        match num {
            b'0'..=b'9' => num - b'0',
            b'a'..=b'f' => num - b'a' + 10,
            unreachable => unreachable!("Unreachable branch num = {}", unreachable),
        }
    }

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

/// Mask a string by "******", but keep `unmask_len` of suffix.
#[inline]
pub fn mask_string(s: &str, unmask_len: usize) -> String {
    if s.len() <= unmask_len {
        s.to_string()
    } else {
        let mut ret = "******".to_string();
        ret.push_str(&s[(s.len() - unmask_len)..]);
        ret
    }
}

/// Returns string after processing escapes.
/// This used for settings string unescape, like unescape format_field_delimiter from `\\x01` to `\x01`.
pub fn unescape_string(escape_str: &str) -> Result<String> {
    enquote::unescape(escape_str, None)
        .map_err(|e| ErrorCode::Internal(format!("unescape:{} error:{:?}", escape_str, e)))
}

pub fn format_byte_size(bytes: usize) -> String {
    if bytes == 0 {
        "0".to_string()
    } else if bytes < 1024 {
        "< 1 KiB".to_string()
    } else {
        convert_byte_size(bytes as f64)
    }
}

pub fn convert_byte_size(num: f64) -> String {
    let negative = if num.is_sign_positive() { "" } else { "-" };
    let num = num.abs();
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];
    if num < 1_f64 {
        return format!("{}{:.02} {}", negative, num, "B");
    }
    let delimiter = 1024_f64;
    let exponent = std::cmp::min(
        (num.ln() / delimiter.ln()).floor() as i32,
        (units.len() - 1) as i32,
    );
    let pretty_bytes = format!("{:.02}", num / delimiter.powi(exponent));
    let unit = units[exponent as usize];
    format!("{}{} {}", negative, pretty_bytes, unit)
}

pub fn convert_number_size(num: f64) -> String {
    if num == 0.0 {
        return String::from("0");
    }

    let negative = if num.is_sign_positive() { "" } else { "-" };
    let num = num.abs();
    let units = [
        "",
        " thousand",
        " million",
        " billion",
        " trillion",
        " quadrillion",
    ];

    if num < 1_f64 {
        return format!("{}{:.2}", negative, num);
    }
    let delimiter = 1000_f64;
    let exponent = std::cmp::min(
        (num.ln() / delimiter.ln()).floor() as i32,
        (units.len() - 1) as i32,
    );
    let pretty_bytes = format!("{:.2}", num / delimiter.powi(exponent))
        .parse::<f64>()
        .unwrap()
        * 1_f64;
    let unit = units[exponent as usize];
    format!("{}{}{}", negative, pretty_bytes, unit)
}

/// Mask the connection info in the sql.
pub fn mask_connection_info(sql: &str) -> String {
    let mut masked_sql = sql.to_string();

    // Regular expression to find the CONNECTION block
    let re_connection = Regex::new(r"CONNECTION\s*=\s*\([^)]+\)").unwrap();

    // Replace the entire CONNECTION block with 'CONNECTION = (***masked***)'
    masked_sql = re_connection
        .replace_all(&masked_sql, "CONNECTION = (***masked***)")
        .to_string();

    masked_sql
}
