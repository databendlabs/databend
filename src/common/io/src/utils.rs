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

use std::cmp;

use common_exception::ErrorCode;
use common_exception::Result;

pub fn convert_byte_size(num: f64) -> String {
    let negative = if num.is_sign_positive() { "" } else { "-" };
    let num = num.abs();
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];
    if num < 1_f64 {
        return format!("{}{:.02} {}", negative, num, "B");
    }
    let delimiter = 1024_f64;
    let exponent = cmp::min(
        (num.ln() / delimiter.ln()).floor() as i32,
        (units.len() - 1) as i32,
    );
    let pretty_bytes = format!("{:.02}", num / delimiter.powi(exponent));
    let unit = units[exponent as usize];
    format!("{}{} {}", negative, pretty_bytes, unit)
}

pub fn convert_number_size(num: f64) -> String {
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
        return format!("{}{}", negative, num);
    }
    let delimiter = 1000_f64;
    let exponent = cmp::min(
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

/// bincode seralize_into wrap with optimized config
#[inline]
pub fn serialize_into_buf<W: std::io::Write, T: serde::Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<()> {
    bincode::serde::encode_into_std_write(value, writer, bincode::config::standard())?;
    Ok(())
}

/// bincode deserialize_from wrap with optimized config
#[inline]
pub fn deserialize_from_slice<T: serde::de::DeserializeOwned>(slice: &mut &[u8]) -> Result<T> {
    let (value, bytes_read) =
        bincode::serde::decode_from_slice(slice, bincode::config::standard())?;
    *slice = &slice[bytes_read..];
    Ok(value)
}

/// Returns string after processing escapes.
/// This used for settings string unescape, like unescape format_field_delimiter from `\\x01` to `\x01`.
pub fn unescape_string(escape_str: &str) -> Result<String> {
    enquote::unescape(escape_str, None)
        .map_err(|e| ErrorCode::Internal(format!("unescape:{} error:{:?}", escape_str, e)))
}

/// Mask a string by "******", but keep `unmask_len` of suffix.
///
/// Copied from `common-base` so that we don't need to depend on it.
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
