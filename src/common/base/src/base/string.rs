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
use std::sync::LazyLock;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use regex::Regex;
use unicode_segmentation::UnicodeSegmentation;

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
pub fn escape_for_key(key: &str) -> std::result::Result<String, FromUtf8Error> {
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
pub fn unescape_for_key(key: &str) -> std::result::Result<String, FromUtf8Error> {
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
pub fn mask_string(s: &str, _unmask_len: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= 4 {
        "***".to_string()
    } else {
        let head: String = chars[..2].iter().collect();
        let tail: String = chars[chars.len() - 2..].iter().collect();
        format!("{}***{}", head, tail)
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

/// Mask a secret value, showing first 2 and last 2 characters.
/// For values with 4 or fewer characters, fully mask.
pub fn mask_partial(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= 4 {
        "***".to_string()
    } else {
        let head: String = chars[..2].iter().collect();
        let tail: String = chars[chars.len() - 2..].iter().collect();
        format!("{}***{}", head, tail)
    }
}

/// Mask the connection info in the sql.
pub fn mask_connection_info(sql: &str) -> String {
    // Quoted string pattern: handles both '' (SQL doubled) and \' (backslash escaped) quotes
    // Also handles other backslash escapes like \\
    // Supports both single-quoted and double-quoted literals (MySQL/Hive dialects)
    const SINGLE_QUOTED_STR: &str = r"'([^'\\]|''|\\.)*'";
    const DOUBLE_QUOTED_STR: &str = r#""([^"\\]|""|\\.)*""#;

    // Match individual secret key-value pairs
    // Supports both single-quoted and double-quoted values
    static RE_SECRET_KV: LazyLock<Regex> = LazyLock::new(|| {
        let pattern = format!(
            r"(?i)(ACCESS_KEY_ID|ACCESS_KEY_SECRET|SECRET_ACCESS_KEY|AWS_KEY_ID|AWS_KEY_SECRET|AWS_SECRET_KEY|AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_TOKEN|AWS_SESSION_TOKEN|MASTER_KEY|ACCOUNT_KEY|ACCOUNT_NAME|PASSWORD|SECURITY_TOKEN|SESSION_TOKEN|SECRET_ID|SECRET_KEY|CREDENTIAL|EXTERNAL_ID|AWS_EXTERNAL_ID|DELEGATION)\s*=\s*({}|{})",
            SINGLE_QUOTED_STR, DOUBLE_QUOTED_STR
        );
        Regex::new(&pattern).unwrap()
    });

    RE_SECRET_KV
        .replace_all(sql, |caps: &regex::Captures| {
            let key = &caps[1];
            let quoted_val = &caps[2];
            let quote_char = &quoted_val[..1];
            let inner = &quoted_val[1..quoted_val.len() - 1];
            format!(
                "{} = {}{}{}",
                key,
                quote_char,
                mask_partial(inner),
                quote_char
            )
        })
        .to_string()
}

/// Maximum length of the SQL query to be displayed or log.
/// If the query exceeds this length and starts with keywords,
/// it will be truncated and appended with the remaining length.
pub fn short_sql(sql: String, max_length: u64) -> String {
    let keywords = ["INSERT"];

    fn starts_with_any(query: &str, keywords: &[&str]) -> bool {
        keywords
            .iter()
            .any(|&keyword| query.to_uppercase().starts_with(keyword))
    }

    let query = sql.trim_start();

    // Graphemes represent user-perceived characters, which might be composed
    // of multiple Unicode code points.
    // This ensures that we handle complex characters like emojis or
    // accented characters properly.
    if query.graphemes(true).count() > max_length as usize && starts_with_any(query, &keywords) {
        let truncated: String = query.graphemes(true).take(max_length as usize).collect();
        let original_length = query.graphemes(true).count();
        let remaining_length = original_length.saturating_sub(max_length as usize);
        // Append the remaining length indicator
        truncated + &format!("...[{} more characters]", remaining_length)
    } else {
        query.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_connection_eq() {
        let sql = "CREATE STAGE s URL='s3://b' CONNECTION = (ACCESS_KEY_ID = 'akid123' SECRET_ACCESS_KEY = 'secret456')";
        let masked = mask_connection_info(sql);
        assert_eq!(
            masked,
            "CREATE STAGE s URL='s3://b' CONNECTION = (ACCESS_KEY_ID = 'ak***23' SECRET_ACCESS_KEY = 'se***56')"
        );
        assert!(!masked.contains("akid123"));
        assert!(!masked.contains("secret456"));
    }

    #[test]
    fn test_mask_connection_arrow() {
        let sql = "SELECT * FROM 's3://b/data.csv' (CONNECTION => (ACCESS_KEY_ID = 'akid123', SECRET_ACCESS_KEY = 'secret456'))";
        let masked = mask_connection_info(sql);
        assert!(masked.contains("ACCESS_KEY_ID = 'ak***23'"));
        assert!(masked.contains("SECRET_ACCESS_KEY = 'se***56'"));
        assert!(!masked.contains("akid123"));
        assert!(!masked.contains("secret456"));
    }

    #[test]
    fn test_mask_connection_with_parens_in_value() {
        // Value contains ')' inside quotes — should not break the regex
        let sql = "CONNECTION = (PASSWORD = 'p@ss(w)rd' ACCESS_KEY_ID = 'mykey123')";
        let masked = mask_connection_info(sql);
        assert!(masked.contains("PASSWORD = 'p@***rd'"));
        assert!(masked.contains("ACCESS_KEY_ID = 'my***23'"));
        assert!(!masked.contains("p@ss(w)rd"));
        assert!(!masked.contains("mykey123"));
    }

    #[test]
    fn test_mask_connection_with_escaped_quotes() {
        // SQL-style escaped quotes ('') inside values
        let sql = "CONNECTION = (PASSWORD = 'it''s_secret' ACCESS_KEY_ID = 'ak')";
        let masked = mask_connection_info(sql);
        assert!(masked.contains("PASSWORD = 'it***et'"));
        assert!(masked.contains("ACCESS_KEY_ID = '***'"));
        assert!(!masked.contains("it''s_secret"));
    }

    #[test]
    fn test_mask_connection_with_backslash_escaped_quotes() {
        // Backslash-escaped quotes (\') inside values
        let sql = r"CONNECTION = (PASSWORD = 'abc\'tail' ACCESS_KEY_ID = 'mykey')";
        let masked = mask_connection_info(sql);
        assert!(!masked.contains("tail"));
        assert!(!masked.contains("mykey"));
    }

    #[test]
    fn test_mask_bare_key_with_backslash_escaped_quote() {
        // Backslash-escaped quote in a bare key value
        let sql = r"PASSWORD = 'p@ss\'word'";
        let masked = mask_connection_info(sql);
        assert!(!masked.contains("p@ss"));
    }

    #[test]
    fn test_mask_connection_with_backslash_in_value() {
        // Backslash-escaped backslash (\\) inside values
        let sql = r"CONNECTION = (PASSWORD = 'path\\to\\secret' ACCESS_KEY_ID = 'key123')";
        let masked = mask_connection_info(sql);
        assert!(!masked.contains("path"));
        assert!(!masked.contains("key123"));
    }

    #[test]
    fn test_mask_secret_kv_standalone() {
        let sql = "CREATE CONNECTION myconn STORAGE_TYPE = 's3' ACCESS_KEY_ID = 'AKID123' SECRET_ACCESS_KEY = 'secret456'";
        let masked = mask_connection_info(sql);
        assert!(masked.contains("ACCESS_KEY_ID = 'AK***23'"));
        assert!(masked.contains("SECRET_ACCESS_KEY = 'se***56'"));
        assert!(masked.contains("STORAGE_TYPE = 's3'"));
        assert!(!masked.contains("AKID123"));
        assert!(!masked.contains("secret456"));
    }

    #[test]
    fn test_mask_password() {
        let sql = "PASSWORD = 'my_password'";
        let masked = mask_connection_info(sql);
        assert_eq!(masked, "PASSWORD = 'my***rd'");
    }

    #[test]
    fn test_mask_session_token() {
        let sql = "SESSION_TOKEN = 'tok123'";
        let masked = mask_connection_info(sql);
        assert_eq!(masked, "SESSION_TOKEN = 'to***23'");
    }

    #[test]
    fn test_mask_security_token() {
        let sql = "SECURITY_TOKEN = 'sectok'";
        let masked = mask_connection_info(sql);
        assert_eq!(masked, "SECURITY_TOKEN = 'se***ok'");
    }

    #[test]
    fn test_mask_secret_id_and_key() {
        let sql = "SECRET_ID = 'sid' SECRET_KEY = 'skey'";
        let masked = mask_connection_info(sql);
        assert_eq!(masked, "SECRET_ID = '***' SECRET_KEY = '***'");
    }

    #[test]
    fn test_mask_account_key() {
        let sql = "ACCOUNT_KEY = 'azure_key_123'";
        let masked = mask_connection_info(sql);
        assert_eq!(masked, "ACCOUNT_KEY = 'az***23'");
        assert!(!masked.contains("azure_key_123"));
    }

    #[test]
    fn test_no_false_positive_on_token() {
        // Plain 'token' column should NOT be masked (we only match specific prefixed tokens)
        let sql = "WHERE token = 'abc'";
        let masked = mask_connection_info(sql);
        assert_eq!(masked, sql);
    }

    #[test]
    fn test_no_false_positive_on_normal_settings() {
        let sql = "SET max_threads = '8'";
        let masked = mask_connection_info(sql);
        assert_eq!(masked, sql);
    }

    #[test]
    fn test_no_false_positive_on_select() {
        let sql = "SELECT name FROM users WHERE id = '123'";
        let masked = mask_connection_info(sql);
        assert_eq!(masked, sql);
    }

    #[test]
    fn test_mask_case_insensitive() {
        let sql = "connection = (access_key_id = 'akid123' secret_access_key = 'secret456')";
        let masked = mask_connection_info(sql);
        assert!(masked.contains("access_key_id = 'ak***23'"));
        assert!(masked.contains("secret_access_key = 'se***56'"));
    }

    #[test]
    fn test_mask_mixed_connection_and_bare_keys() {
        // Both CONNECTION block keys and bare keys get masked
        let sql =
            "COPY INTO t FROM 's3://b' CONNECTION = (ACCESS_KEY_ID = 'akid123') PASSWORD = 'pw123'";
        let masked = mask_connection_info(sql);
        assert!(masked.contains("ACCESS_KEY_ID = 'ak***23'"));
        assert!(masked.contains("PASSWORD = 'pw***23'"));
        assert!(!masked.contains("akid123"));
        assert!(!masked.contains("pw123"));
    }

    #[test]
    fn test_mask_empty_value() {
        let sql = "PASSWORD = ''";
        let masked = mask_connection_info(sql);
        assert_eq!(masked, "PASSWORD = '***'");
    }

    #[test]
    fn test_mask_no_space_around_eq() {
        let sql = "CONNECTION=(ACCESS_KEY_ID='akid123')";
        let masked = mask_connection_info(sql);
        assert!(masked.contains("ACCESS_KEY_ID = 'ak***23'"));
    }

    #[test]
    fn test_mask_aws_secret_key() {
        let sql = "CREATE CONNECTION c STORAGE_TYPE='s3' AWS_SECRET_KEY='my_secret'";
        let masked = mask_connection_info(sql);
        assert!(masked.contains("AWS_SECRET_KEY = 'my***et'"));
        assert!(masked.contains("STORAGE_TYPE='s3'"));
        assert!(!masked.contains("my_secret"));
    }

    #[test]
    fn test_mask_aws_token() {
        let sql = "CREATE CONNECTION c STORAGE_TYPE='s3' AWS_TOKEN='tok_val99'";
        let masked = mask_connection_info(sql);
        assert!(masked.contains("AWS_TOKEN = 'to***99'"));
        assert!(!masked.contains("tok_val99"));
    }

    #[test]
    fn test_mask_aws_access_key_id() {
        let sql = "AWS_ACCESS_KEY_ID='AKIA1234' AWS_SECRET_ACCESS_KEY='secret_val'";
        let masked = mask_connection_info(sql);
        assert!(masked.contains("AWS_ACCESS_KEY_ID = 'AK***34'"));
        assert!(masked.contains("AWS_SECRET_ACCESS_KEY = 'se***al'"));
        assert!(!masked.contains("AKIA1234"));
        assert!(!masked.contains("secret_val"));
    }

    #[test]
    fn test_mask_aws_session_token() {
        let sql = "AWS_SESSION_TOKEN='session_tok_123'";
        let masked = mask_connection_info(sql);
        assert_eq!(masked, "AWS_SESSION_TOKEN = 'se***23'");
        assert!(!masked.contains("session_tok_123"));
    }

    #[test]
    fn test_mask_gcs_credential() {
        let sql =
            "CREATE CONNECTION c STORAGE_TYPE='gcs' CREDENTIAL='service-account-json-content'";
        let masked = mask_connection_info(sql);
        assert!(masked.contains("CREDENTIAL = 'se***nt'"));
        assert!(masked.contains("STORAGE_TYPE='gcs'"));
        assert!(!masked.contains("service-account-json-content"));
    }

    #[test]
    fn test_mask_double_quoted_password() {
        // MySQL/Hive dialect uses double-quoted string literals
        let sql = r#"CREATE CONNECTION c STORAGE_TYPE="s3" PASSWORD="secret_val""#;
        let masked = mask_connection_info(sql);
        assert!(masked.contains(r#"PASSWORD = "se***al""#));
        assert!(!masked.contains("secret_val"));
    }

    #[test]
    fn test_mask_double_quoted_connection_block() {
        let sql = r#"CONNECTION = (ACCESS_KEY_ID = "akid123" SECRET_ACCESS_KEY = "secret456")"#;
        let masked = mask_connection_info(sql);
        assert!(masked.contains(r#"ACCESS_KEY_ID = "ak***23""#));
        assert!(masked.contains(r#"SECRET_ACCESS_KEY = "se***56""#));
        assert!(!masked.contains("akid123"));
        assert!(!masked.contains("secret456"));
    }

    #[test]
    fn test_mask_double_quoted_with_backslash_escape() {
        let sql = r#"PASSWORD = "pass\"word""#;
        let masked = mask_connection_info(sql);
        assert!(!masked.contains("pass"));
    }
}
