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

//! Mask credential query parameters in URLs quoted by error messages.

use std::borrow::Cow;

/// Case-insensitive names of query parameters that carry credentials.
const SENSITIVE_QUERY_PARAMS: &[&str] = &[
    "webidentitytoken",
    "rolearn",
    "x-amz-security-token",
    "x-amz-signature",
    "x-amz-credential",
    "x-goog-signature",
    "x-goog-credential",
    "securitytoken",
    "security-token",
    "x-oss-security-token",
    "x-oss-signature",
    "signature",
    "q-signature",
    "x-cos-security-token",
    "sig",
    "delegation",
    "token",
    "access_token",
];

const REDACTED: &str = "***";

/// Mask the values of credential query parameters in `msg`.
pub fn redact_url_secrets(msg: &str) -> Cow<'_, str> {
    let bytes = msg.as_bytes();
    let mut out: Option<String> = None;
    // Bytes before `copied` are already handled (copied into `out`).
    let mut copied = 0;
    let mut i = 0;

    while i < bytes.len() {
        // A parameter starts after a query separator, or at the start of a
        // quoted query string (e.g. `Url` debug output).
        if !matches!(bytes[i], b'?' | b'&' | b'"') {
            i += 1;
            continue;
        }

        let name_start = i + 1;
        let name_len = bytes[name_start..]
            .iter()
            .position(|b| !(b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')))
            .unwrap_or(bytes.len() - name_start);
        let name_end = name_start + name_len;
        if bytes.get(name_end) != Some(&b'=') {
            i += 1;
            continue;
        }
        let name = &msg[name_start..name_end];
        if !SENSITIVE_QUERY_PARAMS
            .iter()
            .any(|p| p.eq_ignore_ascii_case(name))
        {
            i += 1;
            continue;
        }

        let value_start = name_end + 1;
        let rest = &bytes[value_start..];
        let scanned = rest
            .iter()
            .position(|b| is_query_value_end(*b))
            .unwrap_or(rest.len());
        let value_len = rest[..scanned]
            .iter()
            .rposition(|b| !is_closing_punct(*b))
            .map_or(0, |p| p + 1);
        let value_end = value_start + value_len;

        if value_len > 0 {
            let out = out.get_or_insert_with(|| String::with_capacity(msg.len()));
            out.push_str(&msg[copied..value_start]);
            out.push_str(REDACTED);
            copied = value_end;
        }
        i = value_end;
    }

    match out {
        None => Cow::Borrowed(msg),
        Some(mut out) => {
            out.push_str(&msg[copied..]);
            Cow::Owned(out)
        }
    }
}

/// The next query parameter, a fragment, or a character a URL can't contain
/// unescaped.
fn is_query_value_end(b: u8) -> bool {
    b.is_ascii_whitespace()
        || b.is_ascii_control()
        || matches!(
            b,
            b'&' | b'#' | b'"' | b'`' | b'[' | b']' | b'{' | b'}' | b'<' | b'>'
        )
}

/// Allowed inside a value, but at its end they close the quote, parenthesis or
/// clause around the URL.
fn is_closing_punct(b: u8) -> bool {
    matches!(b, b'\'' | b')' | b',')
}
