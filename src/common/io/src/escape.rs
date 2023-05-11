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

fn hex_safe(n: u8) -> char {
    let n = if n <= 9 { b'0' + n } else { b'A' + n - 10 };
    n as char
}

pub fn escape_string(s: &str) -> String {
    escape_string_with_quote(s, None)
}

pub fn escape_string_with_quote(s: &str, quote: Option<char>) -> String {
    let chars = s.chars().peekable();
    let mut s = String::new();

    for c in chars {
        match c {
            '\t' => s.push_str("\\t"),
            '\r' => s.push_str("\\r"),
            '\n' => s.push_str("\\n"),
            '\'' => s.push_str(if quote == Some('"') { "\'" } else { "\\\'" }),
            '"' => s.push_str(if quote == Some('\'') { "\"" } else { "\\\"" }),
            '\\' => s.push_str("\\\\"),
            '\x00'..='\x1F' => {
                s.push('\\');
                s.push('x');
                s.push(hex_safe(c as u8 / 16));
                s.push(hex_safe(c as u8 % 16));
            }
            _ => s.push(c),
        }
    }
    s
}
