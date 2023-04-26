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

pub fn quote_ident(ident: &str, quote: char) -> String {
    if !need_quote_ident(ident) {
        return ident.to_string();
    }

    let mut s = String::with_capacity(ident.len() + 2);
    for c in ident.chars().peekable() {
        if c == quote {
            s.push(quote);
        }
        s.push(c);
    }
    s
}

pub fn unquote_ident(quoted: &str, quote: char) -> String {
    if quoted.len() < 2 {
        return quoted.to_string();
    }

    let mut chars = quoted.chars().peekable();
    let mut s = String::with_capacity(quoted.len());
    while let Some(c) = chars.next() {
        if c == quote {
            if chars.peek() == Some(&quote) {
                chars.next();
            }
        }
        s.push(c);
    }
    s
}

fn need_quote_ident(ident: &str) -> bool {
    if ident.is_empty() {
        return true;
    }

    let mut chars = ident.chars().peekable();
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return true;
        }
    }
    false
}
