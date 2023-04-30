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

pub fn quote_ident(ident: &str, quote: char, always_quote: bool) -> String {
    if !need_quote_ident(ident) && !always_quote {
        return ident.to_string();
    }

    let mut s = String::with_capacity(ident.len() + 2);
    s.push(quote);
    for c in ident.chars() {
        if c == quote {
            s.push(quote);
        }
        s.push(c);
    }
    s.push(quote);
    s
}

pub fn unquote_ident(s: &str, quote: char) -> String {
    if s.len() < 2 {
        return s.to_string();
    }

    let mut chars = s.chars().peekable();
    if chars.peek() == Some(&quote) {
        chars.next();
    }

    let mut s = String::with_capacity(s.len());
    while let Some(c) = chars.next() {
        if c == quote {
            let nc = chars.peek();
            if nc == Some(&quote) {
                chars.next();
            } else if nc.is_none() {
                break;
            }
        };
        s.push(c);
    }
    s
}

// in ANSI SQL, it do not need to quote an identifier if the identifier matches
// the following regular expression: [A-Za-z_][A-Za-z0-9_$]*.
//
// There're also two known special cases in Databend which do not requires quoting:
// - "~" is a valid stage name
// - '$' is a valid character in some system functions
fn need_quote_ident(ident: &str) -> bool {
    if ident.is_empty() {
        return true;
    }

    // avoid quote the special identifier "~" which is an available stage name
    if ident == "~" {
        return false;
    }

    let mut chars = ident.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return true;
    }

    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' && c != '$' {
            return true;
        }
    }

    false
}
