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

use std::fmt::Display;
use std::fmt::Formatter;
use std::iter::Peekable;
use std::str::FromStr;

use crate::parser::Dialect;

// In ANSI SQL, it does not need to quote an identifier if the identifier matches
// the following regular expression: [A-Za-z_][A-Za-z0-9_$]*.
//
// There are also two known special cases in Databend which do not require quoting:
// - "~" is a valid stage name
// - '$' is a valid character in some system functions
pub fn ident_needs_quote(ident: &str) -> bool {
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

pub fn display_ident(
    name: &str,
    force_quoted_ident: bool,
    quoted_ident_case_sensitive: bool,
    dialect: Dialect,
) -> String {
    // Db-s -> "Db-s" ; dbs -> dbs
    if name.chars().any(|c| c.is_ascii_uppercase()) && quoted_ident_case_sensitive
        || ident_needs_quote(name)
        || force_quoted_ident
    {
        QuotedIdent(name, dialect.default_ident_quote()).to_string()
    } else {
        name.to_string()
    }
}

pub struct QuotedIdent<T: AsRef<str>>(pub T, pub char);

impl FromStr for QuotedIdent<String> {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut chars = s.chars();

        let quote = chars.next().ok_or(())?;

        if chars.next_back().ok_or(())? != quote {
            return Err(());
        }

        let mut output = String::new();

        while let Some(c) = chars.next() {
            if c == quote && chars.next() != Some(quote) {
                return Err(());
            }
            output.push(c);
        }

        Ok(QuotedIdent(output, quote))
    }
}

impl<T: AsRef<str>> Display for QuotedIdent<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let QuotedIdent(ident, quote) = self;
        let ident = ident.as_ref();

        write!(f, "{}", quote)?;
        for c in ident.chars() {
            if c == *quote {
                write!(f, "{}", quote)?;
            }
            write!(f, "{}", c)?;
        }
        write!(f, "{}", quote)
    }
}

pub struct QuotedString<T: AsRef<str>>(pub T, pub char);

impl FromStr for QuotedString<String> {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut chars = s.chars();

        let quote = chars.next().ok_or(())?;

        if chars.next_back().ok_or(())? != quote {
            return Err(());
        }

        let mut chars = chars.peekable();
        let mut output = String::new();

        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    Some('b') => output.push('\u{0008}'),
                    Some('f') => output.push('\u{000C}'),
                    Some('n') => output.push('\n'),
                    Some('r') => output.push('\r'),
                    Some('t') => output.push('\t'),
                    Some('\'') => output.push('\''),
                    Some('"') => output.push('"'),
                    Some('\\') => output.push('\\'),
                    Some('u') => output.push(unescape_unicode(&mut chars).ok_or(())?),
                    Some('x') => output.push(unescape_byte(&mut chars).ok_or(())?),
                    Some(c) if c.is_digit(8) => output.push(unescape_octal(c, &mut chars)),
                    Some(c) => {
                        output.push('\\');
                        output.push(c);
                    }
                    None => {
                        output.push('\\');
                    }
                };
            } else if c == quote {
                if chars.next() != Some(quote) {
                    return Err(());
                }
                output.push(quote);
            } else {
                output.push(c);
            }
        }

        Ok(QuotedString(output, quote))
    }
}

impl<T: AsRef<str>> Display for QuotedString<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let QuotedString(s, quote) = self;
        let s = s.as_ref();

        write!(f, "{}", quote)?;
        for c in s.chars() {
            match c {
                '\t' => write!(f, "\\t")?,
                '\r' => write!(f, "\\r")?,
                '\n' => write!(f, "\\n")?,
                '\\' => write!(f, "\\\\")?,
                '\x00'..='\x1F' => write!(f, "\\x{:02x}", c as u8)?,
                c if c == *quote => write!(f, "\\{}", quote)?,
                _ => write!(f, "{}", c)?,
            }
        }
        write!(f, "{}", quote)
    }
}

fn unescape_unicode(chars: &mut Peekable<impl Iterator<Item = char>>) -> Option<char> {
    let mut code = 0;

    for c in chars.take(4) {
        code = code * 16 + c.to_digit(16)?;
    }

    char::from_u32(code)
}

fn unescape_byte(chars: &mut Peekable<impl Iterator<Item = char>>) -> Option<char> {
    let mut byte = 0;

    for c in chars.take(2) {
        byte = byte * 16 + c.to_digit(16)?;
    }

    char::from_u32(byte)
}

fn unescape_octal(c1: char, chars: &mut Peekable<impl Iterator<Item = char>>) -> char {
    let mut oct = c1.to_digit(8).unwrap();

    while let Some(c) = chars.peek() {
        if let Some(digit) = c.to_digit(8) {
            oct = oct * 8 + digit;
            chars.next();
        } else {
            break;
        }
    }

    char::from_u32(oct).unwrap()
}

pub struct AtString<T: AsRef<str>>(pub T);

impl FromStr for AtString<String> {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut chars = s.chars();

        if chars.next() != Some('@') {
            return Err(());
        }

        let mut output = String::new();

        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    Some(' ') => output.push(' '),
                    Some('\t') => output.push('\t'),
                    Some('\'') => output.push('\''),
                    Some('\"') => output.push('\"'),
                    Some('\\') => output.push('\\'),
                    Some(c) => {
                        output.push('\\');
                        output.push(c);
                    }
                    None => {
                        output.push('\\');
                    }
                }
            } else {
                output.push(c);
            }
        }

        Ok(AtString(output))
    }
}

impl<T: AsRef<str>> Display for AtString<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let AtString(s) = self;
        let s = s.as_ref();

        write!(f, "@")?;
        for c in s.chars() {
            match c {
                ' ' => write!(f, "\\ ")?,
                '\t' => write!(f, "\\t")?,
                '\'' => write!(f, "\\'")?,
                '"' => write!(f, "\\\"")?,
                '\\' => write!(f, "\\\\")?,
                c => write!(f, "{}", c)?,
            }
        }
        Ok(())
    }
}

/// Escape the specified `char`s in a string.
pub struct EscapedString<T: AsRef<str>>(pub T, pub &'static [u8]);

impl<T: AsRef<str>> Display for EscapedString<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let EscapedString(s, chars) = self;
        let s = s.as_ref();

        for c in s.chars() {
            if chars.iter().any(|&x| x as char == c) {
                write!(f, "%{:02x}", c as u8)?;
            } else {
                write!(f, "{}", c)?;
            }
        }

        Ok(())
    }
}
