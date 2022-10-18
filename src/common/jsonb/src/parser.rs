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

use std::borrow::Cow;

use super::constants::*;
use super::error::Error;
use super::error::ParseErrorCode;
use super::number::Number;
use super::util::parse_escaped_string;
use super::value::Object;
use super::value::Value;

// Parse JSON text to JSONB Value.
// Inspired by `https://github.com/jorgecarleitao/json-deserializer`
// Thanks Jorge Leitao.
pub fn parse_value(buf: &[u8]) -> Result<Value<'_>, Error> {
    let mut parser = Parser::new(buf);
    parser.parse()
}

// used to parse value from storage.
// as value has be parsed, string don't need extra escape.
pub fn decode_value(buf: &[u8]) -> Result<Value<'_>, Error> {
    let mut parser = Parser::new_with_escaped(buf);
    parser.parse()
}

struct Parser<'a> {
    buf: &'a [u8],
    idx: usize,
    escaped: bool,
}

impl<'a> Parser<'a> {
    fn new(buf: &'a [u8]) -> Parser<'a> {
        Self {
            buf,
            idx: 0,
            escaped: false,
        }
    }

    fn new_with_escaped(buf: &'a [u8]) -> Parser<'a> {
        Self {
            buf,
            idx: 0,
            escaped: true,
        }
    }

    fn parse(&mut self) -> Result<Value<'a>, Error> {
        let val = self.parse_json_value()?;
        self.skip_unused();
        if self.idx < self.buf.len() {
            self.step();
            return Err(self.error(ParseErrorCode::UnexpectedTrailingCharacters));
        }
        Ok(val)
    }

    fn parse_json_value(&mut self) -> Result<Value<'a>, Error> {
        self.skip_unused();
        let c = self.next()?;
        match c {
            b'n' => self.parse_json_null(),
            b't' => self.parse_json_true(),
            b'f' => self.parse_json_false(),
            b'0'..=b'9' | b'-' => self.parse_json_number(),
            b'"' => self.parse_json_string(),
            b'[' => self.parse_json_array(),
            b'{' => self.parse_json_object(),
            _ => {
                self.step();
                Err(self.error(ParseErrorCode::ExpectedSomeValue))
            }
        }
    }

    fn next(&mut self) -> Result<&u8, Error> {
        match self.buf.get(self.idx) {
            Some(c) => Ok(c),
            None => Err(self.error(ParseErrorCode::InvalidEOF)),
        }
    }

    fn must_is(&mut self, c: u8) -> Result<(), Error> {
        match self.buf.get(self.idx) {
            Some(v) => {
                self.step();
                if v == &c {
                    Ok(())
                } else {
                    Err(self.error(ParseErrorCode::ExpectedSomeIdent))
                }
            }
            None => Err(self.error(ParseErrorCode::InvalidEOF)),
        }
    }

    fn check_next(&mut self, c: u8) -> bool {
        if self.idx < self.buf.len() {
            let v = self.buf.get(self.idx).unwrap();
            if v == &c {
                return true;
            }
        }
        false
    }

    fn check_next_either(&mut self, c1: u8, c2: u8) -> bool {
        if self.idx < self.buf.len() {
            let v = self.buf.get(self.idx).unwrap();
            if v == &c1 || v == &c2 {
                return true;
            }
        }
        false
    }

    fn check_digit(&mut self) -> bool {
        if self.idx < self.buf.len() {
            let v = self.buf.get(self.idx).unwrap();
            if v.is_ascii_digit() {
                return true;
            }
        }
        false
    }

    fn step_digits(&mut self) -> Result<usize, Error> {
        if self.idx == self.buf.len() {
            return Err(self.error(ParseErrorCode::InvalidEOF));
        }
        let mut len = 0;
        while self.idx < self.buf.len() {
            let c = self.buf.get(self.idx).unwrap();
            if !c.is_ascii_digit() {
                break;
            }
            len += 1;
            self.step();
        }
        Ok(len)
    }

    #[inline]
    fn step(&mut self) {
        self.idx += 1;
    }

    #[inline]
    fn step_by(&mut self, n: usize) {
        self.idx += n;
    }

    fn error(&self, code: ParseErrorCode) -> Error {
        let pos = self.idx;
        Error::Syntax(code, pos)
    }

    fn skip_unused(&mut self) {
        while self.idx < self.buf.len() {
            let c = self.buf.get(self.idx).unwrap();
            if !matches!(c, b'\n' | b' ' | b'\r' | b'\t') {
                break;
            }
            self.step();
        }
    }

    fn parse_json_null(&mut self) -> Result<Value<'a>, Error> {
        let data = [b'n', b'u', b'l', b'l'];
        for v in data.into_iter() {
            self.must_is(v)?;
        }
        Ok(Value::Null)
    }

    fn parse_json_true(&mut self) -> Result<Value<'a>, Error> {
        let data = [b't', b'r', b'u', b'e'];
        for v in data.into_iter() {
            self.must_is(v)?;
        }
        Ok(Value::Bool(true))
    }

    fn parse_json_false(&mut self) -> Result<Value<'a>, Error> {
        let data = [b'f', b'a', b'l', b's', b'e'];
        for v in data.into_iter() {
            self.must_is(v)?;
        }
        Ok(Value::Bool(false))
    }

    fn parse_json_number(&mut self) -> Result<Value<'a>, Error> {
        let start_idx = self.idx;

        let mut has_fraction = false;
        let mut has_exponent = false;
        let mut negative: bool = false;

        if self.check_next(b'-') {
            negative = true;
            self.step();
        }
        if self.check_next(b'0') {
            self.step();
            if self.check_digit() {
                self.step();
                return Err(self.error(ParseErrorCode::InvalidNumberValue));
            }
        } else {
            let len = self.step_digits()?;
            if len == 0 {
                self.step();
                return Err(self.error(ParseErrorCode::InvalidNumberValue));
            }
        }
        if self.check_next(b'.') {
            has_fraction = true;
            self.step();
            let len = self.step_digits()?;
            if len == 0 {
                self.step();
                return Err(self.error(ParseErrorCode::InvalidNumberValue));
            }
        }
        if self.check_next_either(b'E', b'e') {
            has_exponent = true;
            self.step();
            if self.check_next_either(b'+', b'-') {
                self.step();
            }
            let len = self.step_digits()?;
            if len == 0 {
                self.step();
                return Err(self.error(ParseErrorCode::InvalidNumberValue));
            }
        }
        let s = unsafe { std::str::from_utf8_unchecked(&self.buf[start_idx..self.idx]) };

        if !has_fraction && !has_exponent {
            if !negative {
                if let Ok(v) = s.parse::<u64>() {
                    return Ok(Value::Number(Number::UInt64(v)));
                }
            } else if let Ok(v) = s.parse::<i64>() {
                return Ok(Value::Number(Number::Int64(v)));
            }
        }

        match fast_float::parse(s) {
            Ok(v) => Ok(Value::Number(Number::Float64(v))),
            Err(_) => Err(self.error(ParseErrorCode::InvalidNumberValue)),
        }
    }

    fn parse_json_string(&mut self) -> Result<Value<'a>, Error> {
        self.must_is(b'"')?;

        let start_idx = self.idx;
        let mut escapes = 0;
        loop {
            let c = self.next()?;
            if c.is_ascii_control() {
                return Err(self.error(ParseErrorCode::ControlCharacterWhileParsingString));
            }
            match c {
                b'\\' => {
                    self.step();
                    escapes += 1;
                    let next_c = self.next()?;
                    if *next_c == b'u' {
                        self.step_by(UNICODE_LEN + 1);
                    } else {
                        self.step();
                    }
                    continue;
                }
                b'"' => {
                    self.step();
                    break;
                }
                _ => {}
            }
            self.step();
        }

        let mut data = &self.buf[start_idx..self.idx - 1];
        let val = if !self.escaped && escapes > 0 {
            let len = self.idx - 1 - start_idx - escapes;
            let mut idx = start_idx + 1;
            let mut str_buf = String::with_capacity(len);
            while !data.is_empty() {
                idx += 1;
                let byte = data[0];
                if byte == b'\\' {
                    data = &data[1..];
                    data = parse_escaped_string(data, &mut idx, &mut str_buf)?;
                } else {
                    str_buf.push(byte as char);
                    data = &data[1..];
                }
            }
            Cow::Owned(str_buf)
        } else {
            std::str::from_utf8(data)
                .map(Cow::Borrowed)
                .map_err(|_| self.error(ParseErrorCode::InvalidStringValue))?
        };
        Ok(Value::String(val))
    }

    fn parse_json_array(&mut self) -> Result<Value<'a>, Error> {
        self.must_is(b'[')?;

        let mut first = true;
        let mut values = Vec::new();
        loop {
            self.skip_unused();
            let c = self.next()?;
            if *c == b']' {
                self.step();
                break;
            }
            if !first {
                if *c != b',' {
                    return Err(self.error(ParseErrorCode::ExpectedArrayCommaOrEnd));
                }
                self.step();
            }
            first = false;
            let value = self.parse_json_value()?;
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn parse_json_object(&mut self) -> Result<Value<'a>, Error> {
        self.must_is(b'{')?;

        let mut first = true;
        let mut obj = Object::new();
        loop {
            self.skip_unused();
            let c = self.next()?;
            if *c == b'}' {
                self.step();
                break;
            }
            if !first {
                if *c != b',' {
                    return Err(self.error(ParseErrorCode::ExpectedObjectCommaOrEnd));
                }
                self.step();
            }
            first = false;
            let key = self.parse_json_value()?;
            if !key.is_string() {
                return Err(self.error(ParseErrorCode::KeyMustBeAString));
            }
            self.skip_unused();
            let c = self.next()?;
            if *c != b':' {
                return Err(self.error(ParseErrorCode::ExpectedColon));
            }
            self.step();
            let value = self.parse_json_value()?;

            let k = key.as_str().unwrap();
            obj.insert(k.to_string(), value);
        }
        Ok(Value::Object(obj))
    }
}
