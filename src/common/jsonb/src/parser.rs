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

#[repr(transparent)]
struct Parser<'a> {
    buf: &'a [u8],
}

impl<'a> Parser<'a> {
    fn new(buf: &'a [u8]) -> Parser<'a> {
        Self { buf }
    }

    fn parse(&mut self) -> Result<Value<'a>, Error> {
        let val = self.parse_json_value()?;
        self.skip_unused();
        if !self.buf.is_empty() {
            return Err(Error::UnexpectedTrailingCharacters);
        }
        Ok(val)
    }

    fn parse_json_value(&mut self) -> Result<Value<'a>, Error> {
        self.skip_unused();
        let byte = self.buf.first().ok_or(Error::InvalidEOF)?;
        match byte {
            b'n' => self.parse_json_null(),
            b't' => self.parse_json_true(),
            b'f' => self.parse_json_false(),
            b'0'..=b'9' | b'-' => self.parse_json_number(),
            b'"' => self.parse_json_string(),
            b'[' => self.parse_json_array(),
            b'{' => self.parse_json_object(),
            _ => Err(Error::InvalidValue),
        }
    }

    fn skip_unused(&mut self) {
        let mut idx = 0;
        while let Some(byte) = self.buf.get(idx) {
            if !matches!(byte, b'\n' | b' ' | b'\r' | b'\t') {
                break;
            } else {
                idx += 1;
            }
        }
        self.buf = &self.buf[idx..]
    }

    fn parse_json_null(&mut self) -> Result<Value<'a>, Error> {
        let data: [u8; NULL_LEN] = self
            .buf
            .get(..NULL_LEN)
            .ok_or(Error::InvalidEOF)?
            .try_into()
            .unwrap();
        self.buf = &self.buf[NULL_LEN..];
        if data != [b'n', b'u', b'l', b'l'] {
            return Err(Error::InvalidNullValue);
        }
        Ok(Value::Null)
    }

    fn parse_json_true(&mut self) -> Result<Value<'a>, Error> {
        let data: [u8; TRUE_LEN] = self
            .buf
            .get(..TRUE_LEN)
            .ok_or(Error::InvalidEOF)?
            .try_into()
            .unwrap();
        self.buf = &self.buf[TRUE_LEN..];
        if data != [b't', b'r', b'u', b'e'] {
            return Err(Error::InvalidTrueValue);
        }
        Ok(Value::Bool(true))
    }

    fn parse_json_false(&mut self) -> Result<Value<'a>, Error> {
        let data: [u8; FALSE_LEN] = self
            .buf
            .get(..FALSE_LEN)
            .ok_or(Error::InvalidEOF)?
            .try_into()
            .unwrap();
        self.buf = &self.buf[FALSE_LEN..];
        if data != [b'f', b'a', b'l', b's', b'e'] {
            return Err(Error::InvalidFalseValue);
        }
        Ok(Value::Bool(false))
    }

    fn parse_json_number(&mut self) -> Result<Value<'a>, Error> {
        let mut idx = 0;
        let mut has_point = false;
        let mut has_exponential = false;

        while let Some(byte) = self.buf.get(idx) {
            if idx == 0 && *byte == b'-' {
                idx += 1;
                continue;
            }
            match byte {
                b'0'..=b'9' => {}
                b'.' => {
                    if has_point || has_exponential {
                        return Err(Error::InvalidNumberValue);
                    }
                    has_point = true;
                }
                b'e' | b'E' => {
                    if has_exponential {
                        return Err(Error::InvalidNumberValue);
                    }
                    has_exponential = true;
                    if let Some(next_byte) = self.buf.get(idx + 1) {
                        if *next_byte == b'+' || *next_byte == b'-' {
                            idx += 1;
                        }
                    }
                }
                _ => break,
            }
            idx += 1
        }
        if idx == 0 {
            return Err(Error::InvalidNumberValue);
        }
        let data = &self.buf[..idx];
        self.buf = &self.buf[idx..];

        let s = std::str::from_utf8(data).unwrap();
        match s.parse() {
            Ok(dec) => Ok(Value::Number(dec)),
            Err(_) => Err(Error::InvalidNumberValue),
        }
    }

    fn parse_json_string(&mut self) -> Result<Value<'a>, Error> {
        let byte = self.buf.first().ok_or(Error::InvalidEOF)?;
        if *byte != b'"' {
            return Err(Error::InvalidStringValue);
        }

        let mut idx = 1;
        let mut escapes = 0;
        loop {
            let byte = self.buf.get(idx).ok_or(Error::InvalidEOF)?;
            idx += 1;
            match byte {
                b'\\' => {
                    escapes += 1;
                    let next_byte = self.buf.get(idx).ok_or(Error::InvalidEOF)?;
                    if *next_byte == b'u' {
                        idx += UNICODE_LEN + 1;
                    } else {
                        idx += 1;
                    }
                }
                b'"' => {
                    break;
                }
                _ => {}
            }
        }

        let mut data = &self.buf[1..idx - 1];
        self.buf = &self.buf[idx..];
        let val = if escapes > 0 {
            let mut str_buf = String::with_capacity(idx - 2 - escapes);
            while !data.is_empty() {
                let byte = data[0];
                if byte == b'\\' {
                    data = &data[1..];
                    data = parse_escaped_string(data, &mut str_buf)?;
                } else {
                    str_buf.push(byte as char);
                    data = &data[1..];
                }
            }
            Cow::Owned(str_buf)
        } else {
            std::str::from_utf8(data)
                .map(Cow::Borrowed)
                .map_err(|_| Error::InvalidStringValue)?
        };
        Ok(Value::String(val))
    }

    fn parse_json_array(&mut self) -> Result<Value<'a>, Error> {
        let byte = self.buf.first().ok_or(Error::InvalidEOF)?;
        if *byte != b'[' {
            return Err(Error::InvalidArrayValue);
        }
        self.buf = &self.buf[1..];
        let mut first = true;
        let mut values = Vec::new();
        loop {
            self.skip_unused();
            let byte = self.buf.first().ok_or(Error::InvalidEOF)?;
            if *byte == b']' {
                self.buf = &self.buf[1..];
                break;
            }
            if !first {
                if *byte != b',' {
                    return Err(Error::InvalidArrayValue);
                }
                self.buf = &self.buf[1..];
            }
            first = false;
            let value = self.parse_json_value()?;
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn parse_json_object(&mut self) -> Result<Value<'a>, Error> {
        let byte = self.buf.first().ok_or(Error::InvalidEOF)?;
        if *byte != b'{' {
            return Err(Error::InvalidObjectValue);
        }
        self.buf = &self.buf[1..];
        let mut first = true;
        let mut obj = Object::new();
        loop {
            self.skip_unused();
            let byte = self.buf.first().ok_or(Error::InvalidEOF)?;
            if *byte == b'}' {
                self.buf = &self.buf[1..];
                break;
            }
            if !first {
                if *byte != b',' {
                    return Err(Error::InvalidObjectValue);
                }
                self.buf = &self.buf[1..];
            }
            first = false;
            self.skip_unused();
            let key = self.parse_json_string()?;
            self.skip_unused();
            let byte = self.buf.first().ok_or(Error::InvalidEOF)?;
            if *byte != b':' {
                return Err(Error::InvalidObjectValue);
            }
            self.buf = &self.buf[1..];
            let value = self.parse_json_value()?;

            let k = key.as_str().unwrap();
            obj.insert(k.to_string(), value);
        }
        Ok(Value::Object(obj))
    }
}
