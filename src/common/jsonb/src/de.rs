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
use std::collections::VecDeque;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use decimal_rs::Decimal;

use super::constants::*;
use super::error::*;
use super::jentry::JEntry;
use super::parser::parse_value;
use super::value::Object;
use super::value::Value;

/// The binary `JSONB` contains three parts, `Header`, `JEntry` and `RawData`.
/// This structure can be nested. Each group of structures starts with a `Header`.
/// The upper-level `Value` will store the `Header` length or offset of
/// the lower-level `Value`.

/// `Header` stores the type of the `Value`, include `Array`, `Object` and `Scalar`,
/// `Scalar` has only one `Value`, and a corresponding `JEntry`.
/// `Array` and `Object` are nested type, they have multiple lower-level `Values`.
/// So the `Header` also stores the number of lower-level `Values`.

/// `JEntry` stores the types of `Scalar Value`, including `Null`, `True`, `False`,
/// `Number`, `String` and `Container`. They have three different decode methods.
/// 1. `Null`, `True` and `False` can be obtained by `JEntry`, no extra work required.
/// 2. `Number` and `String` has related `RawData`, `JEntry` store the length
/// or offset of this data, the `Value` can be read out and then decoded.
/// 3. `Container` is actually a nested `Array` or `Object` with the same structure,
/// `JEntry` store the length or offset of the lower-level `Header`,
/// from where the same decode process can begin.

/// `RawData` is the encoded `Value`.
/// `Number` is a variable-length `Decimal`, store both int and float value.
/// `String` is the original string, can be borrowed directly without extra decode.
/// `Array` and `Object` is a lower-level encoded `JSONB` value.
/// The upper-level doesn't care about the specific content.
/// Decode can be executed recursively.

/// Decode `JSONB` Value from binary bytes.
pub fn from_slice(buf: &[u8]) -> Result<Value<'_>, Error> {
    let mut decoder = Decoder::new(buf);
    match decoder.decode() {
        Ok(value) => Ok(value),
        // for compatible with the first version of `JSON` text, parse it again
        Err(_) => parse_value(buf),
    }
}

#[repr(transparent)]
pub struct Decoder<'a> {
    buf: &'a [u8],
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Decoder<'a> {
        Self { buf }
    }

    pub fn decode(&mut self) -> Result<Value<'a>, Error> {
        // Valid `JSONB` Value has at least one `Header`
        if self.buf.len() < 4 {
            return Err(Error::InvalidJsonb);
        }
        let value = self.decode_jsonb()?;
        Ok(value)
    }

    // Read value type from the `Header`
    // `Scalar` has one `JEntry`
    // `Array` and `Object` store the numbers of elements
    fn decode_jsonb(&mut self) -> Result<Value<'a>, Error> {
        let container_header = self.buf.read_u32::<BigEndian>()?;

        match container_header & CONTAINER_HEADER_TYPE_MASK {
            SCALAR_CONTAINER_TAG => {
                let encoded = self.buf.read_u32::<BigEndian>()?;
                let jentry = JEntry::decode_jentry(encoded);
                self.decode_scalar(jentry)
            }
            ARRAY_CONTAINER_TAG => self.decode_array(container_header),
            OBJECT_CONTAINER_TAG => self.decode_object(container_header),
            _ => Err(Error::InvalidJsonbHeader),
        }
    }

    // Decode `Value` based on the `JEntry`
    // `Null` and `Boolean` don't need to read extra data
    // `Number` and `String` `JEntry` stores the length or offset of the data,
    // read them and decode to the `Value`
    // `Array` and `Object` need to read nested data from the lower-level `Header`
    fn decode_scalar(&mut self, jentry: JEntry) -> Result<Value<'a>, Error> {
        match jentry.type_code {
            NULL_TAG => Ok(Value::Null),
            TRUE_TAG => Ok(Value::Bool(true)),
            FALSE_TAG => Ok(Value::Bool(false)),
            STRING_TAG => {
                let offset = jentry.length as usize;
                let s = std::str::from_utf8(&self.buf[..offset]).unwrap();
                self.buf = &self.buf[offset..];
                Ok(Value::String(Cow::Borrowed(s)))
            }
            NUMBER_TAG => {
                let offset = jentry.length as usize;
                let d = Decimal::decode(&self.buf[..offset]);
                self.buf = &self.buf[offset..];
                Ok(Value::Number(d))
            }
            CONTAINER_TAG => self.decode_jsonb(),
            _ => Err(Error::InvalidJsonbJEntry),
        }
    }

    // Decode the numbers of values from the `Header`,
    // then read all `JEntries`, finally decode the `Value` by `JEntry`
    fn decode_array(&mut self, container_header: u32) -> Result<Value<'a>, Error> {
        let length = (container_header & CONTAINER_HEADER_LEN_MASK) as usize;
        let jentries = self.decode_jentries(length)?;
        let mut values: Vec<Value> = Vec::with_capacity(length);
        // decode all values
        for jentry in jentries.into_iter() {
            let value = self.decode_scalar(jentry)?;
            values.push(value);
        }

        let value = Value::Array(values);
        Ok(value)
    }

    // The basic process is the same as that of `Array`
    // but first decode the keys and then decode the values
    fn decode_object(&mut self, container_header: u32) -> Result<Value<'a>, Error> {
        let length = (container_header & CONTAINER_HEADER_LEN_MASK) as usize;
        let mut jentries = self.decode_jentries(length * 2)?;

        let mut keys: VecDeque<Value> = VecDeque::with_capacity(length);
        // decode all keys first
        for _ in 0..length {
            let jentry = jentries.pop_front().unwrap();
            let key = self.decode_scalar(jentry)?;
            keys.push_back(key);
        }

        let mut obj = Object::new();
        // decode all values
        for _ in 0..length {
            let key = keys.pop_front().unwrap();
            let k = key.as_str().unwrap();
            let jentry = jentries.pop_front().unwrap();
            let value = self.decode_scalar(jentry)?;
            obj.insert(k.to_string(), value);
        }

        let value = Value::Object(obj);
        Ok(value)
    }

    // Decode `JEntries` for `Array` and `Object`
    fn decode_jentries(&mut self, length: usize) -> Result<VecDeque<JEntry>, Error> {
        let mut jentries: VecDeque<JEntry> = VecDeque::with_capacity(length);
        for _ in 0..length {
            let encoded = self.buf.read_u32::<BigEndian>()?;
            let jentry = JEntry::decode_jentry(encoded);
            jentries.push_back(jentry);
        }
        Ok(jentries)
    }
}
