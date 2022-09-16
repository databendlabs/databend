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

use byteorder::BigEndian;
use byteorder::WriteBytesExt;

use super::constants::*;
use super::error::Error;
use super::jentry::JEntry;
use super::value::Object;
use super::value::Value;

pub struct Encoder<'a> {
    pub buf: &'a mut Vec<u8>,
}

impl<'a> Encoder<'a> {
    pub fn new(buf: &'a mut Vec<u8>) -> Encoder<'a> {
        Self { buf }
    }

    // Encode `JSONB` Value to a sequence of bytes
    pub fn encode(&mut self, value: &Value<'a>) -> Result<(), Error> {
        match value {
            Value::Array(array) => self.encode_array(array)?,
            Value::Object(obj) => self.encode_object(obj)?,
            _ => self.encode_scalar(value)?,
        };
        Ok(())
    }

    // Encoded `Scalar` consists of a `Header`, a `JEntry` and encoded data
    fn encode_scalar(&mut self, value: &Value<'a>) -> Result<usize, Error> {
        self.buf
            .write_u32::<BigEndian>(SCALAR_CONTAINER_TAG)
            .unwrap();

        // Scalar Value only has one JEntry
        let mut scalar_len = 4 + 4;
        let mut jentry_index = self.reserve_jentries(4);

        let jentry = self.encode_value(value)?;
        scalar_len += jentry.length as usize;
        self.replace_jentry(jentry, &mut jentry_index);

        Ok(scalar_len)
    }

    // Encoded `Array` consists of a `Header`, N `JEntries` and encoded data
    // N is the number of `Array` inner values
    fn encode_array(&mut self, values: &[Value<'a>]) -> Result<usize, Error> {
        let header = ARRAY_CONTAINER_TAG | values.len() as u32;
        self.buf.write_u32::<BigEndian>(header).unwrap();

        // `Array` has N `JEntries`
        let mut array_len = 4 + values.len() * 4;
        let mut jentry_index = self.reserve_jentries(values.len() * 4);

        // encode all values
        for value in values.iter() {
            let jentry = self.encode_value(value)?;
            array_len += jentry.length as usize;
            self.replace_jentry(jentry, &mut jentry_index);
        }

        Ok(array_len)
    }

    // Encoded `Object` consists of a `Header`, 2 * N `JEntries` and encoded data
    // N is the number of `Object` inner key value pair
    fn encode_object(&mut self, obj: &Object<'a>) -> Result<usize, Error> {
        let header = OBJECT_CONTAINER_TAG | obj.len() as u32;
        self.buf.write_u32::<BigEndian>(header).unwrap();

        // `Object` has 2 * N `JEntries`
        let mut object_len = 4 + obj.len() * 8;
        let mut jentry_index = self.reserve_jentries(obj.len() * 8);

        // encode all keys first
        for (key, _) in obj.iter() {
            let len = key.len();
            object_len += len;
            self.buf.extend_from_slice(key.as_bytes());
            let jentry = JEntry::make_string_jentry(len);
            self.replace_jentry(jentry, &mut jentry_index);
        }
        // encode all values
        for (_, value) in obj.iter() {
            let jentry = self.encode_value(value)?;
            object_len += jentry.length as usize;
            self.replace_jentry(jentry, &mut jentry_index);
        }

        Ok(object_len)
    }

    // Reserve space for `JEntries` and fill them later
    // As the length of each `Value` cannot be known until the `Value` encoded
    fn reserve_jentries(&mut self, len: usize) -> usize {
        let old_len = self.buf.len();
        let new_len = old_len + len;
        self.buf.resize(new_len, 0);
        old_len
    }

    // Write encoded `JEntry` to the corresponding index
    fn replace_jentry(&mut self, jentry: JEntry, jentry_index: &mut usize) {
        let jentry_bytes = jentry.encoded().to_be_bytes();
        for (i, b) in jentry_bytes.iter().enumerate() {
            self.buf[*jentry_index + i] = *b;
        }
        *jentry_index += 4;
    }

    // `Null` and `Boolean` only has a `JEntry`
    // `Number` and `String` has a `JEntry` and an encoded data
    // `Array` and `Object` has a container `JEntry` and nested encoded data
    fn encode_value(&mut self, value: &Value<'a>) -> Result<JEntry, Error> {
        let jentry = match value {
            Value::Null => JEntry::make_null_jentry(),
            Value::Bool(v) => {
                if *v {
                    JEntry::make_true_jentry()
                } else {
                    JEntry::make_false_jentry()
                }
            }
            Value::Number(v) => {
                let old_off = self.buf.len();
                let _ = v.compact_encode(&mut self.buf)?;
                let len = self.buf.len() - old_off;
                JEntry::make_number_jentry(len)
            }
            Value::String(s) => {
                let len = s.len();
                self.buf.extend_from_slice(s.as_ref().as_bytes());
                JEntry::make_string_jentry(len)
            }
            Value::Array(array) => {
                let len = self.encode_array(array)?;
                JEntry::make_container_jentry(len)
            }
            Value::Object(obj) => {
                let len = self.encode_object(obj)?;
                JEntry::make_container_jentry(len)
            }
        };

        Ok(jentry)
    }
}
