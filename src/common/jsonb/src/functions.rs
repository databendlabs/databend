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

use core::convert::TryInto;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::VecDeque;

use super::constants::*;
use super::error::*;
use super::jentry::JEntry;
use super::number::Number;
use super::parser::decode_value;
use super::value::JsonPath;
use super::value::Value;

// builtin functions for `JSONB` bytes and `JSON` strings without decode all Values.
// The input value must be valid `JSONB' or `JSON`.

/// Build `JSONB` array from items.
/// Assuming that the input values is valid JSONB data.
pub fn build_array<'a>(
    items: impl IntoIterator<Item = &'a [u8]>,
    buf: &mut Vec<u8>,
) -> Result<(), Error> {
    let start = buf.len();
    // reserve space for header
    buf.resize(start + 4, 0);
    let mut len: u32 = 0;
    let mut data = Vec::new();
    for value in items.into_iter() {
        let header = read_u32(value, 0)?;
        let encoded_jentry = match header & CONTAINER_HEADER_TYPE_MASK {
            SCALAR_CONTAINER_TAG => {
                let jentry = &value[4..8];
                data.extend_from_slice(&value[8..]);
                jentry.try_into().unwrap()
            }
            ARRAY_CONTAINER_TAG | OBJECT_CONTAINER_TAG => {
                data.extend_from_slice(value);
                (CONTAINER_TAG | value.len() as u32).to_be_bytes()
            }
            _ => return Err(Error::InvalidJsonbHeader),
        };
        len += 1;
        buf.extend_from_slice(&encoded_jentry);
    }
    // write header
    let header = ARRAY_CONTAINER_TAG | len;
    for (i, b) in header.to_be_bytes().iter().enumerate() {
        buf[start + i] = *b;
    }
    buf.extend_from_slice(&data);

    Ok(())
}

/// Build `JSONB` object from items.
/// Assuming that the input values is valid JSONB data.
pub fn build_object<'a, K: AsRef<str>>(
    items: impl IntoIterator<Item = (K, &'a [u8])>,
    buf: &mut Vec<u8>,
) -> Result<(), Error> {
    let start = buf.len();
    // reserve space for header
    buf.resize(start + 4, 0);
    let mut len: u32 = 0;
    let mut key_data = Vec::new();
    let mut val_data = Vec::new();
    let mut val_jentries = VecDeque::new();
    for (key, value) in items.into_iter() {
        let key = key.as_ref();
        // write key jentry and key data
        let encoded_key_jentry = (STRING_TAG | key.len() as u32).to_be_bytes();
        buf.extend_from_slice(&encoded_key_jentry);
        key_data.extend_from_slice(key.as_bytes());

        // build value jentry and write value data
        let header = read_u32(value, 0)?;
        let encoded_val_jentry = match header & CONTAINER_HEADER_TYPE_MASK {
            SCALAR_CONTAINER_TAG => {
                let jentry = &value[4..8];
                val_data.extend_from_slice(&value[8..]);
                jentry.try_into().unwrap()
            }
            ARRAY_CONTAINER_TAG | OBJECT_CONTAINER_TAG => {
                val_data.extend_from_slice(value);
                (CONTAINER_TAG | value.len() as u32).to_be_bytes()
            }
            _ => return Err(Error::InvalidJsonbHeader),
        };
        val_jentries.push_back(encoded_val_jentry);
        len += 1;
    }
    // write header and value jentry
    let header = OBJECT_CONTAINER_TAG | len;
    for (i, b) in header.to_be_bytes().iter().enumerate() {
        buf[start + i] = *b;
    }
    while let Some(val_jentry) = val_jentries.pop_front() {
        buf.extend_from_slice(&val_jentry);
    }
    // write key data and value data
    buf.extend_from_slice(&key_data);
    buf.extend_from_slice(&val_data);

    Ok(())
}

/// Get the length of `JSONB` array.
pub fn array_length(value: &[u8]) -> Option<usize> {
    if !is_jsonb(value) {
        let json_value = decode_value(value).unwrap();
        return json_value.array_length();
    }
    let header = read_u32(value, 0).unwrap();
    match header & CONTAINER_HEADER_TYPE_MASK {
        ARRAY_CONTAINER_TAG => {
            let length = (header & CONTAINER_HEADER_LEN_MASK) as usize;
            Some(length)
        }
        _ => None,
    }
}

/// Get the inner value by ignoring case name of `JSONB` object.
pub fn get_by_name_ignore_case(value: &[u8], name: &str) -> Option<Vec<u8>> {
    if !is_jsonb(value) {
        let json_value = decode_value(value).unwrap();
        return json_value.get_by_name_ignore_case(name).map(to_vec);
    }

    let header = read_u32(value, 0).unwrap();
    match header & CONTAINER_HEADER_TYPE_MASK {
        OBJECT_CONTAINER_TAG => {
            let length = (header & CONTAINER_HEADER_LEN_MASK) as usize;
            let mut jentry_offset = 4;
            let mut val_offset = 8 * length + 4;

            let mut key_jentries: VecDeque<JEntry> = VecDeque::with_capacity(length);
            for _ in 0..length {
                let encoded = read_u32(value, jentry_offset).unwrap();
                let key_jentry = JEntry::decode_jentry(encoded);

                jentry_offset += 4;
                val_offset += key_jentry.length as usize;
                key_jentries.push_back(key_jentry);
            }

            let mut offsets = None;
            let mut key_offset = 8 * length + 4;
            while let Some(key_jentry) = key_jentries.pop_front() {
                let prev_key_offset = key_offset;
                key_offset += key_jentry.length as usize;
                let key =
                    unsafe { std::str::from_utf8_unchecked(&value[prev_key_offset..key_offset]) };
                // first match the value with the same name, if not found,
                // then match the value with the ignoring case name.
                if name.eq(key) {
                    offsets = Some((jentry_offset, val_offset));
                    break;
                } else if name.eq_ignore_ascii_case(key) && offsets.is_none() {
                    offsets = Some((jentry_offset, val_offset));
                }
                let val_encoded = read_u32(value, jentry_offset).unwrap();
                let val_jentry = JEntry::decode_jentry(val_encoded);
                jentry_offset += 4;
                val_offset += val_jentry.length as usize;
            }
            if let Some((jentry_offset, mut val_offset)) = offsets {
                let mut buf: Vec<u8> = Vec::new();
                let encoded = read_u32(value, jentry_offset).unwrap();
                let jentry = JEntry::decode_jentry(encoded);
                let prev_val_offset = val_offset;
                val_offset += jentry.length as usize;
                match jentry.type_code {
                    CONTAINER_TAG => buf.extend_from_slice(&value[prev_val_offset..val_offset]),
                    _ => {
                        let scalar_header = SCALAR_CONTAINER_TAG;
                        buf.extend_from_slice(&scalar_header.to_be_bytes());
                        buf.extend_from_slice(&encoded.to_be_bytes());
                        if val_offset > prev_val_offset {
                            buf.extend_from_slice(&value[prev_val_offset..val_offset]);
                        }
                    }
                }
                return Some(buf);
            }
            None
        }
        _ => None,
    }
}

/// Get the inner value by JSON path of `JSONB` object.
/// JSON path can be a nested index or name,
/// used to get inner value of array and object respectively.
pub fn get_by_path<'a>(value: &'a [u8], paths: Vec<JsonPath<'a>>) -> Option<Vec<u8>> {
    if !is_jsonb(value) {
        let json_value = decode_value(value).unwrap();
        return json_value.get_by_path(&paths).map(to_vec);
    }

    let mut offset = 0;
    let mut buf: Vec<u8> = Vec::new();

    for i in 0..paths.len() {
        let path = paths.get(i).unwrap();
        let header = read_u32(value, offset).unwrap();
        let (jentry_offset, val_offset) = match path {
            JsonPath::String(name) => {
                if header & CONTAINER_HEADER_TYPE_MASK != OBJECT_CONTAINER_TAG {
                    return None;
                }
                let length = (header & CONTAINER_HEADER_LEN_MASK) as usize;
                let mut jentry_offset = offset + 4;
                let mut val_offset = offset + 8 * length + 4;

                let mut key_jentries: VecDeque<JEntry> = VecDeque::with_capacity(length);
                for _ in 0..length {
                    let encoded = read_u32(value, jentry_offset).unwrap();
                    let key_jentry = JEntry::decode_jentry(encoded);

                    jentry_offset += 4;
                    val_offset += key_jentry.length as usize;
                    key_jentries.push_back(key_jentry);
                }

                let mut found = false;
                let mut key_offset = offset + 8 * length + 4;
                while let Some(key_jentry) = key_jentries.pop_front() {
                    let prev_key_offset = key_offset;
                    key_offset += key_jentry.length as usize;
                    let key = unsafe {
                        std::str::from_utf8_unchecked(&value[prev_key_offset..key_offset])
                    };
                    if name.eq(key) {
                        found = true;
                        break;
                    }
                    let val_encoded = read_u32(value, jentry_offset).unwrap();
                    let val_jentry = JEntry::decode_jentry(val_encoded);
                    jentry_offset += 4;
                    val_offset += val_jentry.length as usize;
                }
                if !found {
                    return None;
                }
                (jentry_offset, val_offset)
            }
            JsonPath::UInt64(index) => {
                if header & CONTAINER_HEADER_TYPE_MASK != ARRAY_CONTAINER_TAG {
                    return None;
                }
                let length = (header & CONTAINER_HEADER_LEN_MASK) as usize;
                if *index as usize >= length {
                    return None;
                }
                let mut jentry_offset = offset + 4;
                let mut val_offset = offset + 4 * length + 4;

                for _ in 0..*index {
                    let encoded = read_u32(value, jentry_offset).unwrap();
                    let jentry = JEntry::decode_jentry(encoded);

                    jentry_offset += 4;
                    val_offset += jentry.length as usize;
                }
                (jentry_offset, val_offset)
            }
        };
        let encoded = read_u32(value, jentry_offset).unwrap();
        let jentry = JEntry::decode_jentry(encoded);
        // if the last JSON path, return the value
        // if the value is a container value, then continue get for next JSON path.
        match jentry.type_code {
            CONTAINER_TAG => {
                if i == paths.len() - 1 {
                    buf.extend_from_slice(&value[val_offset..val_offset + jentry.length as usize]);
                } else {
                    offset = val_offset;
                }
            }
            _ => {
                if i == paths.len() - 1 {
                    let scalar_header = SCALAR_CONTAINER_TAG;
                    buf.extend_from_slice(&scalar_header.to_be_bytes());
                    buf.extend_from_slice(&encoded.to_be_bytes());
                    if jentry.length > 0 {
                        buf.extend_from_slice(
                            &value[val_offset..val_offset + jentry.length as usize],
                        );
                    }
                } else {
                    return None;
                }
            }
        }
    }
    Some(buf)
}

/// Get the keys of a `JSONB` object.
pub fn object_keys(value: &[u8]) -> Option<Vec<u8>> {
    if !is_jsonb(value) {
        let json_value = decode_value(value).unwrap();
        return json_value.object_keys().map(|val| to_vec(&val));
    }

    let header = read_u32(value, 0).unwrap();
    match header & CONTAINER_HEADER_TYPE_MASK {
        OBJECT_CONTAINER_TAG => {
            let mut buf: Vec<u8> = Vec::new();
            let length = (header & CONTAINER_HEADER_LEN_MASK) as usize;
            let key_header = ARRAY_CONTAINER_TAG | length as u32;
            buf.extend_from_slice(&key_header.to_be_bytes());

            let mut jentry_offset = 4;
            let mut key_offset = 8 * length + 4;
            let mut key_offsets = Vec::with_capacity(length);
            for _ in 0..length {
                let key_encoded = read_u32(value, jentry_offset).unwrap();
                let key_jentry = JEntry::decode_jentry(key_encoded);
                buf.extend_from_slice(&key_encoded.to_be_bytes());

                jentry_offset += 4;
                key_offset += key_jentry.length as usize;
                key_offsets.push(key_offset);
            }
            let mut prev_key_offset = 8 * length + 4;
            for key_offset in key_offsets {
                if key_offset > prev_key_offset {
                    buf.extend_from_slice(&value[prev_key_offset..key_offset]);
                }
                prev_key_offset = key_offset;
            }
            Some(buf)
        }
        _ => None,
    }
}

/// `JSONB` values supports partial decode for comparison,
/// if the values are found to be unequal, the result will be returned immediately.
/// In first level header, values compare as the following order:
/// Scalar Null > Array > Object > Other Scalars(String > Number > Boolean).
pub fn compare(left: &[u8], right: &[u8]) -> Result<Ordering, Error> {
    if !is_jsonb(left) {
        let lval = decode_value(left).unwrap();
        let lbuf = to_vec(&lval);
        return compare(&lbuf, right);
    } else if !is_jsonb(right) {
        let rval = decode_value(right).unwrap();
        let rbuf = to_vec(&rval);
        return compare(left, &rbuf);
    }

    let left_header = read_u32(left, 0)?;
    let right_header = read_u32(right, 0)?;
    match (
        left_header & CONTAINER_HEADER_TYPE_MASK,
        right_header & CONTAINER_HEADER_TYPE_MASK,
    ) {
        (SCALAR_CONTAINER_TAG, SCALAR_CONTAINER_TAG) => {
            let left_encoded = read_u32(left, 4)?;
            let left_jentry = JEntry::decode_jentry(left_encoded);
            let right_encoded = read_u32(right, 4)?;
            let right_jentry = JEntry::decode_jentry(right_encoded);
            compare_scalar(&left_jentry, &left[8..], &right_jentry, &right[8..])
        }
        (ARRAY_CONTAINER_TAG, ARRAY_CONTAINER_TAG) => {
            compare_array(left_header, &left[4..], right_header, &right[4..])
        }
        (OBJECT_CONTAINER_TAG, OBJECT_CONTAINER_TAG) => {
            compare_object(left_header, &left[4..], right_header, &right[4..])
        }
        (SCALAR_CONTAINER_TAG, ARRAY_CONTAINER_TAG | OBJECT_CONTAINER_TAG) => {
            let left_encoded = read_u32(left, 4)?;
            let left_jentry = JEntry::decode_jentry(left_encoded);
            match left_jentry.type_code {
                NULL_TAG => Ok(Ordering::Greater),
                _ => Ok(Ordering::Less),
            }
        }
        (ARRAY_CONTAINER_TAG | OBJECT_CONTAINER_TAG, SCALAR_CONTAINER_TAG) => {
            let right_encoded = read_u32(right, 4)?;
            let right_jentry = JEntry::decode_jentry(right_encoded);
            match right_jentry.type_code {
                NULL_TAG => Ok(Ordering::Less),
                _ => Ok(Ordering::Greater),
            }
        }
        (ARRAY_CONTAINER_TAG, OBJECT_CONTAINER_TAG) => Ok(Ordering::Greater),
        (OBJECT_CONTAINER_TAG, ARRAY_CONTAINER_TAG) => Ok(Ordering::Less),
        (_, _) => Err(Error::InvalidJsonbHeader),
    }
}

// Different types of values have different levels and are definitely not equal
fn jentry_compare_level(jentry: &JEntry) -> Result<u8, Error> {
    match jentry.type_code {
        NULL_TAG => Ok(5),
        CONTAINER_TAG => Ok(4),
        STRING_TAG => Ok(3),
        NUMBER_TAG => Ok(2),
        TRUE_TAG => Ok(1),
        FALSE_TAG => Ok(0),
        _ => Err(Error::InvalidJsonbJEntry),
    }
}

// `Scalar` values compare as the following order
// Null > Container(Array > Object) > String > Number > Boolean
fn compare_scalar(
    left_jentry: &JEntry,
    left: &[u8],
    right_jentry: &JEntry,
    right: &[u8],
) -> Result<Ordering, Error> {
    let left_level = jentry_compare_level(left_jentry)?;
    let right_level = jentry_compare_level(right_jentry)?;
    if left_level != right_level {
        return Ok(left_level.cmp(&right_level));
    }

    match (left_jentry.type_code, right_jentry.type_code) {
        (NULL_TAG, NULL_TAG) => Ok(Ordering::Equal),
        (CONTAINER_TAG, CONTAINER_TAG) => compare_container(left, right),
        (STRING_TAG, STRING_TAG) => {
            let left_offset = left_jentry.length as usize;
            let left_str = unsafe { std::str::from_utf8_unchecked(&left[..left_offset]) };
            let right_offset = right_jentry.length as usize;
            let right_str = unsafe { std::str::from_utf8_unchecked(&right[..right_offset]) };
            Ok(left_str.cmp(right_str))
        }
        (NUMBER_TAG, NUMBER_TAG) => {
            let left_offset = left_jentry.length as usize;
            let left_num = Number::decode(&left[..left_offset]);
            let right_offset = right_jentry.length as usize;
            let right_num = Number::decode(&right[..right_offset]);
            Ok(left_num.cmp(&right_num))
        }
        (TRUE_TAG, TRUE_TAG) => Ok(Ordering::Equal),
        (FALSE_TAG, FALSE_TAG) => Ok(Ordering::Equal),
        (_, _) => Err(Error::InvalidJsonbJEntry),
    }
}

fn compare_container(left: &[u8], right: &[u8]) -> Result<Ordering, Error> {
    let left_header = read_u32(left, 0)?;
    let right_header = read_u32(right, 0)?;

    match (
        left_header & CONTAINER_HEADER_TYPE_MASK,
        right_header & CONTAINER_HEADER_TYPE_MASK,
    ) {
        (ARRAY_CONTAINER_TAG, ARRAY_CONTAINER_TAG) => {
            compare_array(left_header, &left[4..], right_header, &right[4..])
        }
        (OBJECT_CONTAINER_TAG, OBJECT_CONTAINER_TAG) => {
            compare_object(left_header, &left[4..], right_header, &right[4..])
        }
        (ARRAY_CONTAINER_TAG, OBJECT_CONTAINER_TAG) => Ok(Ordering::Greater),
        (OBJECT_CONTAINER_TAG, ARRAY_CONTAINER_TAG) => Ok(Ordering::Less),
        (_, _) => Err(Error::InvalidJsonbHeader),
    }
}

// `Array` values compares each element in turn.
fn compare_array(
    left_header: u32,
    left: &[u8],
    right_header: u32,
    right: &[u8],
) -> Result<Ordering, Error> {
    let left_length = (left_header & CONTAINER_HEADER_LEN_MASK) as usize;
    let right_length = (right_header & CONTAINER_HEADER_LEN_MASK) as usize;

    let mut jentry_offset = 0;
    let mut left_val_offset = 4 * left_length;
    let mut right_val_offset = 4 * right_length;
    let length = if left_length < right_length {
        left_length
    } else {
        right_length
    };
    for _ in 0..length {
        let left_encoded = read_u32(left, jentry_offset)?;
        let left_jentry = JEntry::decode_jentry(left_encoded);
        let right_encoded = read_u32(right, jentry_offset)?;
        let right_jentry = JEntry::decode_jentry(right_encoded);

        let order = compare_scalar(
            &left_jentry,
            &left[left_val_offset..],
            &right_jentry,
            &right[right_val_offset..],
        )?;
        if order != Ordering::Equal {
            return Ok(order);
        }
        jentry_offset += 4;

        left_val_offset += left_jentry.length as usize;
        right_val_offset += right_jentry.length as usize;
    }

    Ok(left_length.cmp(&right_length))
}

// `Object` values compares each key-value in turn,
// first compare the key, and then compare the value if the key is equal.
// The Greater the key, the Less the Object, the Greater the value, the Greater the Object
fn compare_object(
    left_header: u32,
    left: &[u8],
    right_header: u32,
    right: &[u8],
) -> Result<Ordering, Error> {
    let left_length = (left_header & CONTAINER_HEADER_LEN_MASK) as usize;
    let right_length = (right_header & CONTAINER_HEADER_LEN_MASK) as usize;

    let mut jentry_offset = 0;
    let mut left_val_offset = 8 * left_length;
    let mut right_val_offset = 8 * right_length;

    let length = if left_length < right_length {
        left_length
    } else {
        right_length
    };
    // read all key jentries first
    let mut left_key_jentries: VecDeque<JEntry> = VecDeque::with_capacity(length);
    let mut right_key_jentries: VecDeque<JEntry> = VecDeque::with_capacity(length);
    for _ in 0..length {
        let left_encoded = read_u32(left, jentry_offset)?;
        let left_key_jentry = JEntry::decode_jentry(left_encoded);
        let right_encoded = read_u32(right, jentry_offset)?;
        let right_key_jentry = JEntry::decode_jentry(right_encoded);

        jentry_offset += 4;
        left_val_offset += left_key_jentry.length as usize;
        right_val_offset += right_key_jentry.length as usize;

        left_key_jentries.push_back(left_key_jentry);
        right_key_jentries.push_back(right_key_jentry);
    }

    let mut left_jentry_offset = 4 * left_length;
    let mut right_jentry_offset = 4 * right_length;
    let mut left_key_offset = 8 * left_length;
    let mut right_key_offset = 8 * right_length;
    for _ in 0..length {
        // first compare key, if keys are equal, then compare the value
        let left_key_jentry = left_key_jentries.pop_front().unwrap();
        let right_key_jentry = right_key_jentries.pop_front().unwrap();

        let key_order = compare_scalar(
            &left_key_jentry,
            &left[left_key_offset..],
            &right_key_jentry,
            &right[right_key_offset..],
        )?;
        if key_order != Ordering::Equal {
            if key_order == Ordering::Greater {
                return Ok(Ordering::Less);
            } else {
                return Ok(Ordering::Greater);
            }
        }

        let left_encoded = read_u32(left, left_jentry_offset)?;
        let left_val_jentry = JEntry::decode_jentry(left_encoded);
        let right_encoded = read_u32(right, right_jentry_offset)?;
        let right_val_jentry = JEntry::decode_jentry(right_encoded);

        let val_order = compare_scalar(
            &left_val_jentry,
            &left[left_val_offset..],
            &right_val_jentry,
            &right[right_val_offset..],
        )?;
        if val_order != Ordering::Equal {
            return Ok(val_order);
        }
        left_jentry_offset += 4;
        right_jentry_offset += 4;

        left_key_offset += left_key_jentry.length as usize;
        right_key_offset += right_key_jentry.length as usize;
        left_val_offset += left_val_jentry.length as usize;
        right_val_offset += right_val_jentry.length as usize;
    }

    Ok(left_length.cmp(&right_length))
}

/// Returns true if the `JSONB` is a Null.
pub fn is_null(value: &[u8]) -> bool {
    as_null(value).is_some()
}

/// If the `JSONB` is a Null, returns (). Returns None otherwise.
pub fn as_null(value: &[u8]) -> Option<()> {
    if !is_jsonb(value) {
        let v = value.first().unwrap();
        if *v == b'n' {
            return Some(());
        } else {
            return None;
        }
    }
    let header = read_u32(value, 0).unwrap();
    match header & CONTAINER_HEADER_TYPE_MASK {
        SCALAR_CONTAINER_TAG => {
            let jentry = read_u32(value, 4).unwrap();
            match jentry {
                NULL_TAG => Some(()),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Returns true if the `JSONB` is a Boolean. Returns false otherwise.
pub fn is_boolean(value: &[u8]) -> bool {
    as_bool(value).is_some()
}

/// If the `JSONB` is a Boolean, returns the associated bool. Returns None otherwise.
pub fn as_bool(value: &[u8]) -> Option<bool> {
    if !is_jsonb(value) {
        let v = value.first().unwrap();
        if *v == b't' {
            return Some(true);
        } else if *v == b'f' {
            return Some(false);
        } else {
            return None;
        }
    }
    let header = read_u32(value, 0).unwrap();
    match header & CONTAINER_HEADER_TYPE_MASK {
        SCALAR_CONTAINER_TAG => {
            let jentry = read_u32(value, 4).unwrap();
            match jentry {
                FALSE_TAG => Some(false),
                TRUE_TAG => Some(true),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Returns true if the `JSONB` is a Number. Returns false otherwise.
pub fn is_number(value: &[u8]) -> bool {
    as_number(value).is_some()
}

/// If the `JSONB` is a Number, returns the Number. Returns None otherwise.
pub fn as_number(value: &[u8]) -> Option<Number> {
    if !is_jsonb(value) {
        let json_value = decode_value(value).unwrap();
        return json_value.as_number().cloned();
    }
    let header = read_u32(value, 0).unwrap();
    match header & CONTAINER_HEADER_TYPE_MASK {
        SCALAR_CONTAINER_TAG => {
            let jentry_encoded = read_u32(value, 4).unwrap();
            let jentry = JEntry::decode_jentry(jentry_encoded);
            match jentry.type_code {
                NUMBER_TAG => {
                    let length = jentry.length as usize;
                    let num = Number::decode(&value[8..8 + length]);
                    Some(num)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Returns true if the `JSONB` is a i64 Number. Returns false otherwise.
pub fn is_i64(value: &[u8]) -> bool {
    as_i64(value).is_some()
}

/// If the `JSONB` is a Number, represent it as i64 if possible. Returns None otherwise.
pub fn as_i64(value: &[u8]) -> Option<i64> {
    match as_number(value) {
        Some(num) => num.as_i64(),
        None => None,
    }
}

/// Returns true if the `JSONB` is a u64 Number. Returns false otherwise.
pub fn is_u64(value: &[u8]) -> bool {
    as_u64(value).is_some()
}

/// If the `JSONB` is a Number, represent it as u64 if possible. Returns None otherwise.
pub fn as_u64(value: &[u8]) -> Option<u64> {
    match as_number(value) {
        Some(num) => num.as_u64(),
        None => None,
    }
}

/// Returns true if the `JSONB` is a f64 Number. Returns false otherwise.
pub fn is_f64(value: &[u8]) -> bool {
    as_f64(value).is_some()
}

/// If the `JSONB` is a Number, represent it as f64 if possible. Returns None otherwise.
pub fn as_f64(value: &[u8]) -> Option<f64> {
    match as_number(value) {
        Some(num) => num.as_f64(),
        None => None,
    }
}

/// Returns true if the `JSONB` is a String. Returns false otherwise.
pub fn is_string(value: &[u8]) -> bool {
    as_str(value).is_some()
}

/// If the `JSONB` is a String, returns the String. Returns None otherwise.
pub fn as_str(value: &[u8]) -> Option<Cow<'_, str>> {
    if !is_jsonb(value) {
        let v = value.first().unwrap();
        if *v == b'"' {
            let s = unsafe { std::str::from_utf8_unchecked(&value[1..value.len() - 1]) };
            return Some(Cow::Borrowed(s));
        } else {
            return None;
        }
    }
    let header = read_u32(value, 0).unwrap();
    match header & CONTAINER_HEADER_TYPE_MASK {
        SCALAR_CONTAINER_TAG => {
            let jentry_encoded = read_u32(value, 4).unwrap();
            let jentry = JEntry::decode_jentry(jentry_encoded);
            match jentry.type_code {
                STRING_TAG => {
                    let length = jentry.length as usize;
                    let s = unsafe { std::str::from_utf8_unchecked(&value[8..8 + length]) };
                    Some(Cow::Borrowed(s))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Returns true if the `JSONB` is An Array. Returns false otherwise.
pub fn is_array(value: &[u8]) -> bool {
    if !is_jsonb(value) {
        let v = value.first().unwrap();
        return *v == b'[';
    }
    let header = read_u32(value, 0).unwrap();
    matches!(header & CONTAINER_HEADER_TYPE_MASK, ARRAY_CONTAINER_TAG)
}

/// Returns true if the `JSONB` is An Object. Returns false otherwise.
pub fn is_object(value: &[u8]) -> bool {
    if !is_jsonb(value) {
        let v = value.first().unwrap();
        return *v == b'{';
    }
    let header = read_u32(value, 0).unwrap();
    matches!(header & CONTAINER_HEADER_TYPE_MASK, OBJECT_CONTAINER_TAG)
}

/// Parse path string to Json path.
/// Support `["<name>"]`, `[<index>]`, `:name` and `.name`.
pub fn parse_json_path(path: &[u8]) -> Result<Vec<JsonPath>, Error> {
    let mut idx = 0;
    let mut prev_idx = 0;
    let mut json_paths = Vec::new();
    while idx < path.len() {
        let c = read_char(path, &mut idx)?;
        if c == b'[' {
            let c = read_char(path, &mut idx)?;
            if c == b'"' {
                prev_idx = idx;
                loop {
                    let c = read_char(path, &mut idx)?;
                    if c == b'\\' {
                        idx += 1;
                    } else if c == b'"' {
                        let c = read_char(path, &mut idx)?;
                        if c != b']' {
                            return Err(Error::InvalidToken);
                        }
                        break;
                    }
                }
                if prev_idx == idx - 2 {
                    return Err(Error::InvalidToken);
                }
                let s = std::str::from_utf8(&path[prev_idx..idx - 2])?;
                let json_path = JsonPath::String(Cow::Borrowed(s));

                json_paths.push(json_path);
            } else {
                prev_idx = idx - 1;
                loop {
                    let c = read_char(path, &mut idx)?;
                    if c == b']' {
                        break;
                    }
                }
                if prev_idx == idx - 1 {
                    return Err(Error::InvalidToken);
                }
                let s = std::str::from_utf8(&path[prev_idx..idx - 1])?;
                if let Ok(v) = s.parse::<u64>() {
                    let json_path = JsonPath::UInt64(v);
                    json_paths.push(json_path);
                } else {
                    return Err(Error::InvalidToken);
                }
            }
        } else if c == b'"' {
            prev_idx = idx;
            loop {
                let c = read_char(path, &mut idx)?;
                if c == b'\\' {
                    idx += 1;
                } else if c == b'"' {
                    if idx < path.len() {
                        return Err(Error::InvalidToken);
                    }
                    break;
                }
            }
            let s = std::str::from_utf8(&path[prev_idx..idx - 1])?;
            let json_path = JsonPath::String(Cow::Borrowed(s));
            if json_paths.is_empty() {
                json_paths.push(json_path);
            } else {
                return Err(Error::InvalidToken);
            }
        } else {
            if c == b':' || c == b'.' {
                if idx == 1 {
                    return Err(Error::InvalidToken);
                } else {
                    prev_idx = idx;
                }
            }
            while idx < path.len() {
                let c = read_char(path, &mut idx)?;
                if c == b':' || c == b'.' || c == b'[' {
                    idx -= 1;
                    break;
                } else if c == b'\\' {
                    idx += 1;
                }
            }
            if prev_idx == idx {
                return Err(Error::InvalidToken);
            }
            let s = std::str::from_utf8(&path[prev_idx..idx])?;
            let json_path = JsonPath::String(Cow::Borrowed(s));
            json_paths.push(json_path);
        }
    }
    Ok(json_paths)
}

/// Convert `JSONB` value to String
pub fn to_string(value: &[u8]) -> String {
    if !is_jsonb(value) {
        let json = unsafe { String::from_utf8_unchecked(value.to_vec()) };
        return json;
    }

    let mut json = String::new();
    container_to_string(value, &mut 0, &mut json);
    json
}

fn container_to_string(value: &[u8], offset: &mut usize, json: &mut String) {
    let header = read_u32(value, *offset).unwrap();
    match header & CONTAINER_HEADER_TYPE_MASK {
        SCALAR_CONTAINER_TAG => {
            let mut jentry_offset = 4 + *offset;
            let mut value_offset = 8 + *offset;
            scalar_to_string(value, &mut jentry_offset, &mut value_offset, json);
        }
        ARRAY_CONTAINER_TAG => {
            json.push('[');
            let length = (header & CONTAINER_HEADER_LEN_MASK) as usize;
            let mut jentry_offset = 4 + *offset;
            let mut value_offset = 4 + *offset + 4 * length;
            for i in 0..length {
                if i > 0 {
                    json.push(',');
                }
                scalar_to_string(value, &mut jentry_offset, &mut value_offset, json);
            }
            json.push(']');
        }
        OBJECT_CONTAINER_TAG => {
            json.push('{');
            let length = (header & CONTAINER_HEADER_LEN_MASK) as usize;
            let mut jentry_offset = 4 + *offset;
            let mut key_offset = 4 + *offset + 8 * length;
            let mut keys = VecDeque::with_capacity(length);
            for _ in 0..length {
                let jentry_encoded = read_u32(value, jentry_offset).unwrap();
                let jentry = JEntry::decode_jentry(jentry_encoded);
                let key_length = jentry.length as usize;
                let key = unsafe {
                    std::str::from_utf8_unchecked(&value[key_offset..key_offset + key_length])
                };
                keys.push_back(key);
                jentry_offset += 4;
                key_offset += key_length;
            }
            let mut value_offset = key_offset;
            for i in 0..length {
                if i > 0 {
                    json.push(',');
                }
                let key = keys.pop_front().unwrap();
                json.push('\"');
                json.push_str(key);
                json.push('\"');
                json.push(':');
                scalar_to_string(value, &mut jentry_offset, &mut value_offset, json);
            }
            json.push('}');
        }
        _ => {}
    }
}

fn scalar_to_string(
    value: &[u8],
    jentry_offset: &mut usize,
    value_offset: &mut usize,
    json: &mut String,
) {
    let jentry_encoded = read_u32(value, *jentry_offset).unwrap();
    let jentry = JEntry::decode_jentry(jentry_encoded);
    let length = jentry.length as usize;
    match jentry.type_code {
        NULL_TAG => json.push_str("null"),
        TRUE_TAG => json.push_str("true"),
        FALSE_TAG => json.push_str("false"),
        NUMBER_TAG => {
            let num = Number::decode(&value[*value_offset..*value_offset + length]);
            json.push_str(&format!("{num}"));
        }
        STRING_TAG => {
            let val = unsafe {
                std::str::from_utf8_unchecked(&value[*value_offset..*value_offset + length])
            };
            json.push('\"');
            json.push_str(val);
            json.push('\"');
        }
        CONTAINER_TAG => {
            container_to_string(value, value_offset, json);
        }
        _ => {}
    }
    *jentry_offset += 4;
    *value_offset += length;
}

// Check whether the value is `JSONB` format,
// for compatibility with previous `JSON` string.
fn is_jsonb(value: &[u8]) -> bool {
    if let Some(v) = value.first() {
        if *v == ARRAY_PREFIX || *v == OBJECT_PREFIX || *v == SCALAR_PREFIX {
            return true;
        }
    }
    false
}

fn to_vec(value: &Value<'_>) -> Vec<u8> {
    let mut buf = Vec::new();
    value.to_vec(&mut buf);
    buf
}

fn read_char(buf: &[u8], idx: &mut usize) -> Result<u8, Error> {
    match buf.get(*idx) {
        Some(v) => {
            *idx += 1;
            Ok(*v)
        }
        None => Err(Error::InvalidEOF),
    }
}

fn read_u32(buf: &[u8], idx: usize) -> Result<u32, Error> {
    let bytes: [u8; 4] = buf
        .get(idx..idx + 4)
        .ok_or(Error::InvalidEOF)?
        .try_into()
        .unwrap();
    Ok(u32::from_be_bytes(bytes))
}
