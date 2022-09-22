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
use std::cmp::Ordering;
use std::collections::VecDeque;

use decimal_rs::Decimal;

use super::constants::*;
use super::error::*;
use crate::jentry::JEntry;

// builtin functions for `JSONB` bytes without decode all Values

// build `JSONB` array from items
// Assuming that the input values is valid JSONB data
pub fn build_array<'a>(
    items: impl IntoIterator<Item = &'a [u8]>,
    buf: &mut Vec<u8>,
) -> Result<(), Error> {
    // reserve space for header
    buf.resize(4, 0);
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
        buf[i] = *b;
    }
    buf.extend_from_slice(&data);

    Ok(())
}

// `JSONB` values supports partial decode for comparison,
// if the values are found to be unequal,
// the result will be returned immediately
//
// in first level header, values compare as the following order
// Scalar Null > Array > Object > Other Scalars(String > Number > Boolean)
pub fn compare(left: &[u8], right: &[u8]) -> Result<Ordering, Error> {
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
            let left_str = std::str::from_utf8(&left[..left_offset]).unwrap();
            let right_offset = right_jentry.length as usize;
            let right_str = std::str::from_utf8(&right[..right_offset]).unwrap();
            Ok(left_str.cmp(right_str))
        }
        (NUMBER_TAG, NUMBER_TAG) => {
            let left_offset = left_jentry.length as usize;
            let left_dec = Decimal::decode(&left[..left_offset]);
            let right_offset = right_jentry.length as usize;
            let right_dec = Decimal::decode(&right[..right_offset]);
            Ok(left_dec.partial_cmp(&right_dec).unwrap())
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

fn read_u32(buf: &[u8], idx: usize) -> Result<u32, Error> {
    let bytes: [u8; 4] = buf
        .get(idx..idx + 4)
        .ok_or(Error::InvalidEOF)?
        .try_into()
        .unwrap();
    Ok(u32::from_be_bytes(bytes))
}
