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

use std::ptr::NonNull;
use std::slice::Iter;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;
use databend_common_expression::types::binary::BinaryColumn;
use databend_common_expression::types::binary::BinaryIterator;
use databend_common_expression::types::number::Number;
use databend_common_hashtable::DictionaryKeys;

use super::large_number::LargeNumber;

pub trait KeysColumnIter<T: ?Sized> {
    type Iterator<'a>: Iterator<Item = &'a T>
    where
        Self: 'a,
        T: 'a;

    fn iter(&self) -> Self::Iterator<'_>;
}

pub struct FixedKeysColumnIter<T: Number> {
    column: Buffer<T>,
}

impl<T: Number> FixedKeysColumnIter<T> {
    pub fn create(column: &Buffer<T>) -> Result<Self> {
        Ok(Self {
            column: column.clone(),
        })
    }
}

impl<T: Number> KeysColumnIter<T> for FixedKeysColumnIter<T> {
    type Iterator<'a> = Iter<'a, T> where Self: 'a, T: 'a;

    fn iter(&self) -> Self::Iterator<'_> {
        self.column.iter()
    }
}

pub struct LargeFixedKeysColumnIter<T: LargeNumber> {
    holder: Buffer<T>,
}

impl<T: LargeNumber> LargeFixedKeysColumnIter<T> {
    pub fn create(holder: Buffer<T>) -> Result<Self> {
        Ok(Self { holder })
    }
}

impl<T: LargeNumber> KeysColumnIter<T> for LargeFixedKeysColumnIter<T> {
    type Iterator<'a> = Iter<'a, T> where Self: 'a, T: 'a;

    fn iter(&self) -> Self::Iterator<'_> {
        self.holder.iter()
    }
}

pub struct SerializedKeysColumnIter {
    column: BinaryColumn,
}

impl SerializedKeysColumnIter {
    pub fn create(column: &BinaryColumn) -> Result<SerializedKeysColumnIter> {
        Ok(SerializedKeysColumnIter {
            column: column.clone(),
        })
    }
}

impl KeysColumnIter<[u8]> for SerializedKeysColumnIter {
    type Iterator<'a> = BinaryIterator<'a> where Self: 'a;

    fn iter(&self) -> Self::Iterator<'_> {
        self.column.iter()
    }
}

pub struct DictionarySerializedKeysColumnIter {
    #[allow(dead_code)]
    column: BinaryColumn,
    #[allow(dead_code)]
    points: Vec<NonNull<[u8]>>,
    inner: Vec<DictionaryKeys>,
}

impl DictionarySerializedKeysColumnIter {
    pub fn create(
        dict_keys: usize,
        column: &BinaryColumn,
    ) -> Result<DictionarySerializedKeysColumnIter> {
        let mut inner = Vec::with_capacity(column.len());
        let mut points = Vec::with_capacity(column.len() * dict_keys);
        for (index, mut item) in column.iter().enumerate() {
            for _ in 0..dict_keys {
                let len = item.read_u64::<BigEndian>()? as usize;
                points.push(NonNull::from(&item[0..len]));
                item = &item[len..];
            }

            inner.push(DictionaryKeys::create(&points[index * dict_keys..]));
        }

        Ok(DictionarySerializedKeysColumnIter {
            inner,
            points,
            column: column.clone(),
        })
    }
}

impl KeysColumnIter<DictionaryKeys> for DictionarySerializedKeysColumnIter {
    type Iterator<'a> = std::slice::Iter<'a, DictionaryKeys>;

    fn iter(&self) -> Self::Iterator<'_> {
        self.inner.iter()
    }
}
