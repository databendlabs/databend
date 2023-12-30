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

use core::cell::RefCell;
use std::marker::PhantomData;

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_hashtable::DictionaryKeys;

use crate::types::ValueType;
use crate::Column;

pub trait KeyAccessor {
    type Key: ?Sized;

    /// # Safety
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    unsafe fn key_unchecked(&self, index: usize) -> &Self::Key;
}

pub struct PrimitiveKeyAccessor<T> {
    data: Buffer<T>,
}

impl<T> PrimitiveKeyAccessor<T> {
    pub fn new(data: Buffer<T>) -> Self {
        Self { data }
    }
}

impl<T> KeyAccessor for PrimitiveKeyAccessor<T> {
    type Key = T;

    /// # Safety
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    unsafe fn key_unchecked(&self, index: usize) -> &Self::Key {
        self.data.get_unchecked(index)
    }
}

pub struct StringKeyAccessor {
    data: Buffer<u8>,
    offsets: Buffer<u64>,
}

impl StringKeyAccessor {
    pub fn new(data: Buffer<u8>, offsets: Buffer<u64>) -> Self {
        Self { data, offsets }
    }
}

impl KeyAccessor for StringKeyAccessor {
    type Key = [u8];

    /// # Safety
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    unsafe fn key_unchecked(&self, index: usize) -> &Self::Key {
        &self.data[*self.offsets.get_unchecked(index) as usize
            ..*self.offsets.get_unchecked(index + 1) as usize]
    }
}

pub struct DicKeyAccessor {
    data: Vec<DictionaryKeys>,
}

impl DicKeyAccessor {
    pub fn new(data: Vec<DictionaryKeys>) -> Self {
        Self { data }
    }
}

impl KeyAccessor for DicKeyAccessor {
    type Key = DictionaryKeys;

    /// # Safety
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    unsafe fn key_unchecked(&self, index: usize) -> &Self::Key {
        self.data.get_unchecked(index)
    }
}

pub struct ColumnKeyAccessor<T: ValueType> {
    data: T::Column,
    phantom: PhantomData<T>,
    temp_scalar: RefCell<T::Scalar>,
}

impl<T: ValueType> ColumnKeyAccessor<T> {
    pub fn new(data: Column) -> Self {
        let data = T::try_downcast_column(&data).unwrap();
        Self {
            temp_scalar: RefCell::new(T::to_owned_scalar(T::index_column(&data, 0).unwrap())),
            data,
            phantom: PhantomData,
            // temp_ref: None,
        }
    }
}

impl<T: ValueType> KeyAccessor for ColumnKeyAccessor<T> {
    type Key = T::CompareKey;

    /// # Safety
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    unsafe fn key_unchecked(&self, index: usize) -> &Self::Key {
        let scalar = T::index_column_unchecked(&self.data, index);
        let scalar = T::to_owned_scalar(scalar);
        *self.temp_scalar.borrow_mut() = scalar;
        T::scalar_to_compare_key(&*self.temp_scalar.as_ptr()).unwrap()
    }
}
