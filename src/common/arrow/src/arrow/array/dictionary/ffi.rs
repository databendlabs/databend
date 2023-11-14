// Copyright 2020-2022 Jorge C. Leitão
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

use super::DictionaryArray;
use super::DictionaryKey;
use crate::arrow::array::FromFfi;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::array::ToFfi;
use crate::arrow::error::Error;
use crate::arrow::ffi;

unsafe impl<K: DictionaryKey> ToFfi for DictionaryArray<K> {
    fn buffers(&self) -> Vec<Option<*const u8>> {
        self.keys.buffers()
    }

    fn offset(&self) -> Option<usize> {
        self.keys.offset()
    }

    fn to_ffi_aligned(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            keys: self.keys.to_ffi_aligned(),
            values: self.values.clone(),
        }
    }
}

impl<K: DictionaryKey, A: ffi::ArrowArrayRef> FromFfi<A> for DictionaryArray<K> {
    unsafe fn try_from_ffi(array: A) -> Result<Self, Error> {
        // keys: similar to PrimitiveArray, but the datatype is the inner one
        let validity = unsafe { array.validity() }?;
        let values = unsafe { array.buffer::<K>(1) }?;

        let data_type = array.data_type().clone();

        let keys = PrimitiveArray::<K>::try_new(K::PRIMITIVE.into(), values, validity)?;
        let values = array
            .dictionary()?
            .ok_or_else(|| Error::oos("Dictionary Array must contain a dictionary in ffi"))?;
        let values = ffi::try_from(values)?;

        // the assumption of this trait
        DictionaryArray::<K>::try_new_unchecked(data_type, keys, values)
    }
}
