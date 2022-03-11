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

use common_datavalues::PrimitiveColumn;
use common_datavalues::PrimitiveType;
use common_datavalues::StringColumn;
use common_exception::Result;

use crate::pipelines::transforms::group_by::keys_ref::KeysRef;

pub trait KeysColumnIter<T> {
    fn get_slice(&self) -> &[T];
}

pub struct FixedKeysColumnIter<T>
where T: PrimitiveType
{
    pub inner: PrimitiveColumn<T>,
}

impl<T> FixedKeysColumnIter<T>
where T: PrimitiveType
{
    pub fn create(inner: &PrimitiveColumn<T>) -> Result<Self> {
        Ok(Self {
            inner: inner.clone(),
        })
    }
}

impl<T> KeysColumnIter<T> for FixedKeysColumnIter<T>
where T: PrimitiveType
{
    fn get_slice(&self) -> &[T] {
        self.inner.values()
    }
}

pub struct SerializedKeysColumnIter {
    inner: Vec<KeysRef>,
    #[allow(unused)]
    column: StringColumn,
}

impl SerializedKeysColumnIter {
    pub fn create(column: &StringColumn) -> Result<SerializedKeysColumnIter> {
        let values = column.values();
        let offsets = column.offsets();

        let mut inner = Vec::with_capacity(offsets.len() - 1);
        for index in 0..(offsets.len() - 1) {
            let offset = offsets[index] as usize;
            let offset_1 = offsets[index + 1] as usize;
            let address = values[offset] as *mut u8 as usize;
            inner.push(KeysRef::create(address, offset_1 - offset));
        }

        Ok(SerializedKeysColumnIter {
            inner,
            column: column.clone(),
        })
    }
}

impl KeysColumnIter<KeysRef> for SerializedKeysColumnIter {
    fn get_slice(&self) -> &[KeysRef] {
        self.inner.as_slice()
    }
}
