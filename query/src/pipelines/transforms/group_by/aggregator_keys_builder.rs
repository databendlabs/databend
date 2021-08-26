// Copyright 2020 Datafuse Labs.
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

use common_datablocks::HashMethod;
use common_datablocks::HashMethodFixedKeys;
use common_datavalues::arrays::ArrayBuilder;
use common_datavalues::arrays::BinaryArrayBuilder;
use common_datavalues::arrays::PrimitiveArrayBuilder;
use common_datavalues::prelude::IntoSeries;
use common_datavalues::prelude::Series;
use common_datavalues::DFNumericType;

use crate::pipelines::transforms::group_by::keys_ref::KeysRef;

/// Remove the group by key from the state and rebuild it into a column
pub trait KeysArrayBuilder<Value> {
    fn finish(self) -> Series;
    fn append_value(&mut self, v: &Value);
}

pub struct FixedKeysArrayBuilder<T>
where T: DFNumericType
{
    pub inner_builder: PrimitiveArrayBuilder<T>,
}

impl<T> KeysArrayBuilder<T::Native> for FixedKeysArrayBuilder<T>
where
    T: DFNumericType,
    HashMethodFixedKeys<T>: HashMethod<HashKey = T::Native>,
{
    #[inline]
    fn finish(mut self) -> Series {
        self.inner_builder.finish().array.into_series()
    }

    #[inline]
    fn append_value(&mut self, v: &T::Native) {
        self.inner_builder.append_value(*v)
    }
}

pub struct SerializedKeysArrayBuilder {
    pub inner_builder: BinaryArrayBuilder,
}

impl KeysArrayBuilder<KeysRef> for SerializedKeysArrayBuilder {
    fn finish(mut self) -> Series {
        self.inner_builder.finish().array.into_series()
    }

    fn append_value(&mut self, v: &KeysRef) {
        unsafe {
            let value = std::slice::from_raw_parts(v.address as *const u8, v.length);
            self.inner_builder.append_value(value);
        }
    }
}
