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

use std::marker::PhantomData;

use common_exception::Result;

use crate::prelude::DFPrimitiveArray;
use crate::prelude::DataColumn;
use crate::DFPrimitiveType;
use crate::TypeSerializer;

pub struct NumberSerializer<T: DFPrimitiveType> {
    t: PhantomData<T>,
}

impl<T: DFPrimitiveType> Default for NumberSerializer<T> {
    fn default() -> Self {
        Self {
            t: Default::default(),
        }
    }
}

impl<T: DFPrimitiveType> TypeSerializer for NumberSerializer<T> {
    fn serialize_strings(&self, column: &DataColumn) -> Result<Vec<String>> {
        let array = column.to_array()?;
        let array: &DFPrimitiveArray<T> = array.static_cast();

        let result: Vec<String> = array
            .iter()
            .map(|x| {
                x.map(|v| format!("{}", v))
                    .unwrap_or_else(|| "NULL".to_owned())
            })
            .collect();
        Ok(result)
    }
}
