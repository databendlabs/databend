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

use common_io::prelude::*;

use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

#[derive(Debug, Default)]
pub struct NullDeserializer {
    pub len: usize,
}

impl NullDeserializer {
    pub fn create() -> Self {
        Self { len: 0 }
    }
}

impl TypeDeserializer for NullDeserializer {
    fn memory_size(&self) -> usize {
        self.len
    }

    fn de_default(&mut self, _format: &FormatSettings) {
        self.len += 1;
    }

    fn append_data_value(
        &mut self,
        _value: Scalar,
        _format: &FormatSettings,
    ) -> Result<(), String> {
        self.len += 1;
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<Scalar, String> {
        if self.len > 0 {
            self.len -= 1;
            Ok(Scalar::Null)
        } else {
            Err("Null column is empty when pop data value".to_string())
        }
    }

    fn finish_to_column(&mut self) -> Column {
        Column::Null { len: self.len }
    }
}
