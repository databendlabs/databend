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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_io::prelude::*;

use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

pub struct BooleanDeserializer {
    pub builder: MutableBitmap,
}

impl BooleanDeserializer {
    pub fn create() -> Self {
        Self {
            builder: MutableBitmap::new(),
        }
    }
}

impl TypeDeserializer for BooleanDeserializer {
    fn memory_size(&self) -> usize {
        self.builder.as_slice().len()
    }

    fn de_default(&mut self, _format: &FormatSettings) {
        self.builder.push(false);
    }

    fn append_data_value(&mut self, value: Scalar, _format: &FormatSettings) -> Result<(), String> {
        let v = value
            .as_boolean()
            .ok_or_else(|| "Unable to get boolean value".to_string())?;
        self.builder.push(*v);
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<Scalar, String> {
        match self.builder.pop() {
            Some(v) => Ok(Scalar::Boolean(v)),
            None => Err("Boolean column is empty when pop data value".to_string()),
        }
    }

    fn finish_to_column(&mut self) -> Column {
        self.builder.shrink_to_fit();
        Column::Boolean(std::mem::take(&mut self.builder).into())
    }
}
