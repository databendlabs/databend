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

use crate::types::timestamp::check_timestamp;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

pub struct TimestampDeserializer {
    pub builder: Vec<i64>,
}

impl TimestampDeserializer {
    pub fn create() -> Self {
        Self {
            builder: Vec::new(),
        }
    }
}

impl TypeDeserializer for TimestampDeserializer {
    fn memory_size(&self) -> usize {
        self.builder.len() * std::mem::size_of::<i64>()
    }

    fn de_default(&mut self, _format: &FormatSettings) {
        self.builder.push(i64::default());
    }

    fn append_data_value(&mut self, value: Scalar, _format: &FormatSettings) -> Result<(), String> {
        let v = value
            .as_timestamp()
            .ok_or_else(|| "Unable to get timestamp value".to_string())?;
        check_timestamp(*v)?;
        self.builder.push(*v);
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<Scalar, String> {
        match self.builder.pop() {
            Some(v) => Ok(Scalar::Timestamp(v)),
            None => Err("Timestamp column is empty when pop data value".to_string()),
        }
    }

    fn finish_to_column(&mut self) -> Column {
        self.builder.shrink_to_fit();
        Column::Timestamp(std::mem::take(&mut self.builder).into())
    }
}
