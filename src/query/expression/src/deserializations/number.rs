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

use crate::types::number::Number;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

pub struct NumberDeserializer<T: Number> {
    pub builder: Vec<T>,
}

impl<T: Number> NumberDeserializer<T> {
    pub fn create() -> Self {
        Self {
            builder: Vec::new(),
        }
    }
}

impl<T> TypeDeserializer for NumberDeserializer<T>
where T: Number
{
    fn memory_size(&self) -> usize {
        self.builder.len() * std::mem::size_of::<T>()
    }

    fn de_default(&mut self, _format: &FormatSettings) {
        self.builder.push(T::default());
    }

    fn append_data_value(&mut self, value: Scalar, _format: &FormatSettings) -> Result<(), String> {
        let v = value
            .as_number()
            .ok_or_else(|| "Unable to get number value".to_string())?;
        let num = T::try_downcast_scalar(v).unwrap();
        self.builder.push(num);
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<Scalar, String> {
        match self.builder.pop() {
            Some(v) => {
                let num = T::upcast_scalar(v);
                Ok(Scalar::Number(num))
            }
            None => Err("Number column is empty when pop data value".to_string()),
        }
    }

    fn finish_to_column(&mut self) -> Column {
        self.builder.shrink_to_fit();
        let col = T::upcast_column(std::mem::take(&mut self.builder).into());
        Column::Number(col)
    }
}
