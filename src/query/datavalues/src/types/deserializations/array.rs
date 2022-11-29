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

use common_exception::Result;
use common_io::prelude::BinaryRead;

use crate::prelude::*;

pub struct ArrayDeserializer {
    pub builder: MutableArrayColumn,
    pub inner: Box<TypeDeserializerImpl>,
}

impl TypeDeserializer for ArrayDeserializer {
    fn memory_size(&self) -> usize {
        self.builder.memory_size()
    }

    #[allow(clippy::uninit_vec)]
    fn de_binary(&mut self, reader: &mut &[u8]) -> Result<()> {
        let size = reader.read_uvarint()?;
        let mut values = Vec::with_capacity(size as usize);
        for _i in 0..size {
            self.inner.de_binary(reader)?;
            values.push(self.inner.pop_data_value().unwrap());
        }
        self.builder.append_value(ArrayValue::new(values));
        Ok(())
    }

    fn de_default(&mut self) {
        self.builder.append_default();
    }

    fn de_fixed_binary_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let size = reader.read_uvarint()?;
            let mut values = Vec::with_capacity(size as usize);
            for _i in 0..size {
                self.inner.de_binary(&mut reader)?;
                values.push(self.inner.pop_data_value().unwrap());
            }
            self.builder.append_value(ArrayValue::new(values));
        }
        Ok(())
    }

    fn append_data_value(&mut self, value: DataValue) -> Result<()> {
        self.builder.append_data_value(value)
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        self.builder.pop_data_value()
    }

    fn finish_to_column(&mut self) -> ColumnRef {
        self.builder.to_column()
    }
}
