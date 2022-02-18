// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use crate::prelude::*;

pub struct BooleanDeserializer {
    pub builder: MutableBooleanColumn,
}

impl TypeDeserializer for BooleanDeserializer {
    fn de(&mut self, reader: &mut &[u8]) -> Result<()> {
        let value: bool = reader.read_scalar()?;
        self.builder.append_value(value);
        Ok(())
    }

    fn de_default(&mut self) {
        self.builder.append_value(false);
    }

    fn de_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: bool = reader.read_scalar()?;
            self.builder.append_value(value);
        }

        Ok(())
    }

    fn de_text(&mut self, reader: &[u8]) -> Result<()> {
        let v = if reader.eq_ignore_ascii_case(b"true") {
            Ok(true)
        } else if reader.eq_ignore_ascii_case(b"false") {
            Ok(false)
        } else {
            Err(ErrorCode::BadBytes("Incorrect boolean value"))
        }?;

        self.builder.append_value(v);
        Ok(())
    }

    fn finish_to_column(&mut self) -> ColumnRef {
        self.builder.to_column()
    }
}
