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

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use crate::prelude::*;
use crate::TypeDeserializer;
use crate::TypeSerializer;

// builder.
pub struct BooleanSerializer {}

impl TypeSerializer for BooleanSerializer {
    fn serialize_strings(&self, column: &DataColumn) -> Result<Vec<String>> {
        let array = column.to_array()?;
        let array: &DFBooleanArray = array.static_cast();

        let result: Vec<String> = array
            .into_iter()
            .map(|x| {
                x.map(|v| {
                    if v {
                        "true".to_owned()
                    } else {
                        "false".to_owned()
                    }
                })
                .unwrap_or_else(|| "NULL".to_owned())
            })
            .collect();
        Ok(result)
    }
}

pub struct BoolDeserializer {
    pub builder: BooleanArrayBuilder,
}

impl TypeDeserializer for BoolDeserializer {
    fn de(&mut self, reader: &mut &[u8]) -> Result<()> {
        let value: bool = reader.read_scalar()?;
        self.builder.append_value(value);
        Ok(())
    }

    fn de_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: bool = reader.read_scalar()?;
            self.builder.append_value(value);
        }

        Ok(())
    }

    fn finish_to_series(&mut self) -> Series {
        self.builder.finish().into_series()
    }

    fn de_text(&mut self, reader: &[u8]) -> Result<()> {
        let v = if reader.eq_ignore_ascii_case(b"false") {
            Some(false)
        } else if reader.eq_ignore_ascii_case(b"true") {
            Some(true)
        } else if reader.eq_ignore_ascii_case(b"null") {
            None
        } else {
            return Err(ErrorCode::BadBytes("Incorrect boolean value"));
        };
        self.builder.append_option(v);
        Ok(())
    }

    fn de_null(&mut self) {
        self.builder.append_null()
    }
}
