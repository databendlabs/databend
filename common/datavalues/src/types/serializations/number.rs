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
use lexical_core::FromLexical;

use crate::prelude::*;
use crate::DFPrimitiveType;
use crate::TypeSerializer;

pub struct NumberSerializer<T: DFPrimitiveType> {
    pub builder: PrimitiveArrayBuilder<T>,
}

impl<T> TypeSerializer for NumberSerializer<T>
where
    T: DFPrimitiveType,
    T: Unmarshal<T> + StatBuffer + FromLexical,
    DFPrimitiveArray<T>: IntoSeries,
{
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

    fn de(&mut self, reader: &mut &[u8]) -> Result<()> {
        let value: T = reader.read_scalar()?;
        self.builder.append_value(value);
        Ok(())
    }

    fn de_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: T = reader.read_scalar()?;
            self.builder.append_value(value);
        }
        Ok(())
    }

    fn de_text(&mut self, reader: &[u8]) -> Result<()> {
        if reader.eq_ignore_ascii_case(b"null") {
            self.builder.append_null();
            return Ok(());
        }

        match lexical_core::parse_partial::<T>(reader) {
            Ok((v, _)) => {
                self.builder.append_value(v);
                Ok(())
            }
            Err(e) => Err(ErrorCode::BadBytes(format!(
                "Incorrect number value: {}",
                e
            ))),
        }
    }

    fn de_null(&mut self) {
        self.builder.append_null()
    }

    fn finish_to_series(&mut self) -> Series {
        self.builder.finish().into_series()
    }
}
