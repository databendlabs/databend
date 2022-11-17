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

use std::io::Cursor;
use std::marker::PhantomData;

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::cursor_ext::*;
use common_io::prelude::BinaryRead;
use common_io::prelude::FormatSettings;
use common_io::prelude::StatBuffer;
use lexical_core::FromLexical;
use micromarshal::Unmarshal;

use crate::types::number::Number;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

pub struct NumberDeserializer<T: Number, P> {
    pub builder: Vec<T>,
    _p: PhantomData<P>,
}

impl<T: Number, P> NumberDeserializer<T, P> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: Vec::with_capacity(capacity),
            _p: PhantomData,
        }
    }
}

impl<T, P> TypeDeserializer for NumberDeserializer<T, P>
where
    T: Number + Unmarshal<T> + StatBuffer + From<P>,
    P: Unmarshal<P> + StatBuffer + FromLexical,
{
    fn memory_size(&self) -> usize {
        self.builder.len() * std::mem::size_of::<T>()
    }

    fn de_binary(&mut self, reader: &mut &[u8], _format: &FormatSettings) -> Result<()> {
        let value: T = reader.read_scalar()?;
        self.builder.push(value);
        Ok(())
    }

    fn de_default(&mut self) {
        self.builder.push(T::default());
    }

    fn de_fixed_binary_batch(
        &mut self,
        reader: &[u8],
        step: usize,
        rows: usize,
        _format: &FormatSettings,
    ) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: T = reader.read_scalar()?;
            self.builder.push(value);
        }
        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value, _format: &FormatSettings) -> Result<()> {
        match value {
            serde_json::Value::Number(v) => {
                let v = v.to_string();
                let mut reader = Cursor::new(v.as_bytes());

                let v: P = if !T::FLOATING {
                    reader.read_int_text()
                } else {
                    reader.read_float_text()
                }?;

                self.builder.push(v.into());
                Ok(())
            }
            _ => Err(ErrorCode::from("Incorrect json value, must be number")),
        }
    }

    fn append_data_value(&mut self, value: Scalar, _format: &FormatSettings) -> Result<()> {
        let v = value
            .as_number()
            .ok_or_else(|| ErrorCode::from("Unable to get number value"))?;
        let num = T::try_downcast_scalar(v).unwrap();
        self.builder.push(num);
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<()> {
        match self.builder.pop() {
            Some(_) => Ok(()),
            None => Err(ErrorCode::from(
                "Number column is empty when pop data value",
            )),
        }
    }

    fn finish_to_column(&mut self) -> Column {
        self.builder.shrink_to_fit();
        let col = T::upcast_column(std::mem::take(&mut self.builder).into());
        Column::Number(col)
    }
}
