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

use crate::types::NullType;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

impl TypeDeserializer for usize {
    fn memory_size(&self) -> usize {
        std::mem::size_of::<usize>()
    }

    fn de_binary(&mut self, _reader: &mut &[u8], _format: &FormatSettings) -> Result<()> {
        *self += 1;
        Ok(())
    }

    fn de_default(&mut self, _format: &FormatSettings) {
        *self += 1;
        Ok(())
    }

    fn de_fixed_binary_batch(
        &mut self,
        _reader: &[u8],
        _step: usize,
        rows: usize,
        _format: &FormatSettings,
    ) -> Result<(), String> {
        for _ in 0..rows {
            *self += 1;
        }
        Ok(())
    }

    fn de_json(
        &mut self,
        _value: &serde_json::Value,
        _format: &FormatSettings,
    ) -> Result<(), String> {
        *self += 1;
        Ok(())
    }

    fn append_data_value(
        &mut self,
        _value: Scalar,
        _format: &FormatSettings,
    ) -> Result<(), String> {
        *self += 1;
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<Scalar, String> {
        if *self > 0 {
            *self -= 1;
            Ok(Scalar::Null)
        } else {
            Err("Null column is empty when pop data value".to_string())
        }
    }

    fn finish_to_column(&mut self) -> Column {
        Column::Null { len: *self }
    }
}
