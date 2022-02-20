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

use common_exception::Result;

use crate::ColumnRef;
use crate::MutableColumn;
use crate::MutableNullColumn;
use crate::TypeDeserializer;

#[derive(Debug, Default)]
pub struct NullDeserializer {
    pub builder: MutableNullColumn,
}

impl TypeDeserializer for NullDeserializer {
    fn de(&mut self, _reader: &mut &[u8]) -> Result<()> {
        self.builder.append_default();
        Ok(())
    }

    fn de_default(&mut self) {
        self.builder.append_default();
    }

    fn de_batch(&mut self, _reader: &[u8], _step: usize, rows: usize) -> Result<()> {
        for _ in 0..rows {
            self.builder.append_default();
        }
        Ok(())
    }

    fn de_text(&mut self, _reader: &[u8]) -> Result<()> {
        self.builder.append_default();
        Ok(())
    }

    fn finish_to_column(&mut self) -> ColumnRef {
        self.builder.to_column()
    }
}
