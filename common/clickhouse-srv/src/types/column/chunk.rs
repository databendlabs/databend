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

use std::cmp;
use std::ops;

use super::ColumnData;
use crate::binary::Encoder;
use crate::types::column::column_data::ArcColumnData;
use crate::types::column::column_data::BoxColumnData;
use crate::types::SqlType;
use crate::types::Value;
use crate::types::ValueRef;

pub struct ChunkColumnData {
    data: ArcColumnData,
    range: ops::Range<usize>,
}

impl ChunkColumnData {
    pub(crate) fn new(data: ArcColumnData, range: ops::Range<usize>) -> Self {
        Self { data, range }
    }
}

impl ColumnData for ChunkColumnData {
    fn sql_type(&self) -> SqlType {
        self.data.sql_type()
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        self.data.save(
            encoder,
            self.range.start + start,
            cmp::min(self.range.end, self.range.start + end),
        )
    }

    fn len(&self) -> usize {
        self.range.len()
    }

    fn push(&mut self, _value: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        if index >= self.range.len() {
            panic!("out of range");
        }

        self.data.at(index + self.range.start)
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}
