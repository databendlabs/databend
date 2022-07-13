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

use itertools::Itertools;

use crate::values::Column;

pub struct Chunk {
    columns: Vec<Column>,
}

impl Chunk {
    pub fn new(columns: Vec<Column>) -> Self {
        assert!(columns.iter().map(|col| col.len()).all_equal());
        Self { columns }
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn num_rows(&self) -> usize {
        self.columns.get(0).map(Column::len).unwrap_or(0)
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }
}
