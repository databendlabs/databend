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

use crate::types::AnyType;
use crate::Value;

/// Chunk is a lightweight container for a group of values.
pub struct Chunk {
    values: Vec<Value<AnyType>>,
    num_rows: usize,
    chunk_info: Option<Box<dyn ChunkInfo>>,
}

/// ChunkInfo is extra information about a chunk, could be used during the pipeline transformation.
pub trait ChunkInfo {}

impl Chunk {
    pub fn new(values: Vec<Value<AnyType>>, num_rows: usize) -> Self {
        Self::new_with_info(values, num_rows, None)
    }

    pub fn new_with_info(
        values: Vec<Value<AnyType>>,
        num_rows: usize,
        chunk_info: Option<Box<dyn ChunkInfo>>,
    ) -> Self {
        debug_assert!(
            values
                .iter()
                .filter(|value| match value {
                    Value::Scalar(_) => false,
                    Value::Column(c) => c.len() != num_rows,
                })
                .count()
                == 0
        );
        Self {
            values,
            num_rows,
            chunk_info,
        }
    }

    pub fn values(&self) -> &[Value<AnyType>] {
        &self.values
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn num_values(&self) -> usize {
        self.values.len()
    }
}
