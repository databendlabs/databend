// Copyright 2021 Datafuse Labs
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

use arrow_array::Array;
use arrow_schema::Field;
use crate::error::Result;
use crate::nested::create_struct;
use crate::nested::NestedState;
use crate::read::deserialize::NestedIters;

type StructValues = Vec<Option<Result<(NestedState, ArrayRef)>>>;

/// An iterator adapter over [`DynIter`] assumed to be encoded as Struct arrays
pub struct StructIterator<'a> {
    iters: Vec<NestedIters<'a>>,
    fields: Vec<Field>,
}

impl<'a> StructIterator<'a> {
    /// Creates a new [`StructIterator`] with `iters` and `fields`.
    pub fn new(iters: Vec<NestedIters<'a>>, fields: Vec<Field>) -> Self {
        assert_eq!(iters.len(), fields.len());
        Self { iters, fields }
    }
}

impl<'a> StructIterator<'a> {
    fn deserialize(
        &mut self,
        values: StructValues,
    ) -> Option<Result<(NestedState, ArrayRef)>> {
        // This code is copied from arrow2 `StructIterator` and adds a custom `nth` method implementation
        // https://github.com/jorgecarleitao/arrow2/blob/main/src/io/parquet/read/deserialize/struct_.rs
        if values.iter().any(|x| x.is_none()) {
            return None;
        }

        // todo: unzip of Result not yet supported in stable Rust
        let mut nested = vec![];
        let mut new_values = vec![];
        for x in values {
            match x.unwrap() {
                Ok((nest, values)) => {
                    new_values.push(values);
                    nested.push(nest);
                }
                Err(e) => return Some(Err(e)),
            }
        }

        let array = create_struct(self.fields.clone(), &mut nested, new_values);
        Some(Ok(array))
    }
}

impl<'a> Iterator for StructIterator<'a> {
    type Item = Result<(NestedState, ArrayRef)>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let values = self
            .iters
            .iter_mut()
            .map(|iter| iter.nth(n))
            .collect::<Vec<_>>();

        self.deserialize(values)
    }

    fn next(&mut self) -> Option<Self::Item> {
        let values = self
            .iters
            .iter_mut()
            .map(|iter| iter.next())
            .collect::<Vec<_>>();

        self.deserialize(values)
    }
}
