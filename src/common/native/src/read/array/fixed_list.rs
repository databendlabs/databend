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

use databend_common_expression::Column;
use databend_common_expression::TableDataType;

use crate::error::Result;
use crate::nested::NestedState;
use crate::nested::create_fixed_list;
use crate::read::deserialize::DynIter;

/// An iterator adapter over [`DynIter`] assumed to be encoded as List columns
pub struct FixedListIterator<'a> {
    iter: DynIter<'a, Result<(NestedState, Column)>>,
    data_type: TableDataType,
    dimension: usize,
}

impl<'a> FixedListIterator<'a> {
    /// Creates a new [`FixedListIterator`] with `iter` and `data_type`.
    pub fn new(
        iter: DynIter<'a, Result<(NestedState, Column)>>,
        data_type: TableDataType,
        dimension: usize,
    ) -> Self {
        Self {
            iter,
            data_type,
            dimension,
        }
    }
}

impl FixedListIterator<'_> {
    fn deserialize(
        &mut self,
        value: Option<Result<(NestedState, Column)>>,
    ) -> Option<Result<(NestedState, Column)>> {
        let (mut nested, values) = match value {
            Some(Ok((nested, values))) => (nested, values),
            Some(Err(err)) => return Some(Err(err)),
            None => return None,
        };
        let array = create_fixed_list(self.data_type.clone(), self.dimension, &mut nested, values);
        Some(Ok((nested, array)))
    }
}

impl Iterator for FixedListIterator<'_> {
    type Item = Result<(NestedState, Column)>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let value = self.iter.nth(n);
        self.deserialize(value)
    }

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.iter.next();
        self.deserialize(value)
    }
}
