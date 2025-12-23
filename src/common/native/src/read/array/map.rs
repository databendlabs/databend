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
use crate::nested::create_map;
use crate::read::deserialize::DynIter;

/// An iterator adapter over [`DynIter`] assumed to be encoded as Map columns
pub struct MapIterator<'a> {
    iter: DynIter<'a, Result<(NestedState, Column)>>,
    data_type: TableDataType,
}

impl<'a> MapIterator<'a> {
    /// Creates a new [`MapIterator`] with `iter` and `field`.
    pub fn new(iter: DynIter<'a, Result<(NestedState, Column)>>, data_type: TableDataType) -> Self {
        Self { iter, data_type }
    }
}

impl MapIterator<'_> {
    fn deserialize(
        &mut self,
        value: Option<Result<(NestedState, Column)>>,
    ) -> Option<Result<(NestedState, Column)>> {
        let (mut nested, values) = match value {
            Some(Ok((nested, values))) => (nested, values),
            Some(Err(err)) => return Some(Err(err)),
            None => return None,
        };
        let array = create_map(self.data_type.clone(), &mut nested, values);
        Some(Ok((nested, array)))
    }
}

impl Iterator for MapIterator<'_> {
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
