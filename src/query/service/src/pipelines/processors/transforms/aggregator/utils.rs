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

use common_expression::types::string::StringColumnBuilder;
use common_functions::aggregates::AggregateFunctionRef;
use common_hashtable::HashtableLike;

#[inline]
pub fn estimated_key_size<Table: HashtableLike>(table: &Table) -> usize {
    table.unsize_key_size().unwrap_or_default()
}

pub fn create_state_serializer(func: &AggregateFunctionRef, row: usize) -> StringColumnBuilder {
    let size = func.serialize_size_per_row().unwrap_or(4);
    StringColumnBuilder::with_capacity(row, row * size)
}
