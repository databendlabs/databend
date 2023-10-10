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

use common_expression::types::fixed_string::FixedStringColumnBuilder;
use common_expression::types::string::StringColumnBuilder;
use common_expression::Column;
use common_functions::aggregates::AggregateFunctionRef;
use common_hashtable::HashtableLike;

#[inline]
pub fn estimated_key_size<Table: HashtableLike>(table: &Table) -> usize {
    table.unsize_key_size().unwrap_or_default()
}

pub trait StateSerializer {
    fn data(&mut self) -> &mut Vec<u8>;
    fn commit_row(&mut self);
    fn build(self: Box<Self>) -> Column;
}

pub fn create_state_serializer(
    func: &AggregateFunctionRef,
    row: usize,
) -> Box<dyn StateSerializer> {
    if let Some(size) = func.serialize_size_per_row() {
        println!(
            "create_state_serializer: {} fixed size: {}",
            func.name(),
            size
        );
        Box::new(FixedStringColumnBuilder::with_capacity(row * size, size)) as _
    } else {
        println!("create_state_serializer: {} none fixed size", func.name(),);
        Box::new(StringColumnBuilder::with_capacity(row, row * 4)) as _
    }
}

impl StateSerializer for StringColumnBuilder {
    fn data(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    fn commit_row(&mut self) {
        self.commit_row()
    }

    fn build(self: Box<Self>) -> Column {
        let x = *self;
        Column::String(x.build())
    }
}

impl StateSerializer for FixedStringColumnBuilder {
    fn data(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    fn commit_row(&mut self) {}

    fn build(self: Box<Self>) -> Column {
        let x = *self;
        Column::FixedString(x.build())
    }
}
