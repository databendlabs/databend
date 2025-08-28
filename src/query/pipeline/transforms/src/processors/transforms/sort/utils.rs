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

use std::collections::BinaryHeap;

use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::SortColumnDescription;

use super::order_field_type;

pub const ORDER_COL_NAME: &str = "_order_col";

/// Find the bigger child of the root of the heap.
#[inline(always)]
pub fn find_bigger_child_of_root<T: Ord>(heap: &BinaryHeap<T>) -> &T {
    debug_assert!(heap.len() >= 2);
    let slice = heap.as_slice();
    if heap.len() == 2 {
        &slice[1]
    } else {
        (&slice[1]).max(&slice[2])
    }
}

pub fn has_order_field(schema: &DataSchema) -> bool {
    schema
        .fields
        .last()
        .is_some_and(|f| f.name() == ORDER_COL_NAME)
}

pub fn add_order_field(
    schema: DataSchemaRef,
    desc: &[SortColumnDescription],
    enable_fixed_rows: bool,
) -> DataSchemaRef {
    if has_order_field(&schema) {
        schema
    } else {
        let mut fields = schema.fields.clone();
        fields.push(DataField::new(
            ORDER_COL_NAME,
            order_field_type(&schema, desc, enable_fixed_rows),
        ));
        DataSchemaRefExt::create(fields)
    }
}
