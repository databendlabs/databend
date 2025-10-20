// Copyright 2023 Datafuse Labs.
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

use databend_common_column::buffer::Buffer;
use databend_common_expression::block_debug::box_render;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;

#[test]
fn test_split_block() {
    let value = "abc";
    let n = 10;
    let block = DataBlock::new_from_columns(vec![Column::String(
        StringColumnBuilder::repeat(value, n).build(),
    )]);
    let sizes = block
        .split_by_rows_if_needed_no_tail(3)
        .iter()
        .map(|b| b.num_rows())
        .collect::<Vec<_>>();
    assert_eq!(sizes, vec![3, 3, 4]);
}

#[test]
fn test_box_render_block() {
    let value = "abc";
    let n = 10;
    let block = DataBlock::new_from_columns(vec![
        Int32Type::from_data(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        Column::String(StringColumnBuilder::repeat(value, n).build()),
    ]);

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Number(NumberDataType::Int32)),
        DataField::new("e", DataType::String),
    ]);
    let d = box_render(&schema, &[block], 5, 1000, 1000, true).unwrap();
    let expected = r#"┌────────────────────┐
│     a     │    e   │
│   Int32   │ String │
├───────────┼────────┤
│         1 │ 'abc'  │
│         2 │ 'abc'  │
│         3 │ 'abc'  │
│         · │ ·      │
│         · │ ·      │
│         · │ ·      │
│         9 │ 'abc'  │
│        10 │ 'abc'  │
│   10 rows │        │
│ (5 shown) │        │
└────────────────────┘"#;
    assert_eq!(d, expected);
}

#[test]
fn test_block_entry_memory_size() {
    let scalar_u8 = Scalar::Number(NumberScalar::UInt8(1));

    let entry = BlockEntry::new_const_column(DataType::Number(NumberDataType::UInt8), scalar_u8, 1);
    assert_eq!(1, entry.memory_size());

    let scalar_str = Scalar::String("abc".to_string());
    let entry = BlockEntry::new_const_column(DataType::String, scalar_str, 1);
    assert_eq!(3, entry.memory_size());

    let col = StringType::from_data((0..10).map(|x| x.to_string()).collect::<Vec<_>>());
    assert_eq!(col.memory_size(), 10 + 10 * 16);

    let array = ArrayColumn::<Int64Type>::new(
        Buffer::from_iter(0..10i64),
        Buffer::from(vec![0u64, 1, 3, 6, 10]),
    );
    let total_memory_size = array.memory_size(); // 10 * 8 + 5 * 8 = 80 + 40 = 120
    let expected = array
        .iter()
        .map(|x| Int64Type::column_memory_size(&x))
        .sum::<usize>()
        + (array.len() + 1) * 8;
    assert_eq!(total_memory_size, expected);

    let array2 = array.slice(0..2);
    let array3 = array.slice(2..4);
    assert_eq!(
        total_memory_size,
        array2.memory_size() + array3.memory_size() - 8
    );
}
