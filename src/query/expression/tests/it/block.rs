use databend_common_expression::block_debug::box_render;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::Value;

use crate::common::new_block;

#[test]
fn test_split_block() {
    let value = "abc";
    let n = 10;
    let block = new_block(&[Column::String(
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
    let block = new_block(&[
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

    let entry = BlockEntry::new(
        DataType::Number(NumberDataType::UInt8),
        Value::<AnyType>::Scalar(scalar_u8),
    );
    assert_eq!(1, entry.memory_size());

    let scalar_str = Scalar::String("abc".to_string());
    let entry = BlockEntry::new(DataType::String, Value::<AnyType>::Scalar(scalar_str));
    assert_eq!(3, entry.memory_size());
}
