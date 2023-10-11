use common_expression::block_debug::box_render;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::types::Int32Type;
use common_expression::types::NumberDataType;
use common_expression::Column;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_expression::FromData;

use crate::common::new_block;

#[test]
fn test_split_block() {
    let value = b"abc";
    let n = 10;
    let block = new_block(&[Column::String(
        StringColumnBuilder::repeat(&value[..], n).build(),
    )]);
    let sizes = block
        .split_by_rows_no_tail(3)
        .iter()
        .map(|b| b.num_rows())
        .collect::<Vec<_>>();
    assert_eq!(sizes, vec![3, 3, 4]);
}

#[test]
fn test_box_render_block() {
    let value = b"abc";
    let n = 10;
    let block = new_block(&[
        Int32Type::from_data(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        Column::String(StringColumnBuilder::repeat(&value[..], n).build()),
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
