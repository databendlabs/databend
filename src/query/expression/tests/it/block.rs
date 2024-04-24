use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::OwnedMemoryUsageSize;
use databend_common_base::runtime::ThreadTracker;
use databend_common_expression::block_debug::box_render;
use databend_common_expression::types::string::StringColumn;
use databend_common_expression::types::string::StringColumnBuilder;
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
fn test_block_entry_owned_memory_usage() {
    fn test_block_entry<F: Fn() -> BlockEntry>(f: F) {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();

        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);

        let mut block_entry = f();

        drop(_guard);
        assert_eq!(
            mem_stat.get_memory_usage(),
            block_entry.owned_memory_usage() as i64
        );
    }

    test_block_entry(|| BlockEntry::new(DataType::Null, Value::Scalar(Scalar::Null)));
    test_block_entry(|| {
        BlockEntry::new(
            DataType::String,
            Value::Column(Column::String(StringColumn::new(
                Buffer::from(vec![0; 100 * 3]),
                Buffer::from(vec![0; 100]),
            ))),
        )
    });
}
