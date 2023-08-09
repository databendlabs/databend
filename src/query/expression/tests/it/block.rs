use common_expression::types::string::StringColumnBuilder;
use common_expression::Column;

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
