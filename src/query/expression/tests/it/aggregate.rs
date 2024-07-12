use databend_common_expression::types::*;
use databend_common_expression::FromData;
use databend_common_expression::InputColumns;

use crate::common::new_block;

#[test]
fn test_input_columns() {
    let strings = (0..10).map(|i: i32| i.to_string()).collect::<Vec<String>>();
    let nums = (0..10).collect::<Vec<_>>();
    let bools = (0..10).map(|i: usize| i % 2 == 0).collect();

    let columns = vec![
        StringType::from_data(strings),
        Int32Type::from_data(nums),
        BooleanType::from_data(bools),
    ];
    let block = new_block(&columns);

    let proxy = InputColumns::new_block_proxy(&[1], &block);
    assert_eq!(proxy.len(), 1);

    let proxy = InputColumns::new_block_proxy(&[2, 0, 1], &block);
    assert_eq!(proxy.len(), 3);
    assert!(proxy[0].as_boolean().is_some());
    assert!(proxy[1].as_string().is_some());
    assert!(proxy[2].as_number().is_some());

    assert_eq!(proxy.iter().count(), 3);

    let mut iter = proxy.iter();
    assert_eq!(iter.size_hint(), (3, Some(3)));
    let col = iter.nth(1);
    assert!(col.unwrap().as_string().is_some());

    assert_eq!(iter.size_hint(), (1, Some(1)));
    assert_eq!(iter.count(), 1);

    assert!(proxy.iter().last().unwrap().as_number().is_some());
    assert_eq!(proxy.iter().count(), 3);
    assert_eq!(proxy.iter().size_hint(), (3, Some(3)));

    let s = proxy.slice(..1);
    assert_eq!(s.len(), 1);
    assert!(s[0].as_boolean().is_some());

    let s = proxy.slice(1..=1);
    assert_eq!(s.len(), 1);
    assert!(s[0].as_string().is_some());
}
