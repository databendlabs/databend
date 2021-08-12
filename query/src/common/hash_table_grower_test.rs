use crate::common::hash_table_grower::Grower;

#[test]
fn test_hash_table_grower() {
    let mut grower = Grower::default();

    assert_eq!(grower.max_size(), 256);

    assert!(grower.overflow(129));
    assert!(!grower.overflow(128));

    assert_eq!(grower.place(1), 1);
    assert_eq!(grower.place(255), 255);
    assert_eq!(grower.place(256), 0);
    assert_eq!(grower.place(257), 1);

    assert_eq!(grower.next_place(1), 2);
    assert_eq!(grower.next_place(2), 3);
    assert_eq!(grower.next_place(254), 255);
    assert_eq!(grower.next_place(255), 0);

    grower.increase_size();
    assert_eq!(grower.max_size(), 1024);
}
