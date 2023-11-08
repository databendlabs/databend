use arrow2::types::days_ms;
use arrow2::types::months_days_ns;
use arrow2::types::BitChunkIter;
use arrow2::types::BitChunkOnes;
use arrow2::types::NativeType;

#[test]
fn test_basic1() {
    let a = [0b00000001, 0b00010000]; // 0th and 13th entry
    let a = u16::from_ne_bytes(a);
    let iter = BitChunkIter::new(a, 16);
    let r = iter.collect::<Vec<_>>();
    assert_eq!(r, (0..16).map(|x| x == 0 || x == 12).collect::<Vec<_>>(),);
}

#[test]
fn test_ones() {
    let a = [0b00000001, 0b00010000]; // 0th and 13th entry
    let a = u16::from_ne_bytes(a);
    let mut iter = BitChunkOnes::new(a);
    assert_eq!(iter.size_hint(), (2, Some(2)));
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next(), Some(12));
}

#[test]
fn months_days_ns_roundtrip() {
    let a = months_days_ns(1, 2, 3);
    let bytes = a.to_le_bytes();
    assert_eq!(bytes, [1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0]);

    let a = months_days_ns(1, 1, 1);
    assert_eq!(a, months_days_ns::from_be_bytes(a.to_be_bytes()));
}

#[test]
fn days_ms_roundtrip() {
    let a = days_ms(1, 2);
    let bytes = a.to_le_bytes();
    assert_eq!(bytes, [1, 0, 0, 0, 2, 0, 0, 0]);

    let a = days_ms(1, 2);
    assert_eq!(a, days_ms::from_be_bytes(a.to_be_bytes()));
}
