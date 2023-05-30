use common_vector::index::normalize;

#[test]
fn test_normalize() {
    let mut vec = vec![3.0, 4.0];
    normalize(&mut vec);
    assert_eq!(vec, vec![0.6, 0.8]);
    let mut vec = vec![2.0];
    normalize(&mut vec);
    assert_eq!(vec, vec![1.0]);
}
