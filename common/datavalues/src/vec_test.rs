// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_aligned_vec_allocations() {
    use common_arrow::arrow::alloc;

    use super::*;

    // Can only have a zero copy to arrow memory if address of first byte % 64 == 0
    // check if we can increase above initial capacity and keep the Arrow alignment
    let mut v = AlignedVec::with_capacity_aligned(2);
    v.push(1);
    v.push(2);
    v.push(3);
    v.push(4);

    let ptr = v.as_ptr();
    assert_eq!((ptr as usize) % alloc::ALIGNMENT, 0);

    // check if we can shrink to fit
    let mut v = AlignedVec::with_capacity_aligned(10);
    v.push(1);
    v.push(2);
    v.shrink_to_fit();
    assert_eq!(v.len(), 2);
    assert_eq!(v.capacity(), 2);
    let ptr = v.as_ptr();
    assert_eq!((ptr as usize) % alloc::ALIGNMENT, 0);

    let a = v.into_primitive_array::<Int32Type>(None);
    assert_eq!(&a.values()[..2], &[1, 2])
}
