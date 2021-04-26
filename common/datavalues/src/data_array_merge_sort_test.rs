// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_indices_other() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_arrow::arrow::compute::SortOptions;

    use crate::*;

    let a = Arc::new(UInt32Array::from(vec![None, Some(1), Some(2), Some(4)]));
    let b = Arc::new(UInt32Array::from(vec![None, Some(3)]));
    let c = DataArrayMerge::merge_indices(&[a], &[b], &[SortOptions::default()], None)?;

    // [0] false: when equal (None = None), rhs is picked
    // [1] true: None < 3
    // [2] true: 1 < 3
    // [3] true: 2 < 3
    // [4] false: 3 < 4
    // [5] true: rhs has finished => pick lhs
    assert_eq!(c, vec![false, true, true, true, false, true]);
    Ok(())
}

#[test]
fn test_indices_many() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_arrow::arrow::compute::SortOptions;

    use crate::*;

    let a1 = Arc::new(UInt32Array::from(vec![None, Some(1), Some(3)]));
    let b1 = Arc::new(UInt32Array::from(vec![None, Some(2), Some(3), Some(5)]));
    let option1 = SortOptions {
        descending: false,
        nulls_first: true
    };

    let a2 = Arc::new(UInt32Array::from(vec![Some(2), Some(3), Some(5)]));
    let b2 = Arc::new(UInt32Array::from(vec![Some(1), Some(4), Some(6), Some(6)]));
    let option2 = SortOptions {
        descending: true,
        nulls_first: true
    };

    let c = DataArrayMerge::merge_indices(&[a1, a2], &[b1, b2], &[option1, option2], None)?;

    // [0] true: (N = N, 2 > 1)
    // [1] false: (1 < N, irrelevant)
    // [2] true: (1 < 2, irrelevant)
    // [3] false: (2 < 3, irrelevant)
    // [4] false: (3 = 3, 5 < 6)
    // [5] true: (3 < 5, irrelevant)
    // [6] false: lhs finished
    assert_eq!(c, vec![true, false, true, false, false, true, false]);
    Ok(())
}

#[test]
fn test_merge_array() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_arrow::arrow::compute::SortOptions;

    use crate::*;

    let a1: DataArrayRef = Arc::new(UInt32Array::from(vec![Some(1), Some(3), Some(5)]));
    let b1: DataArrayRef = Arc::new(UInt32Array::from(vec![Some(2), Some(4), Some(6)]));

    let option1 = SortOptions {
        descending: false,
        nulls_first: true
    };

    let a2: DataArrayRef = Arc::new(UInt32Array::from(vec![Some(1), Some(3), Some(5)]));
    let b2: DataArrayRef = Arc::new(UInt32Array::from(vec![Some(2), Some(4), Some(6)]));
    let option2 = SortOptions {
        descending: false,
        nulls_first: true
    };

    let c = DataArrayMerge::merge_indices(
        &[a1.clone(), a2.clone()],
        &[b1.clone(), b2.clone()],
        &[option1, option2],
        None
    )?;

    assert_eq!(c, vec![true, false, true, false, true, false]);

    let d = DataArrayMerge::merge_array(&a1, &b1, &c)?;
    let expect: DataArrayRef = Arc::new(UInt32Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ]));

    assert_eq!(*d, *expect);
    Ok(())
}

#[test]
fn test_merge_array2() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_arrow::arrow::compute::SortOptions;

    use crate::*;

    let a1: DataArrayRef = Arc::new(UInt32Array::from(
        (1..500)
            .map(|s| Some(s as u32))
            .collect::<Vec<Option<u32>>>()
    ));
    let b1: DataArrayRef = Arc::new(UInt32Array::from(
        (500..1000)
            .map(|s| Some(s as u32))
            .collect::<Vec<Option<u32>>>()
    ));

    let option1 = SortOptions {
        descending: false,
        nulls_first: true
    };

    let c = DataArrayMerge::merge_indices(&[a1.clone()], &[b1.clone()], &[option1], None)?;

    let c_expected = (1..1000).map(|s| s < 500).collect::<Vec<_>>();
    assert_eq!(c, c_expected);

    let d = DataArrayMerge::merge_array(&a1, &b1, &c)?;
    let expect: DataArrayRef = Arc::new(UInt32Array::from(
        (1..1000)
            .map(|s| Some(s as u32))
            .collect::<Vec<Option<u32>>>()
    ));

    assert_eq!(*d, *expect);
    Ok(())
}
