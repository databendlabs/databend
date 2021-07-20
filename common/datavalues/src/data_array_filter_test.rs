// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn filter_batch_array() -> Result<()> {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use super::*;

    #[allow(dead_code)]
    struct FilterArrayTest {
        name: &'static str,
        filter: BooleanArray,
        expect: Vec<DataArrayRef>,
    }

    let batch_array: Vec<DataArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        Arc::new(Int64Array::from(vec![6, 7, 8, 9, 10])),
    ];

    let tests = vec![
        FilterArrayTest {
            name: "normal filter",
            filter: BooleanArray::from(vec![true, false, true, false, true]),
            expect: vec![
                Arc::new(Int64Array::from(vec![1, 3, 5])),
                Arc::new(Int64Array::from(vec![6, 8, 10])),
            ],
        },
        FilterArrayTest {
            name: "filter contain null",
            filter: {
                let mut filter = BooleanArray::builder(5);
                filter.append_slice(&[true, false, true])?;
                filter.append_null()?;
                filter.append_null()?;
                filter.finish()
            },
            expect: vec![
                Arc::new(Int64Array::from(vec![1, 3])),
                Arc::new(Int64Array::from(vec![6, 8])),
            ],
        },
    ];

    for t in tests {
        let result = DataArrayFilter::filter_batch_array(batch_array.to_vec(), &t.filter)?;
        assert_eq!(t.expect.len(), result.len());
        for i in 0..t.expect.len() {
            assert_eq!(
                result.get(i).as_ref(),
                t.expect.get(i).as_ref(),
                "{}",
                t.name
            )
        }
    }

    Ok(())
}
