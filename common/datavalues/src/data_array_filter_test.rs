// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::prelude::*;

#[test]
fn filter_batch_array() -> Result<()> {
    #[allow(dead_code)]
    struct FilterArrayTest {
        name: &'static str,
        filter: DFBooleanArray,
        expect: Vec<Series>,
    }

    let batch_array = vec![
        Series::new(vec![1, 2, 3, 4, 5]),
        Series::new(vec![6, 7, 8, 9, 10]),
    ];

    let tests = vec![
        FilterArrayTest {
            name: "normal filter",
            filter: DFBooleanArray::new_from_slice(&vec![true, false, true, false, true]),
            expect: vec![Series::new(vec![1, 3, 5]), Series::new(vec![6, 8, 10])],
        },
        FilterArrayTest {
            name: "filter contain null",
            filter: DFBooleanArray::new_from_opt_slice(&vec![
                Some(true),
                Some(false),
                Some(true),
                None,
                None,
            ]),
            expect: vec![Series::new(vec![1, 3]), Series::new(vec![6, 8])],
        },
    ];

    for t in tests {
        let result = DataArrayFilter::filter_batch_array(batch_array.clone(), &t.filter)?;
        assert_eq!(t.expect.len(), result.len());
        for i in 0..t.expect.len() {
            assert!(result[i].series_equal(&(t.expect[i])), "{}", t.name)
        }
    }

    Ok(())
}
