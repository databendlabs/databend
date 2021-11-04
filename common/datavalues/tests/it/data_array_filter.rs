// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

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
            filter: DFBooleanArray::new_from_slice(&[true, false, true, false, true]),
            expect: vec![Series::new(vec![1, 3, 5]), Series::new(vec![6, 8, 10])],
        },
        FilterArrayTest {
            name: "filter contain null",
            filter: DFBooleanArray::new_from_opt_slice(&[
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
        for (i, item) in result.iter().enumerate().take(t.expect.len()) {
            assert!(item.series_equal(&(t.expect[i])), "{}", t.name)
        }
    }

    Ok(())
}
