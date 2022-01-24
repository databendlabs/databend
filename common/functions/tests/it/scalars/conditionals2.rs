// Copyright 2021 Datafuse Labs.
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

use common_datavalues2::prelude::*;
use common_exception::Result;
use common_functions::scalars::IfFunction2;

use crate::scalars::scalar_function2_test::test_scalar_functions2;
use crate::scalars::scalar_function2_test::ScalarFunction2Test;

#[test]
fn test_if_function() -> Result<()> {
    let tests = vec![
        ScalarFunction2Test {
        name: "if-passed",
        columns: vec![
            Series::from_data([true, false, false, true]).into(),
            Series::from_data([1u8, 2, 3, 4]).into(),
            Series::from_data([2u8, 3u8, 2u8, 2u8]).into(),
        ],
        expect: Series::from_data(vec![Some(1u8), Some(3), Some(2), Some(4u8)]).into(),
        error: "",
    },
    ScalarFunction2Test {
        name: "if-null-in-predicate",
        columns: vec![
            Series::from_data([Some(true), None, Some(false), Some(true)]).into(),
            Series::from_data([1u8, 2, 3, 4]).into(),
            Series::from_data([2u8, 3u8, 2u8, 2u8]).into(),
        ],
        expect: Series::from_data(vec![Some(1u8), None, Some(2), Some(4u8)]).into(),
        error: "",
    },
    ];

    test_scalar_functions2(IfFunction2::try_create("if")?, &tests)
}
