// Copyright 2022 Datafuse Labs.
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

use std::vec;

use common_datavalues::prelude::*;
use common_exception::Result;
use serde_json::json;

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_length_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "length",
            columns: vec![Series::from_data(vec!["abc", ""])],
            expect: Series::from_data(vec![3_u64, 0_u64]),
            error: "",
        },
        ScalarFunctionTest {
            name: "length_array",
            columns: vec![Series::from_data(vec![
                ArrayValue::new(vec![1_i64.into(), 2_i64.into(), 3_i64.into()]),
                ArrayValue::new(vec![4_i64.into()]),
                ArrayValue::new(vec![]),
            ])],
            expect: Series::from_data(vec![3_u64, 1_u64, 0_u64]),
            error: "",
        },
        ScalarFunctionTest {
            name: "length",
            columns: vec![Series::from_data(vec![
                VariantValue::from(json!([1_i64, 2_i64, 3_i64])),
                VariantValue::from(json!([1_i64])),
            ])],
            expect: Series::from_data(vec![3_u64, 1_u64]),
            error: "",
        },
    ];

    test_scalar_functions("length", &tests)
}
