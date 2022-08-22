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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_json_extract_path_text_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "json_extract_path_text",
            columns: vec![
                Series::from_data(vec!["{\"a\":[[1],[2]],\"o\":{\"p\":{\"q\":\"r\"}}}"]),
                Series::from_data(vec!["a[0][0]", "a.b", "o.p:q", "o['p']['q']", "o[0]"]),
            ],
            expect: Series::from_data(vec![
                Some("1"),
                None,
                Some("r"),
                Some("r"),
                None,
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "json_extract_path_text_error_type",
            columns: vec![
                Series::from_data(vec!["abc", "123"]),
                Series::from_data(vec![0_i32]),
            ],
            expect: Series::from_data(vec![None::<&str>, None::<&str>]),
            error: "Invalid argument types for function 'JSON_EXTRACT_PATH_TEXT': (String, Int32)",
        },
    ];

    test_scalar_functions("json_extract_path_text", &tests)
}
