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
fn test_geo_to_h3_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "geo-to-h3-test1",
        columns: vec![
            Series::from_data(vec![37.79506683_f64]),
            Series::from_data(vec![55.71290588_f64]),
            Series::from_data(vec![15_u8]),
        ],
        expect: Series::from_data([644325524701193974_u64]),
        error: "",
    }];

    test_scalar_functions("geo_to_h3", &tests)
}
