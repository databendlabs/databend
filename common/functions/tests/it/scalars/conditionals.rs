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

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::*;

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_if_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "if-passed",
        nullable: false,
        columns: vec![
            Series::new([true, false, false, true]).into(),
            Series::new([1i32, 2, 3, 4]).into(),
            Series::new([2.5_f64, 2.5_f64, 2.5_f64, 2.5_f64]).into(),
        ],
        expect: Series::new(vec![1f64, 2.5, 2.5, 4f64]).into(),
        error: "",
    }];

    test_scalar_functions(IfFunction::try_create_func("")?, &tests)
}
