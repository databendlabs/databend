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
fn test_connection_id_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "connection_id-function-passed",
        columns: vec![Series::from_data(["a9105d8c-4e66-4a44-9249-9531e1e9b6a8"])],
        expect: Series::from_data(["a9105d8c-4e66-4a44-9249-9531e1e9b6a8"]),
        error: "",
    }];

    test_scalar_functions("connection_id", &tests)
}
