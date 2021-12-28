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

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;
use common_functions::scalars::*;
use crate::scalars::scalar_function_test::{ScalarFunctionTest, test_scalar_functions};

#[test]
fn test_udf_example_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "udf-example-passed",
            nullable: false,
            columns: vec![
                Series::new(vec![true, true, true, false]).into(),
                Series::new(vec![true, false, true, true]).into(),
            ],
            expect: Series::new(vec![true, true, true, true]).into(),
            error: "",
        }
    ];

    test_scalar_functions(UdfExampleFunction::try_create("example")?, &tests)
}
