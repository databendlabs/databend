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
use crate::scalars::scalar_function_test::{ScalarFunctionTest, test_scalar_functions};

#[test]
fn test_crc32_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "crc-MySQL-passed",
            nullable: false,
            columns: vec![Series::new(vec!["MySQL"]).into()],
            expect: Series::new(vec![3259397556u32]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "crc-mysql-passed",
            nullable: false,
            columns: vec![Series::new(vec!["mysql"]).into()],
            expect: Series::new(vec![2501908538u32]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "crc-1-passed",
            nullable: false,
            columns: vec![Series::new(vec![1]).into()],
            expect: Series::new(vec![2212294583u32]).into(),
            error: "",
        },
    ];

    test_scalar_functions(CRC32Function::try_create("crc")?, &tests)
}
