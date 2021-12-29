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
use common_functions::scalars::UpperFunction;

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_upper_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "upper-abc-passed",
            nullable: false,
            columns: vec![Series::new(vec!["Abc"]).into()],
            expect: DataColumn::Constant(DataValue::String(Some("ABC".as_bytes().to_vec())), 1),
            error: "",
        },
        ScalarFunctionTest {
            name: "upper-utf8-passed",
            nullable: false,
            columns: vec![Series::new(vec!["Dobrý den"]).into()],
            expect: DataColumn::Constant(
                DataValue::String(Some("DOBRÝ DEN".as_bytes().to_vec())),
                1,
            ),
            error: "",
        },
        ScalarFunctionTest {
            name: "ucase-utf8-passed",
            nullable: false,
            columns: vec![Series::new(vec!["Dobrý den"]).into()],
            expect: DataColumn::Constant(
                DataValue::String(Some("DOBRÝ DEN".as_bytes().to_vec())),
                1,
            ),
            error: "",
        },
    ];

    test_scalar_functions(UpperFunction::try_create("upper")?, &tests)
}

#[test]
fn test_upper_nullable() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "ucase-null-passed",
        nullable: true,
        columns: vec![Series::new(vec![Option::<Vec<u8>>::None]).into()],
        expect: DataColumn::Constant(DataValue::String(None), 1),
        error: "",
    }];

    test_scalar_functions(UpperFunction::try_create("ucase")?, &tests)
}
