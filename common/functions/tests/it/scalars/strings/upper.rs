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

use super::run_tests;
use super::Test;

#[test]
fn test_upper_function() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::String, false)]);

    let tests = vec![
        Test {
            name: "upper-abc-passed",
            display: "upper",
            nullable: true,
            arg_names: vec!["a"],
            columns: vec![Series::new(vec!["Abc"]).into()],
            func: UpperFunction::try_create("upper")?,
            expect: DataColumn::Constant(DataValue::String(Some("ABC".as_bytes().to_vec())), 1),
            error: "",
        },
        Test {
            name: "upper-utf8-passed",
            display: "upper",
            nullable: true,
            arg_names: vec!["a"],
            columns: vec![Series::new(vec!["Dobrý den"]).into()],
            func: UpperFunction::try_create("upper")?,
            expect: DataColumn::Constant(
                DataValue::String(Some("DOBRÝ DEN".as_bytes().to_vec())),
                1,
            ),
            error: "",
        },
        Test {
            name: "ucase-utf8-passed",
            display: "ucase",
            nullable: true,
            arg_names: vec!["a"],
            columns: vec![Series::new(vec!["Dobrý den"]).into()],
            func: UpperFunction::try_create("ucase")?,
            expect: DataColumn::Constant(
                DataValue::String(Some("DOBRÝ DEN".as_bytes().to_vec())),
                1,
            ),
            error: "",
        },
        Test {
            name: "ucase-null-passed",
            display: "ucase",
            nullable: true,
            arg_names: vec!["a"],
            columns: vec![Series::new(vec![Option::<Vec<u8>>::None]).into()],
            func: UpperFunction::try_create("ucase")?,
            expect: DataColumn::Constant(DataValue::String(None), 1),
            error: "",
        },
    ];
    run_tests(tests, schema)
}
