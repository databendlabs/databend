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
use common_functions::scalars::LTrimFunction;
use common_functions::scalars::RTrimFunction;
use common_functions::scalars::TrimFunction;

use super::run_tests;
use super::Test;

#[test]
fn test_trim_function() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::String, false),
        DataField::new("b", DataType::Int64, false),
        DataField::new("c", DataType::UInt64, false),
    ]);

    let tests = vec![
        Test {
            name: "ltrim-abc-passed",
            display: "ltrim",
            nullable: false,
            arg_names: vec!["a"],
            columns: vec![Series::new(vec!["  abc"]).into()],
            func: LTrimFunction::try_create("ltrim")?,
            expect: DataColumn::Constant(DataValue::String(Some("abc".as_bytes().to_vec())), 1),
            error: "",
        },
        Test {
            name: "rtrim-abc-passed",
            display: "rtrim",
            nullable: false,
            arg_names: vec!["a"],
            columns: vec![Series::new(vec!["abc  "]).into()],
            func: RTrimFunction::try_create("rtrim")?,
            expect: DataColumn::Constant(DataValue::String(Some("abc".as_bytes().to_vec())), 1),
            error: "",
        },
        Test {
            name: "trim-abc-passed",
            display: "trim",
            nullable: false,
            arg_names: vec!["a"],
            columns: vec![Series::new(vec!["   abc  "]).into()],
            func: TrimFunction::try_create("trim")?,
            expect: DataColumn::Constant(DataValue::String(Some("abc".as_bytes().to_vec())), 1),
            error: "",
        },
        Test {
            name: "trim-blank-passed",
            display: "trim",
            nullable: false,
            arg_names: vec!["a"],
            columns: vec![Series::new(vec!["     "]).into()],
            func: TrimFunction::try_create("trim")?,
            expect: DataColumn::Constant(DataValue::String(Some("".as_bytes().to_vec())), 1),
            error: "",
        },
    ];
    run_tests(tests, schema)
}

#[test]
fn test_trim_nullable() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new(
        "nullable_string",
        DataType::String,
        true,
    )]);

    let tests = vec![Test {
        name: "trim-nullable-passed",
        display: "trim",
        nullable: true,
        arg_names: vec!["nullable_string"],
        columns: vec![Series::new(vec![Option::<Vec<u8>>::None]).into()],
        func: TrimFunction::try_create("trim")?,
        expect: DataColumn::Constant(DataValue::String(None), 1),
        error: "",
    }];
    run_tests(tests, schema)
}
