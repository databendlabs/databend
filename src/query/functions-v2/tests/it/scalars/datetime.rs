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

use std::io::Write;

use common_expression::types::DataType;
use common_expression::Column;
use common_expression::ColumnFrom;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_boolean() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("datetime.txt").unwrap();

    test_to_timestamp(file);
}

fn test_to_timestamp(file: &mut impl Write) {
    run_ast(file, "to_timestamp(-30610224000000001)", &[]);
    run_ast(file, "to_timestamp(-315360000000000)", &[]);
    run_ast(file, "to_timestamp(-315360000000)", &[]);
    run_ast(file, "to_timestamp(-100)", &[]);
    run_ast(file, "to_timestamp(-0)", &[]);
    run_ast(file, "to_timestamp(0)", &[]);
    run_ast(file, "to_timestamp(100)", &[]);
    run_ast(file, "to_timestamp(315360000000)", &[]);
    run_ast(file, "to_timestamp(315360000000000)", &[]);
    run_ast(file, "to_timestamp(253402300800000000)", &[]);
    run_ast(file, "to_timestamp(a)", &[(
        "a",
        DataType::Int64,
        Column::from_data(vec![
            -315360000000000i64,
            315360000000,
            -100,
            0,
            100,
            315360000000,
            315360000000000,
        ]),
    )]);
}
