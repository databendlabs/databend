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

use std::io::Write;

use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::utils::ColumnFrom;
use common_expression::Column;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_other() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("other.txt").unwrap();

    test_run_diff(file);
}

fn test_run_diff(file: &mut impl Write) {
    run_ast(file, "running_difference(-1)", &[]);
    run_ast(file, "running_difference(0.2)", &[]);
    run_ast(file, "running_difference(to_datetime(10000))", &[]);
    run_ast(file, "running_difference(to_date(10000))", &[]);
    run_ast(file, "running_difference(a)", &[(
        "a",
        DataType::Number(NumberDataType::UInt16),
        Column::from_data(vec![224u16, 384, 512]),
    )]);
    run_ast(file, "running_difference(a)", &[(
        "a",
        DataType::Number(NumberDataType::Float64),
        Column::from_data(vec![37.617673, 38.617673, 39.617673]),
    )]);
}
