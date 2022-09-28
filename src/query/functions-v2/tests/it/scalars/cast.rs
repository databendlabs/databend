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
use common_expression::Column;
use common_expression::ColumnFrom;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_cast() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("cast.txt").unwrap();

    test_cast_to_variant(file);
}

fn test_cast_to_variant(file: &mut impl Write) {
    run_ast(file, "CAST(NULL AS VARIANT)", &[]);
    run_ast(file, "CAST(0 AS VARIANT)", &[]);
    run_ast(file, "CAST(-1 AS VARIANT)", &[]);
    run_ast(file, "CAST(1.1 AS VARIANT)", &[]);
    // FIXME(andylokandy): Nan is not supported yet.
    run_ast(file, "CAST(0/0 AS VARIANT)", &[]);
    // FIXME(andylokandy): Inf is not supported yet.
    run_ast(file, "CAST(1/0 AS VARIANT)", &[]);
    // FIXME(andylokandy): NegInf is not supported yet.
    run_ast(file, "CAST(-1/0 AS VARIANT)", &[]);
    run_ast(file, "CAST('🍦 が美味しい' AS VARIANT)", &[]);
    run_ast(file, "CAST([0, 1, 2] AS VARIANT)", &[]);
    run_ast(file, "CAST([0, 'a'] AS VARIANT)", &[]);
    run_ast(file, "CAST(to_timestamp(1000000) AS VARIANT)", &[]);
    run_ast(file, "CAST(false AS VARIANT)", &[]);
    run_ast(file, "CAST(true AS VARIANT)", &[]);
    run_ast(
        file,
        "CAST(CAST('🍦 が美味しい' AS VARIANT) AS VARIANT)",
        &[],
    );
    // TODO(andylokandy): test CAST(<tuple> as variant)

    run_ast(file, "CAST(a AS VARIANT)", &[(
        "a",
        DataType::Nullable(Box::new(DataType::String)),
        Column::from_data_with_validity(vec!["a", "bc", "def"], vec![true, false, true]),
    )]);

    run_ast(file, "TRY_CAST(NULL AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(0 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(-1 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(1.1 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(0/0 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(1/0 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(-1/0 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST('🍦 が美味しい' AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST([0, 1, 2] AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST([0, 'a'] AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(to_timestamp(1000000) AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(false AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(true AS VARIANT)", &[]);
    run_ast(
        file,
        "TRY_CAST(TRY_CAST('🍦 が美味しい' AS VARIANT) AS VARIANT)",
        &[],
    );

    run_ast(file, "TRY_CAST(a AS VARIANT)", &[(
        "a",
        DataType::Nullable(Box::new(DataType::String)),
        Column::from_data_with_validity(vec!["a", "bc", "def"], vec![true, false, true]),
    )]);
}
