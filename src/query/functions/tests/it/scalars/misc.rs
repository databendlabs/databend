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

use databend_common_expression::types::*;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_misc() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("misc.txt").unwrap();

    // Mistyped function name
    run_ast(file, "plu(1, 2)", &[]);

    // Constant function call, even though it may throw, should not stop constant folding
    run_ast(file, "const_false AND CAST('1000' AS UINT32) = 1000", &[(
        "const_false",
        BooleanType::from_data(vec![false]),
    )]);
    run_ast(file, "false AND CAST(str AS UINT32) = 1000", &[(
        "str",
        StringType::from_data(vec!["1000"]),
    )]);
}
