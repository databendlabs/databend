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

use databend_common_expression::types::*;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_map_ops() {
    crate::ensure_tracing_initialized();
    
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("map_ops.txt").unwrap();

    test_map_cat_ops_basic(file);
}

fn test_map_cat_ops_basic(file: &mut impl Write) {
    let columns = [
        ("a_col", StringType::from_data(vec!["a_k1", "a_k2", "a_k3"])),
        ("b_col", StringType::from_data(vec!["b_k1", "b_k2", "b_k3"])),
        ("c_col", StringType::from_data(vec!["c_k1", "c_k2", "c_k3"])),
        (
            "d_col",
            StringType::from_data_with_validity(vec!["aaa", "bbb", "ccc"], vec![true, true, true]),
        ),
        (
            "e_col",
            StringType::from_data_with_validity(vec!["ddd", "eee", ""], vec![true, true, false]),
        ),
        (
            "f_col",
            StringType::from_data_with_validity(vec!["yyy", "", "zzz"], vec![true, false, true]),
        ),
    ];

    run_ast(
        file,
        "map_cat(map([a_col, b_col], [d_col, e_col]), map([c_col], [f_col]))",
        &columns,
    );
}
