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
fn test_variant_map_ops() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("variant_map_ops.txt").unwrap();

    test_map_cat_op(file);
}

fn test_map_cat_op(file: &mut impl Write) {
    let columns = [
        (
            "map1",
            StringType::from_data_with_validity(
                vec![
                    "{\"key1\": \"value1\", \"key2\": \"value2\"}",
                    "{\"a\":1,\"b\":2}",
                    "{\"x\":\"hello\",\"y\":\"world\"}",
                ],
                vec![true, true, true],
            ),
        ),
        (
            "map2",
            StringType::from_data_with_validity(
                vec![
                    "{\"key3\": \"value3\", \"key4\": \"value4\"}",
                    "{\"c\":3,\"d\":4}",
                    "{\"alice\":\"cream\",\"z\":\"!\"}",
                ],
                vec![true, true, true],
            ),
        ),
    ];

    run_ast(file, r#"map_cat(NULL, parse_json(map2))"#, &columns);
    run_ast(file, r#"map_cat(parse_json(map1), NULL)"#, &columns);
    run_ast(file, r#"map_cat(NULL, NULL)"#, &columns);
    run_ast(
        file,
        r#"map_cat(parse_json(map1), parse_json(map2))"#,
        &columns,
    );
}
