// Copyright 2023 Datafuse Labs.
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

use comfy_table::Table;
use common_expression::types::array::ArrayColumnBuilder;
use common_expression::types::AnyType;
use common_expression::types::Int32Type;
use common_expression::types::NumberColumnBuilder;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::FromData;
use goldenfile::Mint;
use itertools::Itertools;

use super::run_ast_and_eval;

#[test]
fn test_unnest() {
    let mut mint = Mint::new("tests/it/scalars/testdata/");
    let file = &mut mint.new_goldenfile("unnest.txt").unwrap();

    run_unnest(file, "unnest(1)", &[]); // failed.
    run_unnest(file, "unnest([]::array(int))", &[]);
    run_unnest(file, "unnest([1, 2, 3])", &[]);

    let mut array_col_builder = ArrayColumnBuilder::<AnyType> {
        builder: ColumnBuilder::Number(NumberColumnBuilder::Int32(Vec::with_capacity(5))),
        offsets: vec![0],
    };
    array_col_builder.push(Int32Type::from_data(vec![1, 2]));
    array_col_builder.push(Int32Type::from_data(vec![3, 4, 5]));
    let array_col = array_col_builder.build();

    run_unnest(file, "unnest(a)", &[(
        "a",
        Column::Array(Box::new(array_col)),
    )]);
}

pub fn run_unnest(file: &mut impl Write, text: &str, columns: &[(&str, Column)]) {
    let result = run_ast_and_eval(text, columns);
    match &result {
        Ok((raw_expr, expr, _, _, _, column)) => {
            writeln!(file, "ast            : {text}").unwrap();
            writeln!(file, "raw expr       : {raw_expr}").unwrap();
            writeln!(file, "checked expr   : {expr}").unwrap();
            let column = column.as_column().unwrap().as_array().unwrap();

            let used_columns = raw_expr
                .column_refs()
                .keys()
                .cloned()
                .sorted()
                .collect::<Vec<_>>();

            let mut table = Table::new();
            table.load_preset("||--+-++|    ++++++");
            table.set_header(["Column", "Data"]);

            let ids = match used_columns.is_empty() {
                true => {
                    if columns.is_empty() {
                        vec![]
                    } else {
                        vec![0]
                    }
                }
                false => used_columns,
            };

            for id in ids.iter() {
                let (name, col) = &columns[*id];
                table.add_row(&[name.to_string(), format!("{col:?}")]);
            }

            table.add_row(["Output".to_string(), format!("{column:?}")]);
            writeln!(file, "evaluation (internal):\n{table}").unwrap();

            write!(file, "\n\n").unwrap();
        }
        Err(e) => {
            writeln!(file, "error: {}\n", e.message()).unwrap();
        }
    }
}
