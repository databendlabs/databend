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

use std::collections::HashMap;
use std::io::Write;

use databend_common_constraint::mir::MirDataType;
use databend_common_constraint::mir::MirExpr;
use databend_common_constraint::simplify::mir_to_z3_assertion;
use databend_common_constraint::simplify::simplify_z3_ast;
use goldenfile::Mint;
use z3::Config;
use z3::Context;

use crate::parser;

pub fn simplify_ast(
    file: &mut impl Write,
    variables: &HashMap<String, MirDataType>,
    text: impl AsRef<str>,
) {
    let ctx = &Context::new(&Config::new());

    let text = text.as_ref();

    let mir_expr = parser::parse_mir_expr(text, variables);
    let z3_ast = mir_to_z3_assertion(ctx, &mir_expr);
    let output_z3_ast = z3_ast
        .as_ref()
        .and_then(|z3_ast| simplify_z3_ast(ctx, z3_ast));
    let output_mir_expr = output_z3_ast.as_ref().and_then(|output_z3_ast| {
        output_z3_ast
            .iter()
            .map(|formula| MirExpr::from_z3_ast(formula, variables))
            .collect::<Option<Vec<_>>>()
    });
    let ouput_sql_ast = output_mir_expr.as_ref().map(|output_mir_expr| {
        output_mir_expr
            .iter()
            .map(parser::pretty_mir_expr)
            .collect::<Vec<_>>()
    });

    writeln!(file, "{:-^80}", " Input ").unwrap();
    writeln!(file, "{}", text).unwrap();
    writeln!(file, "{:-^80}", " Output ").unwrap();

    if let Some(ouput_sql_ast) = ouput_sql_ast {
        for conjunction in ouput_sql_ast {
            writeln!(file, "{}", conjunction).unwrap();
        }
    } else {
        writeln!(file, "UNCHANGED").unwrap();
    }

    writeln!(file, "{:-^80}", " Input MIR ").unwrap();
    writeln!(file, "{:#?}", mir_expr).unwrap();

    if let Some(z3_ast) = z3_ast {
        writeln!(file, "{:-^80}", " Input Z3 AST ").unwrap();
        writeln!(file, "{:#?}", z3_ast).unwrap();
    }

    if let Some(output_z3_ast) = output_z3_ast {
        writeln!(file, "{:-^80}", " Output Z3 AST ").unwrap();
        writeln!(file, "{:#?}", output_z3_ast).unwrap();
    }

    if let Some(output_mir_expr) = output_mir_expr {
        writeln!(file, "{:-^80}", " Output MIR ").unwrap();
        writeln!(file, "{:#?}", output_mir_expr).unwrap();
    }

    writeln!(file, "\n").unwrap();
}

#[test]
fn test_simplify() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("simplify.txt").unwrap();

    let variables = &[
        ("a".to_string(), MirDataType::Int),
        ("b".to_string(), MirDataType::Int),
        ("p".to_string(), MirDataType::Bool),
        ("q".to_string(), MirDataType::Bool),
    ]
    .into_iter()
    .collect();

    simplify_ast(file, variables, "a > 0 or true");
    simplify_ast(file, variables, "a = 1 and a = 1");
    simplify_ast(file, variables, "a = 1 and a = 2");
    simplify_ast(file, variables, "a = 1 and a != 1");
    simplify_ast(file, variables, "a = 1 and a != 2");
    simplify_ast(file, variables, "a = 1 and a < 2 ");
    simplify_ast(file, variables, "a = 1 and a < 1");
    simplify_ast(file, variables, "a = 1 and a <= 2");
    simplify_ast(file, variables, "a = 1 and a <= 1");
    simplify_ast(file, variables, "a = 1 and a > 0");
    simplify_ast(file, variables, "a = 1 and a > 1");
    simplify_ast(file, variables, "a = 1 and a >= 0");
    simplify_ast(file, variables, "a = 1 and a >= 1");
    simplify_ast(file, variables, "a != 1 and a = 1");
    simplify_ast(file, variables, "a != 1 and a = 2");
    simplify_ast(file, variables, "a != 1 and a != 1");
    simplify_ast(file, variables, "a != 1 and a != 2");
    simplify_ast(file, variables, "a != 1 and a < 1 ");
    simplify_ast(file, variables, "a != 1 and a < 2");
    simplify_ast(file, variables, "a != 1 and a <= 1");
    simplify_ast(file, variables, "a != 1 and a <= 2");
    simplify_ast(file, variables, "a != 1 and a > 1");
    simplify_ast(file, variables, "a != 1 and a > 0");
    simplify_ast(file, variables, "a != 1 and a >= 1");
    simplify_ast(file, variables, "a != 1 and a >= 0");
    simplify_ast(file, variables, "a < 5 and a = 10");
    simplify_ast(file, variables, "a < 5 and a = 2");
    simplify_ast(file, variables, "a < 5 and a != 10");
    simplify_ast(file, variables, "a < 5 and a != 2");
    simplify_ast(file, variables, "a < 5 and a <= 10 ");
    simplify_ast(file, variables, "a < 5 and a > 10");
    simplify_ast(file, variables, "a < 5 and a > 2");
    simplify_ast(file, variables, "a <= 1 and a >= 1");
    simplify_ast(file, variables, "a = 1 or a = 1");
    simplify_ast(file, variables, "a = 1 or a = 2");
    simplify_ast(file, variables, "a = 1 or a != 1");
    simplify_ast(file, variables, "a = 1 or a != 2");
    simplify_ast(file, variables, "a = 1 or a < 2 ");
    simplify_ast(file, variables, "a = 1 or a < 1");
    simplify_ast(file, variables, "a = 1 or a <= 2");
    simplify_ast(file, variables, "a = 1 or a <= 1");
    simplify_ast(file, variables, "a = 1 or a > 0");
    simplify_ast(file, variables, "a = 1 or a > 1");
    simplify_ast(file, variables, "a = 1 or a >= 0");
    simplify_ast(file, variables, "a = 1 or a >= 1");
    simplify_ast(file, variables, "a != 1 or a = 1");
    simplify_ast(file, variables, "a != 1 or a = 2");
    simplify_ast(file, variables, "a != 1 or a != 1");
    simplify_ast(file, variables, "a != 1 or a != 2");
    simplify_ast(file, variables, "a != 1 or a < 1 ");
    simplify_ast(file, variables, "a != 1 or a < 2");
    simplify_ast(file, variables, "a != 1 or a <= 1");
    simplify_ast(file, variables, "a != 1 or a <= 2");
    simplify_ast(file, variables, "a != 1 or a > 1");
    simplify_ast(file, variables, "a != 1 or a > 0");
    simplify_ast(file, variables, "a != 1 or a >= 1");
    simplify_ast(file, variables, "a != 1 or a >= 0");
    simplify_ast(file, variables, "a < 5 or a = 10");
    simplify_ast(file, variables, "a < 5 or a = 2");
    simplify_ast(file, variables, "a < 5 or a != 10");
    simplify_ast(file, variables, "a < 5 or a != 2");
    simplify_ast(file, variables, "a < 5 or a <= 10 ");
    simplify_ast(file, variables, "a < 5 or a > 10");
    simplify_ast(file, variables, "a < 5 or a > 2");
    simplify_ast(file, variables, "a <= 1 or a >= 1");
    simplify_ast(file, variables, "a + b < 1 and a >= 0");
    simplify_ast(file, variables, "a + b < 1 and a - b >= 0");
    simplify_ast(file, variables, "a < a + b");
    simplify_ast(file, variables, "1 - 30 * a < 20 * a + b");
    simplify_ast(file, variables, "a > 0 and b > 0");
    simplify_ast(file, variables, "a > 0 and b > 0 and a * b = 0");
}
