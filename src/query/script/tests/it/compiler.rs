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

use databend_common_ast::parser::run_parser;
use databend_common_ast::parser::script::script_stmts;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::ParseMode;
use databend_common_script::compile;
use goldenfile::Mint;

fn run_compiler(file: &mut dyn Write, src: &str) {
    let src = unindent::unindent(src);
    let src = src.trim();
    let tokens = tokenize_sql(src).unwrap();
    let ast = run_parser(
        &tokens,
        Dialect::PostgreSQL,
        ParseMode::Template,
        false,
        script_stmts,
    )
    .unwrap();
    match compile(&ast) {
        Ok(ir) => {
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{}", src).unwrap();
            writeln!(file, "---------- Output ---------").unwrap();
            for line in ir {
                writeln!(file, "{}", line).unwrap();
            }
            writeln!(file, "\n").unwrap();
        }
        Err(err) => {
            let report = err.display_with_sql(src).message().trim().to_string();
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{}", src).unwrap();
            writeln!(file, "---------- Output ----------").unwrap();
            writeln!(file, "{}", report).unwrap();
            writeln!(file, "\n").unwrap();
        }
    }
}

#[test]
fn test_compile() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("compiler.txt").unwrap();
    let cases = &[
        r#"
            CREATE TABLE t1 (a INT, b INT, c INT);
            INSERT INTO t1 VALUES (1, 2, 3);
            DROP TABLE t1;
        "#,
        r#"
            LET x := 1;
            LET y := x + 1;
            LET z RESULTSET := SELECT :y + 1;
        "#,
        r#"
            RETURN;
        "#,
        r#"
            LET x := 1;
            IF x < 0 THEN
                RETURN 'LESS THAN 0';
            ELSEIF x = 0 THEN
                RETURN 'EQUAL TO 0';
            ELSE
                RETURN 'GREATER THAN 0';
            END IF;
        "#,
        r#"
            LET x := 1;
            LET sum := 0;
            FOR x IN x TO x + 10 DO
                sum := sum + x;
            END FOR;
        "#,
        r#"
            FOR x IN REVERSE -100 TO 100 DO
                CONTINUE;
            END FOR;
        "#,
        r#"
            LET x RESULTSET := SELECT * FROM numbers(10);
            FOR row IN x DO
                LET y := row.number;
            END FOR;
        "#,
        r#"
            LET x := 1;
            WHILE x < 5 DO
                x := x + 1;
            END WHILE;
            RETURN x;
        "#,
        r#"
            LET x := 1;
            REPEAT
                x := x + 1;
            UNTIL x = 5
            END REPEAT;
        "#,
        r#"
            LOOP
                LOOP
                    IF rand() < 0.5 THEN BREAK;
                    ELSE CONTINUE loop_label;
                    END IF;
                END LOOP;
            END LOOP loop_label;
        "#,
        // case
        // if
    ];

    for case in cases {
        run_compiler(file, case);
    }
}

#[test]
fn test_compile_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("compiler-error.txt").unwrap();
    let cases = &[
        r#"
            LET x := y + 1;
        "#,
        r#"
            LET x := 1;
            LET x RESULTSET := SELECT 1;
        "#,
        r#"
            LET x RESULTSET := SELECT 1;
            LET x := 1;
        "#,
        r#"
            LET x RESULTSET := SELECT 1;
            LET y := x;
        "#,
        r#"
            LET x := 1;
            LET y := x.a;
        "#,
        r#"
            LET x := 'min';
            LET y := IDENTIFIER(:x)([1,2]);
        "#,
        r#"
            LET x := 1;
            LET y := :x + 1;
        "#,
        r#"
            LET x := 1;
            FOR row IN x DO
                BREAK;
            END FOR;
        "#,
        r#"
            BREAK;
        "#,
        r#"
            CONTINUE;
        "#,
        r#"
            LOOP
                BREAK foo;
            END LOOP bar;
        "#,
        r#"
            LOOP
                CONTINUE foo;
            END LOOP bar;
        "#,
    ];

    for case in cases {
        run_compiler(file, case);
    }
}
