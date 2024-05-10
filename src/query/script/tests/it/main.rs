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

#![feature(try_blocks)]

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_ast::parser::run_parser;
use databend_common_ast::parser::script::script_stmts;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::ParseMode;
use databend_common_exception::ErrorCode;
use databend_common_exception::Range;
use databend_common_exception::Result;
use databend_common_script::compile;
use databend_common_script::ir::ColumnAccess;
use databend_common_script::Client;
use databend_common_script::Executor;
use goldenfile::Mint;
use tokio::runtime::Runtime;

fn run_script(file: &mut dyn Write, src: &str) {
    let src = unindent::unindent(src);
    let src = src.trim();

    let res: Result<_> = try {
        let tokens = tokenize_sql(src).unwrap();
        let ast = run_parser(
            &tokens,
            Dialect::PostgreSQL,
            ParseMode::Template,
            false,
            script_stmts,
        )?;
        let ir = compile(&ast)?;
        let client = mock_client();
        let query_log = client.query_log.clone();
        let mut executor = Executor::load(None, client, ir.clone());
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(executor.run(1000))?;

        (ir, query_log, result)
    };

    match res {
        Ok((ir, query_log, result)) => {
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{}", src).unwrap();
            writeln!(file, "---------- IR -------------").unwrap();
            for line in ir {
                writeln!(file, "{}", line).unwrap();
            }
            writeln!(file, "---------- QUERY ---------").unwrap();
            for (query, block) in query_log.lock().unwrap().iter() {
                writeln!(file, "QUERY: {}", query).unwrap();
                writeln!(file, "BLOCK: {}", block).unwrap();
            }
            writeln!(file, "---------- Output ---------").unwrap();
            writeln!(file, "{:?}", result).unwrap();
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
fn test_script() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("script.txt").unwrap();

    run_script(
        file,
        r#"
            CREATE OR REPLACE TABLE t1 (a INT, b INT, c INT);
            INSERT INTO t1 VALUES (1, 2, 3);
            RETURN TABLE(SELECT a FROM t1);
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            LET x := x + 1;
            LET x RESULTSET := SELECT :x + 1;
            RETURN TABLE(x);
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            RETURN TABLE(SELECT :x + 1);
        "#,
    );
    run_script(
        file,
        r#"
            LET x RESULTSET := SELECT 1;
            LET x := 1;
            RETURN x;
        "#,
    );
    run_script(
        file,
        r#"
            LET table := 1;
            RETURN table;
        "#,
    );
    run_script(
        file,
        r#"
            RETURN;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            LET sum := 0;
            FOR x IN x TO x + 2 DO
                sum := sum + x;
            END FOR;
            RETURN sum;
        "#,
    );
    run_script(
        file,
        r#"
            LET sum := 0;
            FOR x IN REVERSE -1 TO 1 DO
                sum := sum + x;
            END FOR;
            RETURN sum;
        "#,
    );
    run_script(
        file,
        r#"
            LET x RESULTSET := SELECT * FROM numbers(3);
            LET sum := 0;
            FOR row IN x DO
                sum := sum + row.number;
            END FOR;
            RETURN sum;
        "#,
    );
    run_script(
        file,
        r#"
            LET sum := 0;
            FOR row IN SELECT * FROM numbers(3) DO
                sum := sum + row.number;
            END FOR;
            RETURN sum;
        "#,
    );
    run_script(
        file,
        r#"
            LET x RESULTSET := SELECT * FROM numbers(3);
            LET sum := 0;
            FOR x IN x DO
                LET x := x.number;
                sum := sum + x;
            END FOR x;
            RETURN sum;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            WHILE x < 3 DO
                x := x + 1;
            END WHILE;
            RETURN x;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            REPEAT
                x := x + 1;
            UNTIL x = 3
            END REPEAT;
            RETURN x;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            REPEAT
                x := x + 1;
                LET y := 3;
            UNTIL x = y
            END REPEAT;
            RETURN x;
        "#,
    );
    run_script(
        file,
        r#"
            LOOP
                LET x := 0;
                LOOP
                    LET y := x;
                    IF y < 2 THEN
                        x := x + 1;
                        CONTINUE;
                    ELSE
                        BREAK loop_label;
                    END IF;
                END LOOP;
            END LOOP loop_label;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 0;
            LOOP
                LET x := 1;
                BREAK;
            END LOOP;
            RETURN x;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            CASE x
                WHEN 1 THEN RETURN 'ONE';
                WHEN 2 THEN RETURN 'TWO';
                ELSE RETURN 'OTHER';
            END CASE;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 2;
            CASE x
                WHEN 1 THEN RETURN 'ONE';
                WHEN 2 THEN RETURN 'TWO';
                ELSE RETURN 'OTHER';
            END CASE;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 3;
            CASE x
                WHEN 1 THEN RETURN 'ONE';
                WHEN 2 THEN RETURN 'TWO';
                ELSE RETURN 'OTHER';
            END CASE;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            CASE
                WHEN x = 1 THEN RETURN 'ONE';
                WHEN x = 2 THEN RETURN 'TWO';
                ELSE RETURN 'OTHER';
            END CASE;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 2;
            CASE
                WHEN x = 1 THEN RETURN 'ONE';
                WHEN x = 2 THEN RETURN 'TWO';
                ELSE RETURN 'OTHER';
            END CASE;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 3;
            CASE
                WHEN x = 1 THEN RETURN 'ONE';
                WHEN x = 2 THEN RETURN 'TWO';
                ELSE RETURN 'OTHER';
            END CASE;
        "#,
    );
}

#[test]
fn test_script_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("script-error.txt").unwrap();

    run_script(
        file,
        r#"
            LET x := y + 1;
        "#,
    );
    run_script(
        file,
        r#"
            LET x RESULTSET := SELECT 1;
            LET y := x;
        "#,
    );
    run_script(
        file,
        r#"
            RETURN TABLE(x);
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            RETURN TABLE(x);
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            LET x RESULTSET := SELECT 1;
            RETURN x;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            LET y := x.a;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 'min';
            LET y := IDENTIFIER(:x)([1,2]);
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            LET y := :x + 1;
        "#,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            FOR row IN x DO
                BREAK;
            END FOR;
        "#,
    );
    run_script(
        file,
        r#"
            BREAK;
        "#,
    );
    run_script(
        file,
        r#"
            CONTINUE;
        "#,
    );
    run_script(
        file,
        r#"
            LOOP
                BREAK foo;
            END LOOP bar;
        "#,
    );
    run_script(
        file,
        r#"
            LOOP
                CONTINUE foo;
            END LOOP bar;
        "#,
    );
    run_script(
        file,
        r#"
            LOOP
                CONTINUE;
            END LOOP;
        "#,
    );
    run_script(
        file,
        r#"
            LET zero := 0;
            LET y := 1 + zero / 0;
        "#,
    );
    run_script(
        file,
        r#"
            LET zero := 0;
            SELECT 1 + :zero / 0;
        "#,
    );
    run_script(
        file,
        r#"
            LET zero := 0;
            RETURN 1 + zero / 0;
        "#,
    );
    run_script(
        file,
        r#"
            LET zero := 0;
            RETURN TABLE(SELECT 1 + :zero / 0);
        "#,
    );
    run_script(
        file,
        r#"
            FOR x IN REVERSE 3 TO 1 DO
                RETURN;
            END FOR;
        "#,
    );
}

fn mock_client() -> MockClient {
    MockClient::new()
        .response_when(
            "SELECT * FROM numbers(3)",
            MockSet::named(vec!["number"], vec![
                vec![Literal::UInt64(0)],
                vec![Literal::UInt64(1)],
                vec![Literal::UInt64(2)],
            ]),
        )
        .response_when(
            "CREATE OR REPLACE TABLE t1 (a Int32, b Int32, c Int32)",
            MockSet::empty(),
        )
        .response_when("INSERT INTO t1 VALUES (1, 2, 3)", MockSet::empty())
        .response_when(
            "SELECT a FROM t1",
            MockSet::named(vec!["a"], vec![vec![Literal::UInt64(1)]]),
        )
        .response_when(
            "SELECT * FROM generate_series(1, 1 + 2, 1)",
            MockSet::unnamed(vec![
                vec![Literal::UInt64(1)],
                vec![Literal::UInt64(2)],
                vec![Literal::UInt64(3)],
            ]),
        )
        .response_when(
            "SELECT * FROM generate_series(1, - 1, -1)",
            MockSet::unnamed(vec![
                vec![Literal::UInt64(1)],
                vec![Literal::UInt64(0)],
                vec![Literal::Decimal256 {
                    value: (-1).into(),
                    precision: 1,
                    scale: 0,
                }],
            ]),
        )
        .response_when("SELECT 0", MockSet::unnamed(vec![vec![Literal::UInt64(0)]]))
        .response_when("SELECT 1", MockSet::unnamed(vec![vec![Literal::UInt64(1)]]))
        .response_when("SELECT 2", MockSet::unnamed(vec![vec![Literal::UInt64(2)]]))
        .response_when("SELECT 3", MockSet::unnamed(vec![vec![Literal::UInt64(3)]]))
        .response_when("SELECT 6", MockSet::unnamed(vec![vec![Literal::UInt64(6)]]))
        .response_when(
            "SELECT 'ONE'",
            MockSet::unnamed(vec![vec![Literal::String("ONE".to_string())]]),
        )
        .response_when(
            "SELECT 'TWO'",
            MockSet::unnamed(vec![vec![Literal::String("TWO".to_string())]]),
        )
        .response_when(
            "SELECT 'OTHER'",
            MockSet::unnamed(vec![vec![Literal::String("OTHER".to_string())]]),
        )
        .response_when(
            "SELECT 0 + 0",
            MockSet::unnamed(vec![vec![Literal::UInt64(0)]]),
        )
        .response_when(
            "SELECT 0 + 1",
            MockSet::unnamed(vec![vec![Literal::UInt64(1)]]),
        )
        .response_when(
            "SELECT 1 + 0",
            MockSet::unnamed(vec![vec![Literal::UInt64(1)]]),
        )
        .response_when(
            "SELECT 1 + -1",
            MockSet::unnamed(vec![vec![Literal::UInt64(0)]]),
        )
        .response_when(
            "SELECT 1 + 1",
            MockSet::unnamed(vec![vec![Literal::UInt64(2)]]),
        )
        .response_when(
            "SELECT 1 + 2",
            MockSet::unnamed(vec![vec![Literal::UInt64(3)]]),
        )
        .response_when(
            "SELECT 2 + 1",
            MockSet::unnamed(vec![vec![Literal::UInt64(3)]]),
        )
        .response_when(
            "SELECT 3 + 3",
            MockSet::unnamed(vec![vec![Literal::UInt64(6)]]),
        )
        .response_when(
            "SELECT 3 * 3",
            MockSet::unnamed(vec![vec![Literal::UInt64(9)]]),
        )
        .response_when(
            "SELECT is_true(0 < 2)",
            MockSet::unnamed(vec![vec![Literal::Boolean(true)]]),
        )
        .response_when(
            "SELECT is_true(1 < 2)",
            MockSet::unnamed(vec![vec![Literal::Boolean(true)]]),
        )
        .response_when(
            "SELECT is_true(2 < 2)",
            MockSet::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT is_true(1 = 1)",
            MockSet::unnamed(vec![vec![Literal::Boolean(true)]]),
        )
        .response_when(
            "SELECT is_true(2 = 1)",
            MockSet::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT is_true(2 = 2)",
            MockSet::unnamed(vec![vec![Literal::Boolean(true)]]),
        )
        .response_when(
            "SELECT is_true(2 = 3)",
            MockSet::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT is_true(3 = 1)",
            MockSet::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT is_true(3 = 2)",
            MockSet::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT is_true(3 = 3)",
            MockSet::unnamed(vec![vec![Literal::Boolean(true)]]),
        )
        .response_when(
            "SELECT NOT is_true(1 < 3)",
            MockSet::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT NOT is_true(2 < 3)",
            MockSet::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT NOT is_true(3 < 3)",
            MockSet::unnamed(vec![vec![Literal::Boolean(true)]]),
        )
        .throw_error_when(
            "SELECT 1 + 0 / 0",
            ErrorCode::BadArguments("division by zero".to_string())
                .set_span(Some(Range { start: 13, end: 14 })),
        )
        .throw_error_when(
            "SELECT * FROM generate_series(1, 3, -1)",
            ErrorCode::BadArguments(
                "start must be greater than or equal to end when step is negative".to_string(),
            )
            .set_span(Some(Range { start: 14, end: 39 })),
        )
}

#[derive(Debug, Clone)]
struct MockClient {
    responses: HashMap<String, MockSet>,
    throw_errors: HashMap<String, ErrorCode>,
    query_log: Arc<Mutex<Vec<(String, MockSet)>>>,
}

impl MockClient {
    pub fn new() -> Self {
        MockClient {
            responses: HashMap::new(),
            throw_errors: HashMap::new(),
            query_log: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn response_when(mut self, query: &str, block: MockSet) -> Self {
        self.responses.insert(query.to_string(), block);
        self
    }

    pub fn throw_error_when(mut self, query: &str, err: ErrorCode) -> Self {
        self.throw_errors.insert(query.to_string(), err);
        self
    }
}

impl Client for MockClient {
    type Var = Literal;
    type Set = MockSet;

    async fn query(&self, query: &str) -> Result<Self::Set> {
        if let Some(block) = self.responses.get(query) {
            self.query_log
                .lock()
                .unwrap()
                .push((query.to_string(), block.clone()));
            return Ok(block.clone());
        }

        if let Some(err) = self.throw_errors.get(query) {
            return Err(err.clone());
        }

        panic!("response to query is not defined: {query}")
    }

    fn var_to_ast(&self, scalar: &Self::Var) -> Result<Expr> {
        Ok(Expr::Literal {
            span: None,
            value: scalar.clone(),
        })
    }

    fn read_from_set(&self, set: &Self::Set, row: usize, col: &ColumnAccess) -> Result<Self::Var> {
        let var = match col {
            ColumnAccess::Position(col) => set.data[row][*col].clone(),
            ColumnAccess::Name(name) => {
                let col = set.column_names.iter().position(|x| x == name).unwrap();
                set.data[row][col].clone()
            }
        };
        Ok(var)
    }

    fn set_len(&self, set: &Self::Set) -> usize {
        set.data.len()
    }

    fn is_true(&self, scalar: &Self::Var) -> Result<bool> {
        Ok(*scalar == Literal::Boolean(true))
    }
}

#[derive(Debug, Clone)]
struct MockSet {
    column_names: Vec<String>,
    data: Vec<Vec<Literal>>,
}

impl MockSet {
    pub fn empty() -> Self {
        MockSet {
            column_names: vec![],
            data: vec![],
        }
    }

    pub fn unnamed(data: Vec<Vec<Literal>>) -> Self {
        MockSet {
            column_names: (0..data[0].len()).map(|x| format!("${x}")).collect(),
            data,
        }
    }

    pub fn named(
        column_names: impl IntoIterator<Item = &'static str>,
        data: Vec<Vec<Literal>>,
    ) -> Self {
        MockSet {
            column_names: column_names.into_iter().map(|x| x.to_string()).collect(),
            data,
        }
    }
}

impl Display for MockSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, name) in self.column_names.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", name)?;
        }
        write!(f, "): ")?;
        for (i, row) in self.data.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "(")?;
            for (j, cell) in row.iter().enumerate() {
                if j > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", cell)?;
            }
            write!(f, ")")?;
        }
        Ok(())
    }
}
