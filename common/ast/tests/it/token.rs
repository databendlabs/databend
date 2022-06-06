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

use std::fs::File;
use std::io::Write;

use common_ast::parser::token::*;
use common_exception::Result;
use goldenfile::Mint;

fn run_lexer(file: &mut File, source: &str) {
    let tokens = Tokenizer::new(source).collect::<Result<Vec<_>>>();
    match tokens {
        Ok(tokens) => {
            let tuples: Vec<_> = tokens
                .into_iter()
                .map(|token| (token.kind, token.text(), token.span))
                .collect();
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{}", source).unwrap();
            writeln!(file, "---------- Output ---------").unwrap();
            writeln!(file, "{:?}", tuples).unwrap();
            writeln!(file, "\n").unwrap();
        }
        Err(err) => {
            let report = err.message().trim().to_string();
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{}", source).unwrap();
            writeln!(file, "---------- Output ---------").unwrap();
            writeln!(file, "{}", report).unwrap();
            writeln!(file, "\n").unwrap();
        }
    }
}

#[test]
fn test_lexer() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("lexer.txt").unwrap();

    let cases = vec![
        r#""#,
        r#"x'deadbeef' -- a hex string\n 'a string literal\n escape quote by '' or \\\'. '"#,
        r#"'中文' '日本語'"#,
        r#"42 3.5 4. .001 5e2 1.925e-3 .38e+7 1.e-01 0xfff x'deedbeef'"#,
        r#"create table "user" (id int, name varchar /* the user name */);"#,
    ];

    for case in cases {
        run_lexer(&mut file, case);
    }
}

#[test]
fn test_lexer_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("lexer-error.txt").unwrap();

    let cases = vec![r#"select †∑∂ from t;"#];

    for case in cases {
        run_lexer(&mut file, case);
    }
}
