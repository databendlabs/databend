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

use common_ast::error::Error;
use common_ast::parser::token::*;
use logos::Span;
use pretty_assertions::assert_eq;

#[test]
fn test_lexer() {
    assert_lex("", &[(EOI, "", 0..0)]);
    assert_lex(
        "x'deadbeef' -- a hex string\n 'a string literal\n escape quote by '' or \\\'. '",
        &[
            (LiteralHex, "x'deadbeef'", 0..11),
            (
                LiteralString,
                "'a string literal\n escape quote by '' or \\'. '",
                29..75,
            ),
            (EOI, "", 75..75),
        ],
    );
    assert_lex("'中文' '日本語'", &[
        (LiteralString, "'中文'", 0..8),
        (LiteralString, "'日本語'", 9..20),
        (EOI, "", 20..20),
    ]);
    assert_lex("42 3.5 4. .001 5e2 1.925e-3 .38e+7 1.e-01", &[
        (LiteralNumber, "42", 0..2),
        (LiteralNumber, "3.5", 3..6),
        (LiteralNumber, "4.", 7..9),
        (LiteralNumber, ".001", 10..14),
        (LiteralNumber, "5e2", 15..18),
        (LiteralNumber, "1.925e-3", 19..27),
        (LiteralNumber, ".38e+7", 28..34),
        (LiteralNumber, "1.e-01", 35..41),
        (EOI, "", 41..41),
    ]);
    assert_lex(
        r#"create table "user" (id int, name varchar /* the user name */);"#,
        &[
            (CREATE, "create", 0..6),
            (TABLE, "table", 7..12),
            (QuotedIdent, "\"user\"", 13..19),
            (LParen, "(", 20..21),
            (Ident, "id", 21..23),
            (INT, "int", 24..27),
            (Comma, ",", 27..28),
            (Ident, "name", 29..33),
            (VARCHAR, "varchar", 34..41),
            (RParen, ")", 61..62),
            (SemiColon, ";", 62..63),
            (EOI, "", 63..63),
        ],
    )
}

#[test]
fn test_lexer_error() {
    assert_eq!(
        tokenise("select †∑∂ from t;").unwrap_err(),
        Error::UnrecognisedToken {
            rest: "†∑∂ from t;".to_string(),
            position: 7
        }
    );
}

fn assert_lex<'a>(source: &'a str, expected_tokens: &[(TokenKind, &'a str, Span)]) {
    let tokens = tokenise(source).unwrap();

    let tuples: Vec<_> = tokens
        .into_iter()
        .map(|token| (token.kind, token.text, token.span))
        .collect();

    assert_eq!(tuples, expected_tokens);
}
