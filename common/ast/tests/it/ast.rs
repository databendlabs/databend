// Copyright 2021 Datafuse Labs.
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

use common_ast::parser::ast::*;
use pretty_assertions::assert_eq;

#[test]
fn test_display_create_database() {
    let stmt = Statement::CreateDatabase {
        if_not_exists: true,
        name: Identifier {
            name: String::from("column"),
            quote: Some('`'),
        },
        engine: "".to_string(),
        options: vec![],
    };
    assert_eq!(
        format!("{}", stmt),
        r#"CREATE DATABASE IF NOT EXISTS `column`"#
    );
}

#[test]
fn test_display_create_table() {
    let stmt = Statement::CreateTable {
        if_not_exists: true,
        database: Some(Identifier {
            name: "db".to_owned(),
            quote: Some('`'),
        }),
        table: Identifier {
            name: "table".to_owned(),
            quote: Some('`'),
        },
        columns: vec![ColumnDefinition {
            name: Identifier {
                name: "column".to_owned(),
                quote: None,
            },
            data_type: TypeName::Int(None),
            nullable: false,
            default_value: Some(Literal::Number("123".to_owned())),
        }],
        engine: "".to_string(),
        options: vec![],
        like_db: None,
        like_table: None,
    };
    assert_eq!(
        format!("{}", stmt),
        r#"CREATE TABLE IF NOT EXISTS `db`.`table` (column INTEGER NOT NULL DEFAULT 123)"#
    );
}

#[test]
fn test_display_query() {
    let stmt = SelectStmt {
        distinct: true,
        select_list: vec![
            SelectTarget::Indirections(vec![
                Indirection::Identifier(Identifier {
                    name: "table".to_owned(),
                    quote: None,
                }),
                Indirection::Identifier(Identifier {
                    name: "column".to_owned(),
                    quote: None,
                }),
            ]),
            SelectTarget::Indirections(vec![Indirection::Star]),
        ],
        from: TableReference::Join(Join {
            op: JoinOperator::Inner,
            condition: JoinCondition::Natural,
            left: Box::new(TableReference::Table {
                database: None,
                table: Identifier {
                    name: "left_table".to_owned(),
                    quote: None,
                },
                alias: None,
            }),
            right: Box::new(TableReference::Table {
                database: None,
                table: Identifier {
                    name: "right_table".to_owned(),
                    quote: None,
                },
                alias: None,
            }),
        }),
        selection: Some(Expr::BinaryOp {
            op: BinaryOperator::Eq,
            left: Box::new(Expr::ColumnRef {
                database: None,
                table: None,
                column: Identifier {
                    name: "a".to_owned(),
                    quote: None,
                },
            }),
            right: Box::new(Expr::ColumnRef {
                database: None,
                table: None,
                column: Identifier {
                    name: "b".to_owned(),
                    quote: None,
                },
            }),
        }),
        group_by: vec![Expr::ColumnRef {
            database: None,
            table: None,
            column: Identifier {
                name: "a".to_owned(),
                quote: None,
            },
        }],
        having: Some(Expr::BinaryOp {
            op: BinaryOperator::NotEq,
            left: Box::new(Expr::ColumnRef {
                database: None,
                table: None,
                column: Identifier {
                    name: "a".to_owned(),
                    quote: None,
                },
            }),
            right: Box::new(Expr::ColumnRef {
                database: None,
                table: None,
                column: Identifier {
                    name: "b".to_owned(),
                    quote: None,
                },
            }),
        }),
    };

    assert_eq!(
        format!("{}", stmt),
        r#"SELECT DISTINCT table.column, * FROM left_table NATURAL INNER JOIN right_table WHERE a = b GROUP BY a HAVING a <> b"#
    );
}

#[test]
fn test_display_table_reference() {
    let table_ref = TableReference::Table {
        database: None,
        table: Identifier {
            name: "table".to_owned(),
            quote: None,
        },
        alias: Some(TableAlias {
            name: Identifier {
                name: "table1".to_owned(),
                quote: None,
            },
            columns: vec![],
        }),
    };
    assert_eq!(format!("{}", table_ref), "table AS table1");
}

#[test]
fn test_display_expr() {
    let expr = Expr::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expr::FunctionCall {
            distinct: true,
            name: "FUNC".to_owned(),
            args: vec![
                Expr::Cast {
                    expr: Box::new(Expr::Literal(Literal::Number("1".to_string()))),
                    target_type: TypeName::Int(None),
                },
                Expr::Between {
                    expr: Box::new(Expr::Literal(Literal::Number("1".to_string()))),
                    low: Box::new(Expr::Literal(Literal::Number("1".to_string()))),
                    high: Box::new(Expr::Literal(Literal::Number("1".to_string()))),
                    not: true,
                },
                Expr::InList {
                    expr: Box::new(Expr::Literal(Literal::Number("1".to_string()))),
                    list: vec![
                        Expr::Literal(Literal::Number("1".to_string())),
                        Expr::Literal(Literal::Number("1".to_string())),
                    ],
                    not: true,
                },
            ],
            params: vec![Literal::Number("123".to_owned())],
        }),
        right: Box::new(Expr::Case {
            operand: Some(Box::new(Expr::Literal(Literal::Number("1".to_string())))),
            conditions: vec![Expr::Literal(Literal::Number("1".to_string()))],
            results: vec![Expr::Literal(Literal::Number("1".to_string()))],
            else_result: Some(Box::new(Expr::Literal(Literal::Number("1".to_string())))),
        }),
    };

    assert_eq!(
        format!("{}", expr),
        r#"FUNC(123)(DISTINCT CAST(1 AS INTEGER), 1 NOT BETWEEN 1 AND 1, 1 NOT IN(1, 1)) AND CASE 1 WHEN 1 THEN 1 ELSE 1 END"#
    );
}
