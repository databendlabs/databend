// Copyright 2021 Datafuse Labs
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

use std::cell::Cell;
use std::cell::RefCell;

use crate::ast::Expr;
use crate::ast::FunctionCall;
use crate::ast::Identifier;
use crate::ast::Literal;
use crate::ast::Statement;
use crate::ast::UnaryOperator;
use crate::visitor::StatementReplacer;

fn json_to_expr(value: &serde_json::Value) -> Expr {
    match value {
        serde_json::Value::Null => Expr::Literal {
            span: None,
            value: Literal::Null,
        },
        serde_json::Value::Bool(b) => Expr::Literal {
            span: None,
            value: Literal::Boolean(*b),
        },
        serde_json::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                Expr::Literal {
                    span: None,
                    value: Literal::UInt64(u),
                }
            } else if let Some(i) = n.as_i64() {
                Expr::UnaryOp {
                    span: None,
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::Literal {
                        span: None,
                        value: Literal::UInt64(i.unsigned_abs()),
                    }),
                }
            } else if let Some(f) = n.as_f64() {
                Expr::Literal {
                    span: None,
                    value: Literal::Float64(f),
                }
            } else {
                Expr::Literal {
                    span: None,
                    value: Literal::String(n.to_string()),
                }
            }
        }
        serde_json::Value::String(s) => Expr::Literal {
            span: None,
            value: Literal::String(s.clone()),
        },
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Expr::FunctionCall {
            span: None,
            func: FunctionCall {
                name: Identifier::from_name(None, "parse_json"),
                args: vec![Expr::Literal {
                    span: None,
                    value: Literal::String(serde_json::to_string(value).unwrap_or_default()),
                }],
                ..Default::default()
            },
        },
    }
}

pub fn substitute_params(stmt: &mut Statement, params: &serde_json::Value) -> Result<(), String> {
    let pos_index = Cell::new(0usize);
    let error: RefCell<Option<String>> = RefCell::new(None);

    let replace_expr = |expr: &mut Expr| {
        if error.borrow().is_some() {
            return;
        }
        match expr {
            Expr::Placeholder { .. } => {
                let arr = match params.as_array() {
                    Some(arr) => arr,
                    None => {
                        *error.borrow_mut() = Some(
                            "params must be a JSON array for positional placeholders (?)".into(),
                        );
                        return;
                    }
                };
                let idx = pos_index.get();
                pos_index.set(idx + 1);
                if idx >= arr.len() {
                    *error.borrow_mut() = Some(format!(
                        "not enough parameters: placeholder index {} but only {} params provided",
                        idx + 1,
                        arr.len()
                    ));
                    return;
                }
                *expr = json_to_expr(&arr[idx]);
            }
            Expr::Hole { name, .. } => {
                if let Some(obj) = params.as_object() {
                    match obj.get(name.as_str()) {
                        Some(val) => {
                            *expr = json_to_expr(val);
                        }
                        None => {
                            *error.borrow_mut() = Some(format!("missing named parameter: :{name}"));
                        }
                    }
                } else {
                    *error.borrow_mut() =
                        Some("params must be a JSON object for named placeholders (:name)".into());
                }
            }
            _ => {}
        }
    };

    let mut replacer = StatementReplacer::new(replace_expr, |_: &mut crate::ast::Identifier| {});
    replacer.visit(stmt);

    let error = error.into_inner();
    if let Some(err) = error {
        return Err(err);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Query;
    use crate::ast::SelectTarget;
    use crate::ast::SetExpr;
    use crate::parser::parse_sql;
    use crate::parser::tokenize_sql;

    fn parse_stmt(sql: &str) -> Statement {
        let tokens = tokenize_sql(sql).unwrap();
        let (stmt, _) = parse_sql(&tokens, crate::parser::Dialect::PostgreSQL).unwrap();
        stmt
    }

    fn extract_select_exprs(stmt: &Statement) -> Vec<&Expr> {
        let Statement::Query(query) = stmt else {
            panic!("expected Statement::Query");
        };
        let Query { body, .. } = query.as_ref();
        let SetExpr::Select(select) = body else {
            panic!("expected SetExpr::Select");
        };
        select
            .select_list
            .iter()
            .map(|t| match t {
                SelectTarget::AliasedExpr { expr, .. } => expr.as_ref(),
                other => panic!("expected AliasedExpr, got: {other:?}"),
            })
            .collect()
    }

    fn assert_string_literal(expr: &Expr) -> &str {
        match expr {
            Expr::Literal {
                value: Literal::String(s),
                ..
            } => s.as_str(),
            other => panic!("expected Literal::String, got: {other:?}"),
        }
    }

    #[test]
    fn test_positional_placeholder() {
        let mut stmt = parse_stmt("SELECT ?, ?");
        let params = serde_json::json!([42, "hello"]);
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(result.contains("42"), "expected 42 in: {result}");
        assert!(result.contains("'hello'"), "expected 'hello' in: {result}");
    }

    #[test]
    fn test_named_placeholder() {
        let mut stmt = parse_stmt("SELECT :age, :name");
        let params = serde_json::json!({"age": 30, "name": "Alice"});
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(result.contains("30"), "expected 30 in: {result}");
        assert!(result.contains("'Alice'"), "expected 'Alice' in: {result}");
    }

    #[test]
    fn test_null_and_bool_params() {
        let mut stmt = parse_stmt("SELECT ?, ?, ?");
        let params = serde_json::json!([null, true, false]);
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(result.contains("NULL"), "expected NULL in: {result}");
        assert!(result.contains("TRUE"), "expected TRUE in: {result}");
        assert!(result.contains("FALSE"), "expected FALSE in: {result}");
    }

    #[test]
    fn test_negative_number() {
        let mut stmt = parse_stmt("SELECT ?");
        let params = serde_json::json!([-42]);
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(
            result.contains("- 42") || result.contains("-42"),
            "expected negative 42 in: {result}"
        );
    }

    #[test]
    fn test_not_enough_params() {
        let mut stmt = parse_stmt("SELECT ?, ?");
        let params = serde_json::json!([42]);
        let result = substitute_params(&mut stmt, &params);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not enough parameters"));
    }

    #[test]
    fn test_missing_named_param() {
        let mut stmt = parse_stmt("SELECT :name");
        let params = serde_json::json!({"age": 30});
        let result = substitute_params(&mut stmt, &params);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("missing named parameter"));
    }

    #[test]
    fn test_wrong_param_type_for_positional() {
        let mut stmt = parse_stmt("SELECT ?");
        let params = serde_json::json!({"name": "Alice"});
        let result = substitute_params(&mut stmt, &params);
        assert!(result.is_err());
    }

    #[test]
    fn test_no_placeholders() {
        let mut stmt = parse_stmt("SELECT 1, 'hello'");
        let params = serde_json::json!([]);
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(result.contains('1'), "expected 1 in: {result}");
        assert!(result.contains("'hello'"), "expected 'hello' in: {result}");
    }

    #[test]
    fn test_float_param() {
        let mut stmt = parse_stmt("SELECT ?");
        let params = serde_json::json!([1.5]);
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(result.contains("1.5"), "expected 1.5 in: {result}");
    }

    #[test]
    fn test_zero_param() {
        let mut stmt = parse_stmt("SELECT ?");
        let params = serde_json::json!([0]);
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(result.contains('0'), "expected 0 in: {result}");
    }

    #[test]
    fn test_large_uint_param() {
        let mut stmt = parse_stmt("SELECT ?");
        let params = serde_json::json!([u64::MAX]);
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(
            result.contains(&u64::MAX.to_string()),
            "expected u64::MAX in: {result}"
        );
    }

    #[test]
    fn test_empty_string_param() {
        let mut stmt = parse_stmt("SELECT ?");
        let params = serde_json::json!([""]);
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(
            result.contains("''"),
            "expected empty string '' in: {result}"
        );
    }

    #[test]
    fn test_json_array_param() {
        let mut stmt = parse_stmt("SELECT ?");
        let params = serde_json::json!([["a", "b", "c"]]);
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(
            result.contains("parse_json"),
            "expected parse_json wrapper in: {result}"
        );
        assert!(
            result.contains(r#"["a","b","c"]"#),
            "expected JSON array string in: {result}"
        );
    }

    #[test]
    fn test_json_object_param() {
        let mut stmt = parse_stmt("SELECT ?");
        let params = serde_json::json!([{"key": "value"}]);
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(
            result.contains("parse_json"),
            "expected parse_json wrapper in: {result}"
        );
        assert!(
            result.contains("key") && result.contains("value"),
            "expected JSON object string in: {result}"
        );
    }

    #[test]
    fn test_sql_injection_single_quote() {
        let mut stmt = parse_stmt("SELECT ?");
        let payload = "Robert'); DROP TABLE students;--";
        let params = serde_json::json!([payload]);
        substitute_params(&mut stmt, &params).unwrap();
        let exprs = extract_select_exprs(&stmt);
        assert_eq!(exprs.len(), 1);
        let val = assert_string_literal(exprs[0]);
        assert_eq!(val, payload);
    }

    #[test]
    fn test_sql_injection_double_dash_comment() {
        let mut stmt = parse_stmt("SELECT ?");
        let payload = "1 -- comment";
        let params = serde_json::json!([payload]);
        substitute_params(&mut stmt, &params).unwrap();
        let exprs = extract_select_exprs(&stmt);
        let val = assert_string_literal(exprs[0]);
        assert_eq!(val, payload);
    }

    #[test]
    fn test_sql_injection_semicolon() {
        let mut stmt = parse_stmt("SELECT ?");
        let payload = "1; DROP TABLE users";
        let params = serde_json::json!([payload]);
        substitute_params(&mut stmt, &params).unwrap();
        let exprs = extract_select_exprs(&stmt);
        let val = assert_string_literal(exprs[0]);
        assert_eq!(val, payload);
    }

    #[test]
    fn test_sql_injection_union_select() {
        let mut stmt = parse_stmt("SELECT ?");
        let payload = "' UNION SELECT * FROM passwords --";
        let params = serde_json::json!([payload]);
        substitute_params(&mut stmt, &params).unwrap();
        let exprs = extract_select_exprs(&stmt);
        let val = assert_string_literal(exprs[0]);
        assert_eq!(val, payload);
    }

    #[test]
    fn test_sql_injection_named_params() {
        let mut stmt = parse_stmt("SELECT :name");
        let payload = "'; DROP TABLE users; --";
        let params = serde_json::json!({"name": payload});
        substitute_params(&mut stmt, &params).unwrap();
        let exprs = extract_select_exprs(&stmt);
        let val = assert_string_literal(exprs[0]);
        assert_eq!(val, payload);
    }

    #[test]
    fn test_sql_injection_backslash_escape() {
        let mut stmt = parse_stmt("SELECT ?");
        let payload = "value\\' OR '1'='1";
        let params = serde_json::json!([payload]);
        substitute_params(&mut stmt, &params).unwrap();
        let exprs = extract_select_exprs(&stmt);
        let val = assert_string_literal(exprs[0]);
        assert_eq!(val, payload);
    }

    #[test]
    fn test_sql_injection_numeric_string_not_promoted() {
        let mut stmt = parse_stmt("SELECT ?");
        let params = serde_json::json!(["42"]);
        substitute_params(&mut stmt, &params).unwrap();
        let exprs = extract_select_exprs(&stmt);
        let val = assert_string_literal(exprs[0]);
        assert_eq!(val, "42");
    }

    #[test]
    fn test_sql_injection_null_byte() {
        let mut stmt = parse_stmt("SELECT ?");
        let payload = "hello\x00world";
        let params = serde_json::json!([payload]);
        substitute_params(&mut stmt, &params).unwrap();
        let exprs = extract_select_exprs(&stmt);
        let val = assert_string_literal(exprs[0]);
        assert_eq!(val, payload);
    }

    #[test]
    fn test_sql_injection_multiline() {
        let mut stmt = parse_stmt("SELECT ?");
        let payload = "x'\nDROP TABLE users;\n--";
        let params = serde_json::json!([payload]);
        substitute_params(&mut stmt, &params).unwrap();
        let exprs = extract_select_exprs(&stmt);
        let val = assert_string_literal(exprs[0]);
        assert_eq!(val, payload);
    }

    #[test]
    fn test_sql_injection_statement_is_still_select() {
        let mut stmt = parse_stmt("SELECT ?, ?");
        let params = serde_json::json!(["'; DROP TABLE users; --", "' OR '1'='1"]);
        substitute_params(&mut stmt, &params).unwrap();
        assert!(
            matches!(&stmt, Statement::Query(_)),
            "statement should still be a SELECT query, got: {stmt}"
        );
        let exprs = extract_select_exprs(&stmt);
        assert_eq!(exprs.len(), 2);
        assert_string_literal(exprs[0]);
        assert_string_literal(exprs[1]);
    }

    #[test]
    fn test_where_clause_positional() {
        let mut stmt = parse_stmt("SELECT * FROM t WHERE id = ? AND name = ?");
        let params = serde_json::json!([42, "Alice"]);
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(result.contains("42"), "expected 42 in: {result}");
        assert!(result.contains("'Alice'"), "expected 'Alice' in: {result}");
        assert!(
            !result.contains('?'),
            "no placeholders should remain in: {result}"
        );
    }

    #[test]
    fn test_where_clause_named() {
        let mut stmt = parse_stmt("SELECT * FROM t WHERE id = :id AND name = :name");
        let params = serde_json::json!({"id": 1, "name": "Bob"});
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(!result.contains(":id"), "no :id should remain in: {result}");
        assert!(
            !result.contains(":name"),
            "no :name should remain in: {result}"
        );
    }

    #[test]
    fn test_insert_values_positional() {
        let mut stmt = parse_stmt("INSERT INTO t (a, b, c) VALUES (?, ?, ?)");
        let params = serde_json::json!([1, "hello", true]);
        substitute_params(&mut stmt, &params).unwrap();
        let result = stmt.to_string();
        assert!(result.contains('1'), "expected 1 in: {result}");
        assert!(result.contains("'hello'"), "expected 'hello' in: {result}");
        assert!(result.contains("TRUE"), "expected TRUE in: {result}");
    }
}
