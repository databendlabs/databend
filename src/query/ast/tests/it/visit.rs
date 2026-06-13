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

use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::MapAccessor;
use databend_common_ast::ast::Statement;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::Visitor;
use databend_common_ast::visit::VisitorMut;
use databend_common_ast::visit::Walk;
use databend_common_ast::visit::WalkMut;

fn parse_stmt(sql: &str) -> Statement {
    let tokens = tokenize_sql(sql).unwrap();
    let (stmt, _) = parse_sql(&tokens, Dialect::Experimental).unwrap();
    stmt
}

#[test]
fn test_visit2_collect_identifiers_and_break() {
    struct CollectOnce {
        idents: Vec<String>,
    }

    impl Visitor for CollectOnce {
        type Break = String;

        fn visit_identifier(&mut self, ident: &Identifier) -> Result<VisitControl<String>, !> {
            self.idents.push(ident.name.clone());
            if ident.name.eq_ignore_ascii_case("t2") {
                return Ok(VisitControl::Break(ident.name.clone()));
            }
            Ok(VisitControl::Continue)
        }
    }

    let tokens = tokenize_sql("a + foo(t1:b, t2)").unwrap();
    let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();

    let mut visitor = CollectOnce { idents: vec![] };
    let result = expr.walk(&mut visitor).unwrap();

    assert_eq!(result, VisitControl::Break("t2".to_string()));
    assert!(visitor.idents.iter().any(|s| s == "a"));
    assert!(visitor.idents.iter().any(|s| s == "foo"));
}

#[test]
fn test_visit2_skip_children_for_map_accessor() {
    struct SkipMapAccess {
        visited: Vec<String>,
    }

    impl Visitor for SkipMapAccess {
        fn visit_map_accessor(&mut self, accessor: &MapAccessor) -> Result<VisitControl, !> {
            self.visited.push(format!("map:{accessor:?}"));
            Ok(VisitControl::SkipChildren)
        }

        fn visit_identifier(&mut self, ident: &Identifier) -> Result<VisitControl, !> {
            self.visited.push(ident.name.clone());
            Ok(VisitControl::Continue)
        }
    }

    let tokens = tokenize_sql("obj:key").unwrap();
    let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();

    let mut visitor = SkipMapAccess { visited: vec![] };
    let result = expr.walk(&mut visitor).unwrap();

    assert_eq!(result, VisitControl::Continue);
    assert!(visitor.visited.iter().any(|s| s == "obj"));
    assert!(!visitor.visited.iter().any(|s| s == "key"));
}

#[test]
fn test_visit2_mut_rewrite_and_error() {
    #[derive(Default)]
    struct Rewriter {
        seen_calls: usize,
    }

    impl VisitorMut for Rewriter {
        type Error = String;

        fn visit_function_call(&mut self, call: &mut FunctionCall) -> Result<VisitControl, String> {
            self.seen_calls += 1;
            if call.name.name.eq_ignore_ascii_case("forbidden") {
                return Err("forbidden function".to_string());
            }
            if call.name.name.eq_ignore_ascii_case("sum") {
                call.name.name = "count".to_string();
                return Ok(VisitControl::SkipChildren);
            }
            Ok(VisitControl::Continue)
        }

        fn visit_identifier(&mut self, ident: &mut Identifier) -> Result<VisitControl, String> {
            if ident.name == "a" {
                ident.name = "renamed_a".to_string();
            }
            Ok(VisitControl::Continue)
        }
    }

    let tokens = tokenize_sql("sum(a)").unwrap();
    let mut expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();

    let mut visitor = Rewriter::default();
    let result = expr.walk_mut(&mut visitor).unwrap();
    assert_eq!(result, VisitControl::Continue);
    assert_eq!(visitor.seen_calls, 1);
    assert_eq!(expr.to_string(), "count(a)");

    let tokens = tokenize_sql("forbidden(a)").unwrap();
    let mut expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
    let err = expr.walk_mut(&mut visitor).unwrap_err();
    assert_eq!(err, "forbidden function".to_string());
}

#[test]
fn test_visit2_mut_rewrite_insert_and_replace_source_queries() {
    #[derive(Default)]
    struct RenameIdents;

    impl VisitorMut for RenameIdents {
        type Error = String;

        fn visit_identifier(&mut self, ident: &mut Identifier) -> Result<VisitControl, String> {
            match ident.name.as_str() {
                "src_col" => ident.name = "renamed_src_col".to_string(),
                "src_filter" => ident.name = "renamed_src_filter".to_string(),
                "grp" => ident.name = "renamed_grp".to_string(),
                _ => {}
            }
            Ok(VisitControl::Continue)
        }
    }

    let mut insert = parse_stmt("INSERT INTO target SELECT src_col FROM source WHERE src_filter");
    let mut replace =
        parse_stmt("REPLACE INTO target ON (k) SELECT src_col FROM source GROUP BY grp");

    insert.walk_mut(&mut RenameIdents).unwrap();
    replace.walk_mut(&mut RenameIdents).unwrap();

    assert_eq!(
        insert.to_string(),
        "INSERT INTO target SELECT renamed_src_col FROM source WHERE renamed_src_filter"
    );
    assert_eq!(
        replace.to_string(),
        "REPLACE INTO target ON (k) SELECT renamed_src_col FROM source GROUP BY renamed_grp"
    );
}

#[test]
fn test_visit2_mut_rewrite_temporal_clause_identifiers_and_exprs() {
    #[derive(Default)]
    struct RenameIdents;

    impl VisitorMut for RenameIdents {
        type Error = String;

        fn visit_identifier(&mut self, ident: &mut Identifier) -> Result<VisitControl, String> {
            match ident.name.as_str() {
                "db1" => ident.name = "db2".to_string(),
                "st1" => ident.name = "st2".to_string(),
                "offset_expr" => ident.name = "renamed_offset_expr".to_string(),
                _ => {}
            }
            Ok(VisitControl::Continue)
        }
    }

    let mut stmt = parse_stmt(
        "SELECT * FROM t CHANGES (INFORMATION => DEFAULT) AT (STREAM => db1.st1) END (OFFSET => offset_expr)",
    );

    stmt.walk_mut(&mut RenameIdents).unwrap();

    assert_eq!(
        stmt.to_string(),
        "SELECT * FROM t CHANGES (INFORMATION => DEFAULT) AT (STREAM => db2.st2) END (OFFSET => renamed_offset_expr)"
    );
}

#[test]
fn test_visit2_mut_rewrite_window_frame_bounds() {
    #[derive(Default)]
    struct RenameIdents;

    impl VisitorMut for RenameIdents {
        type Error = String;

        fn visit_identifier(&mut self, ident: &mut Identifier) -> Result<VisitControl, String> {
            match ident.name.as_str() {
                "start_bound" => ident.name = "renamed_start_bound".to_string(),
                "end_bound" => ident.name = "renamed_end_bound".to_string(),
                _ => {}
            }
            Ok(VisitControl::Continue)
        }
    }

    let mut stmt = parse_stmt(
        "SELECT sum(a) OVER (ROWS BETWEEN start_bound PRECEDING AND end_bound FOLLOWING) FROM t",
    );

    stmt.walk_mut(&mut RenameIdents).unwrap();

    assert_eq!(
        stmt.to_string(),
        "SELECT sum(a) OVER (ROWS BETWEEN renamed_start_bound PRECEDING AND renamed_end_bound FOLLOWING) FROM t"
    );
}

#[test]
fn test_visit2_read_group_by_variants() {
    #[derive(Default)]
    struct CollectIdents {
        idents: Vec<String>,
    }

    impl Visitor for CollectIdents {
        fn visit_identifier(&mut self, ident: &Identifier) -> Result<VisitControl, !> {
            self.idents.push(ident.name.clone());
            Ok(VisitControl::Continue)
        }
    }

    let stmt = parse_stmt(
        "SELECT a FROM t GROUP BY GROUPING SETS ((grp1), (grp2)), ROLLUP (grp3), CUBE (grp4)",
    );

    let mut visitor = CollectIdents::default();
    stmt.walk(&mut visitor).unwrap();

    assert!(visitor.idents.iter().any(|ident| ident == "grp1"));
    assert!(visitor.idents.iter().any(|ident| ident == "grp2"));
    assert!(visitor.idents.iter().any(|ident| ident == "grp3"));
    assert!(visitor.idents.iter().any(|ident| ident == "grp4"));
}

#[test]
fn test_visit2_mut_rewrite_select_hint_literals() {
    #[derive(Default)]
    struct RewriteHintLiteral;

    impl VisitorMut for RewriteHintLiteral {
        type Error = String;

        fn visit_expr(
            &mut self,
            expr: &mut databend_common_ast::ast::Expr,
        ) -> Result<VisitControl, String> {
            if let databend_common_ast::ast::Expr::Literal { span, value } = expr
                && matches!(value, Literal::String(_))
            {
                *expr = databend_common_ast::ast::Expr::Literal {
                    span: *span,
                    value: Literal::Null,
                };
                return Ok(VisitControl::SkipChildren);
            }

            Ok(VisitControl::Continue)
        }
    }

    let mut stmt = parse_stmt("SELECT /*+ SET_VAR(timezone='UTC') */ 1");

    stmt.walk_mut(&mut RewriteHintLiteral).unwrap();

    assert_eq!(stmt.to_string(), "SELECT /*+ SET_VAR(timezone=NULL)*/ 1");
}

#[test]
fn test_visit2_mut_rewrite_unpivot_identifiers() {
    #[derive(Default)]
    struct RenameIdents;

    impl VisitorMut for RenameIdents {
        type Error = String;

        fn visit_identifier(&mut self, ident: &mut Identifier) -> Result<VisitControl, String> {
            match ident.name.as_str() {
                "sales" => ident.name = "renamed_sales".to_string(),
                "month" => ident.name = "renamed_month".to_string(),
                "jan" => ident.name = "renamed_jan".to_string(),
                "feb" => ident.name = "renamed_feb".to_string(),
                _ => {}
            }
            Ok(VisitControl::Continue)
        }
    }

    let mut stmt =
        parse_stmt("SELECT * FROM monthly_sales_1 UNPIVOT(sales FOR month IN (jan, feb))");

    stmt.walk_mut(&mut RenameIdents).unwrap();

    assert_eq!(
        stmt.to_string(),
        "SELECT * FROM monthly_sales_1 UNPIVOT(renamed_sales FOR renamed_month IN (renamed_jan, renamed_feb))"
    );
}

#[test]
fn test_visit2_unset_workload_group_quotas_skips_quota_keys() {
    #[derive(Default)]
    struct CollectIdents {
        idents: Vec<String>,
    }

    impl Visitor for CollectIdents {
        fn visit_identifier(&mut self, ident: &Identifier) -> Result<VisitControl, !> {
            self.idents.push(ident.name.clone());
            Ok(VisitControl::Continue)
        }
    }

    let stmt = parse_stmt("ALTER WORKLOAD GROUP wg UNSET (cpu_quota, memory_quota)");

    let mut visitor = CollectIdents::default();
    stmt.walk(&mut visitor).unwrap();

    assert!(visitor.idents.iter().any(|ident| ident == "wg"));
    assert!(!visitor.idents.iter().any(|ident| ident == "cpu_quota"));
    assert!(!visitor.idents.iter().any(|ident| ident == "memory_quota"));
}

#[test]
fn test_visit2_mut_rewrite_with_cte_aliases() {
    #[derive(Default)]
    struct RenameIdents;

    impl VisitorMut for RenameIdents {
        type Error = String;

        fn visit_identifier(&mut self, ident: &mut Identifier) -> Result<VisitControl, String> {
            match ident.name.as_str() {
                "src_col" => ident.name = "renamed_src_col".to_string(),
                "cte_alias" => ident.name = "renamed_cte_alias".to_string(),
                _ => {}
            }
            Ok(VisitControl::Continue)
        }
    }

    let mut stmt = parse_stmt("WITH cte_alias AS (SELECT src_col FROM t) SELECT * FROM cte_alias");

    stmt.walk_mut(&mut RenameIdents).unwrap();

    assert_eq!(
        stmt.to_string(),
        "WITH renamed_cte_alias AS (SELECT renamed_src_col FROM t) SELECT * FROM renamed_cte_alias"
    );
}

#[test]
fn test_visit2_mut_rewrite_create_table_components() {
    #[derive(Default)]
    struct RenameIdents;

    impl VisitorMut for RenameIdents {
        type Error = String;

        fn visit_identifier(&mut self, ident: &mut Identifier) -> Result<VisitControl, String> {
            match ident.name.as_str() {
                "default_expr" => ident.name = "renamed_default_expr".to_string(),
                "check_expr" => ident.name = "renamed_check_expr".to_string(),
                "cluster_expr" => ident.name = "renamed_cluster_expr".to_string(),
                "src_col" => ident.name = "renamed_src_col".to_string(),
                "src_filter" => ident.name = "renamed_src_filter".to_string(),
                _ => {}
            }
            Ok(VisitControl::Continue)
        }
    }

    let mut stmt = parse_stmt(
        "CREATE TABLE t (a int DEFAULT default_expr CHECK (check_expr > 0)) CLUSTER BY(cluster_expr) AS SELECT src_col FROM src WHERE src_filter",
    );

    stmt.walk_mut(&mut RenameIdents).unwrap();

    assert_eq!(
        stmt.to_string(),
        "CREATE TABLE t (a Int32 DEFAULT renamed_default_expr CHECK (renamed_check_expr > 0)) CLUSTER BY LINEAR(renamed_cluster_expr) AS SELECT renamed_src_col FROM src WHERE renamed_src_filter"
    );
}

#[test]
fn test_visit2_mut_rewrite_alter_table_add_column_components() {
    #[derive(Default)]
    struct RenameIdents;

    impl VisitorMut for RenameIdents {
        type Error = String;

        fn visit_identifier(&mut self, ident: &mut Identifier) -> Result<VisitControl, String> {
            match ident.name.as_str() {
                "default_expr" => ident.name = "renamed_default_expr".to_string(),
                "check_expr" => ident.name = "renamed_check_expr".to_string(),
                "after_col" => ident.name = "renamed_after_col".to_string(),
                _ => {}
            }
            Ok(VisitControl::Continue)
        }
    }

    let mut stmt = parse_stmt(
        "ALTER TABLE t ADD COLUMN c int DEFAULT default_expr CHECK (check_expr > 0) AFTER after_col",
    );

    stmt.walk_mut(&mut RenameIdents).unwrap();

    assert_eq!(
        stmt.to_string(),
        "ALTER TABLE t ADD COLUMN c Int32 DEFAULT renamed_default_expr CHECK (renamed_check_expr > 0) AFTER renamed_after_col"
    );
}
