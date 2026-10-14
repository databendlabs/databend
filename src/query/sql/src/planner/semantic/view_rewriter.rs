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

use std::collections::HashSet;

use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::TableReference;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::Visitor;
use databend_common_ast::visit::VisitorMut;
use databend_common_ast::visit::Walk;

use crate::NameResolutionContext;
use crate::normalize_identifier;

/// Qualifies table references without qualifying possible CTE references.
///
/// CTE definitions may be bound eagerly (materialized) or in the referencing scope
/// (non-materialized). An AST walk cannot decide which binding a name will have. Leave
/// every name that could refer to a CTE unqualified, and let the binder resolve it in
/// the stored definition's default catalog/database. Other table names retain the
/// existing database qualification, as do references the user qualified explicitly.
pub struct ViewRewriter {
    current_database: String,
    name_resolution_ctx: NameResolutionContext,
    cte_names: Option<HashSet<String>>,
}

impl ViewRewriter {
    pub fn new(
        current_database: impl Into<String>,
        name_resolution_ctx: NameResolutionContext,
    ) -> Self {
        Self {
            current_database: current_database.into(),
            name_resolution_ctx,
            cte_names: None,
        }
    }
}

impl VisitorMut for ViewRewriter {
    fn visit_query(&mut self, query: &mut Query) -> Result<VisitControl, !> {
        if self.cte_names.is_none() {
            let mut collector = CteNameCollector {
                names: HashSet::new(),
                name_resolution_ctx: &self.name_resolution_ctx,
            };
            let Ok(_) = query.walk(&mut collector);
            self.cte_names = Some(collector.names);
        }
        Ok(VisitControl::Continue)
    }

    fn visit_table_reference(&mut self, table_ref: &mut TableReference) -> Result<VisitControl, !> {
        if let TableReference::Table { span, table, .. } = table_ref
            && table.database.is_none()
        {
            let name = normalize_identifier(&table.table, &self.name_resolution_ctx).name;
            if !self
                .cte_names
                .as_ref()
                .is_some_and(|names| names.contains(&name))
            {
                table.database = Some(Identifier::from_name(*span, self.current_database.clone()));
            }
        }
        Ok(VisitControl::Continue)
    }
}

struct CteNameCollector<'a> {
    names: HashSet<String>,
    name_resolution_ctx: &'a NameResolutionContext,
}

impl Visitor for CteNameCollector<'_> {
    fn visit_query(&mut self, query: &Query) -> Result<VisitControl, !> {
        if let Some(with) = &query.with {
            self.names.extend(
                with.ctes.iter().map(|cte| {
                    normalize_identifier(&cte.alias.name, self.name_resolution_ctx).name
                }),
            );
        }
        Ok(VisitControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_ast::ast::Statement;
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_sql;
    use databend_common_ast::parser::tokenize_sql;
    use databend_common_ast::visit::WalkMut;

    use super::*;

    fn rewrite(sql: &str) -> String {
        let tokens = tokenize_sql(sql).unwrap();
        let (Statement::Query(mut query), _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap()
        else {
            panic!("not a query: {sql}");
        };
        let Ok(_) = query.walk_mut(&mut ViewRewriter::new(
            "db",
            NameResolutionContext::default(),
        ));
        query.to_string()
    }

    #[test]
    fn test_view_rewriter_keeps_possible_cte_references_unqualified() {
        for (sql, expected) in [
            ("SELECT * FROM t", "SELECT * FROM db.t"),
            (
                "WITH t AS (SELECT * FROM s) SELECT * FROM t, u",
                "WITH t AS (SELECT * FROM db.s) SELECT * FROM t, db.u",
            ),
            // Explicit qualifiers name tables, whether or not a CTE has that name.
            (
                "WITH t AS (SELECT 1) SELECT * FROM t, db.t, other.t, c.other.t",
                "WITH t AS (SELECT 1) SELECT * FROM t, db.t, other.t, c.other.t",
            ),
            (
                "WITH a AS (SELECT * FROM b), b AS (SELECT 99 AS n) SELECT * FROM a",
                "WITH a AS (SELECT * FROM b), b AS (SELECT 99 AS n) SELECT * FROM a",
            ),
            // A lazy CTE can be bound in a scope containing a nested CTE. Do not decide
            // whether z is a table or CTE while walking a's definition.
            (
                "WITH a AS (SELECT * FROM z) SELECT * FROM (WITH z AS (SELECT 77 AS n) SELECT * FROM a)",
                "WITH a AS (SELECT * FROM z) SELECT * FROM (WITH z AS (SELECT 77 AS n) SELECT * FROM a)",
            ),
            // Here the outer t is a table, not the nested CTE; the binding namespace
            // supplies its database without risking qualification of a CTE reference.
            (
                "SELECT * FROM (WITH t AS (SELECT 1) SELECT * FROM t), t",
                "SELECT * FROM (WITH t AS (SELECT 1) SELECT * FROM t), t",
            ),
            (
                "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT * FROM t",
                "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT * FROM t",
            ),
            (
                r#"WITH T AS (SELECT 1) SELECT * FROM t, "T""#,
                r#"WITH T AS (SELECT 1) SELECT * FROM t, db."T""#,
            ),
        ] {
            assert_eq!(rewrite(sql), expected, "{sql}");
        }
    }
}
