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

use databend_common_ast::ast::MatchOperation;
use databend_common_ast::ast::MatchedClause;
use databend_common_ast::ast::MutationUpdateExpr;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::UpdateStmt;
use databend_common_exception::Result;

use crate::binder::bind_mutation::bind::Mutation;
use crate::binder::bind_mutation::bind::MutationStrategy;
use crate::binder::bind_mutation::mutation_expression::MutationExpression;
use crate::binder::util::TableIdentifier;
use crate::binder::Binder;
use crate::plans::Plan;
use crate::BindContext;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_update(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &UpdateStmt,
    ) -> Result<Plan> {
        let UpdateStmt {
            catalog,
            database,
            table,
            table_alias,
            update_list,
            from,
            selection,
            with,
            ..
        } = stmt;

        self.init_cte(bind_context, with)?;

        let target_table_identifier =
            TableIdentifier::new(self, catalog, database, table, table_alias);

        let target_table_reference = TableReference::Table {
            span: None,
            catalog: catalog.clone(),
            database: database.clone(),
            table: table.clone(),
            alias: table_alias.clone(),
            temporal: None,
            with_options: None,
            pivot: None,
            unpivot: None,
            sample: None,
        };

        let update_exprs = update_list
            .iter()
            .map(|update_expr| MutationUpdateExpr {
                table: None,
                name: update_expr.name.clone(),
                expr: update_expr.expr.clone(),
            })
            .collect::<Vec<_>>();
        let matched_clause = MatchedClause {
            selection: None,
            operation: MatchOperation::Update {
                update_list: update_exprs,
                is_star: false,
            },
        };

        let from = from.as_ref().map(|from| from.transform_table_reference());
        let mutation = Mutation {
            target_table_identifier,
            expression: MutationExpression::Update {
                target: target_table_reference,
                filter: selection.clone(),
                from,
            },
            strategy: MutationStrategy::MatchedOnly,
            matched_clauses: vec![matched_clause],
            unmatched_clauses: vec![],
        };

        self.bind_mutation(bind_context, mutation).await
    }
}
