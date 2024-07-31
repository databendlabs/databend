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

use databend_common_ast::ast::DeleteStmt;
use databend_common_ast::ast::MatchOperation;
use databend_common_ast::ast::MatchedClause;
use databend_common_ast::ast::TableReference;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::bind_mutation::bind::Mutation;
use crate::binder::bind_mutation::mutation_expression::MutationExpression;
use crate::binder::util::TableIdentifier;
use crate::binder::Binder;
use crate::binder::MutationStrategy;
use crate::plans::Plan;
use crate::BindContext;

impl<'a> Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_delete(
        &mut self,
        bind_context: &mut BindContext,
        stamt: &DeleteStmt,
    ) -> Result<Plan> {
        let DeleteStmt {
            table,
            selection,
            with,
            ..
        } = stamt;

        self.init_cte(bind_context, with)?;

        let target_table_identifier = if let TableReference::Table {
            catalog,
            database,
            table,
            alias,
            ..
        } = table
        {
            TableIdentifier::new(self, catalog, database, table, alias)
        } else {
            // we do not support USING clause yet
            return Err(ErrorCode::Internal(
                "should not happen, parser should have report error already",
            ));
        };

        let matched_clause = MatchedClause {
            selection: None,
            operation: MatchOperation::Delete,
        };

        let mutation = Mutation {
            target_table_identifier,
            expression: MutationExpression::Delete {
                target: table.clone(),
                filter: selection.clone(),
            },
            strategy: MutationStrategy::MatchedOnly,
            matched_clauses: vec![matched_clause],
            unmatched_clauses: vec![],
        };

        self.bind_mutation(bind_context, mutation).await
    }
}
