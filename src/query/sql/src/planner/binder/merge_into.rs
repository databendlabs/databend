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

use common_ast::ast::Join;
use common_ast::ast::JoinCondition;
use common_ast::ast::JoinOperator::RightOuter;
use common_ast::ast::MergeIntoStmt;
use common_ast::ast::TableReference;
use common_exception::Result;

use crate::binder::Binder;
use crate::normalize_identifier;
use crate::plans::Plan;
use crate::BindContext;
// implementation of merge into for now:
//      use an left outer join for target_source and source.
//  target_table: (a,b)
//  source: (b,c)
// Merge into target_table using source on target_table.a = source.b
impl Binder {
    #[allow(warnings)]
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_merge_into(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &MergeIntoStmt,
    ) -> Result<Plan> {
        let MergeIntoStmt {
            catalog,
            database,
            table,
            source,
            alias_source,
            alias_target,
            join_expr,
            ..
        } = stmt;

        // get target_table_reference
        let target_table = TableReference::Table {
            span: None,
            catalog: catalog.clone(),
            database: database.clone(),
            table: table.clone(),
            alias: alias_target.clone(),
            travel_point: None,
            pivot: None,
            unpivot: None,
        };

        // get_source_table_reference
        let source_data = TableReference::MergeIntoSourceReference {
            span: None,
            source: source.clone(),
            alias: alias_source.clone(),
        };

        // build Join
        let join_reference = TableReference::Join {
            span: None,
            join: Join {
                op: RightOuter,
                left: Box::new(source_data),
                right: Box::new(target_table.clone()),
                condition: JoinCondition::On(Box::new(join_expr.clone())),
            },
        };
        self.bind_single_table(bind_context, &target_table);
        // self.bind_join(bind_context, left_context, right_context, left_child, right_child, join)
        let catalog_name = catalog.as_ref().map_or_else(
            || self.ctx.get_current_catalog(),
            |ident| normalize_identifier(ident, &self.name_resolution_ctx).name,
        );
        let database_name = database.as_ref().map_or_else(
            || self.ctx.get_current_database(),
            |ident| normalize_identifier(ident, &self.name_resolution_ctx).name,
        );
        let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;
        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;
        let table_id = table.get_id();

        let schema = table.schema();
        let input_source = self
            .get_source(
                bind_context,
                catalog_name,
                database_name,
                table_name,
                schema,
                source.clone(),
            )
            .await?;
        // let (new_expr, scalar) = self
        //     .bind_where(&mut from_context, &aliases, expr, s_expr)
        //     .await?;
        // s_expr = new_expr;
        todo!()
    }
}
