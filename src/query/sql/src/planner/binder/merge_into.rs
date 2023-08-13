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

use std::collections::binary_heap;

use common_ast::ast::Join;
use common_ast::ast::JoinCondition;
use common_ast::ast::JoinOperator::FullOuter;
use common_ast::ast::MergeIntoStmt;
use common_ast::ast::TableReference;
use common_catalog::plan::InternalColumn;
use common_catalog::plan::InternalColumnType;
use common_exception::Result;
use common_expression::ROW_ID_COL_NAME;

use crate::binder::Binder;
use crate::binder::InternalColumnBinding;
use crate::executor::PhysicalPlanBuilder;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::plans::MergeIntoPlan;
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

        let database_name = database.as_ref().map_or_else(
            || self.ctx.get_current_database(),
            |ident| normalize_identifier(ident, &self.name_resolution_ctx).name,
        );
        let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;

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

        // bind table for target table
        let (right_child, right_context) =
            self.bind_single_table(bind_context, &target_table).await?;

        // add internal_column (_row_id)
        let table_index = self
            .metadata
            .read()
            .get_table_index(Some(database_name.as_str()), table_name.as_str())
            .expect("can't get target_table binding");
        let row_id_column_binding = InternalColumnBinding {
            database_name: Some(database_name),
            table_name: Some(table_name),
            internal_column: InternalColumn {
                column_name: ROW_ID_COL_NAME.to_string(),
                column_type: InternalColumnType::RowId,
            },
        };

        let column_binding = bind_context
            .add_internal_column_binding(&row_id_column_binding, self.metadata.clone())?;

        SExpr::add_internal_column_index(&right_child, table_index, column_binding.index);
        // bind source data
        let (left_child, left_context) = self
            .bind_merge_into_source(bind_context, None, alias_source, &source.clone())
            .await?;

        // add join,use full outer join in V1, we use _row_id to check_duplicate
        // join row.
        let join = Join {
            op: FullOuter,
            condition: JoinCondition::On(Box::new(join_expr.clone())),
            left: Box::new(source_data.clone()),
            right: Box::new(target_table),
        };

        let (join_sexpr, bind_ctx) = self
            .bind_join(
                bind_context,
                left_context,
                right_context,
                left_child,
                right_child,
                &join,
            )
            .await?;
        let mut builder = PhysicalPlanBuilder::new(self.metadata.clone(), self.ctx.clone(), false);
        // add cluase column
        let (match_cluases, unmatch_cluases) = stmt.split_clauses();

        let join_plan = builder.build(&join_sexpr, bind_ctx.column_set()).await?;
        // let catalog_name = catalog.as_ref().map_or_else(
        //     || self.ctx.get_current_catalog(),
        //     |ident| normalize_identifier(ident, &self.name_resolution_ctx).name,
        // );

        //     .ctx
        //     .get_table(&catalog_name, &database_name, &table_name)
        //     .await?;
        // let table_id = table.get_id();

        // let schema = table.schema();
        // let input_source = self
        //     .get_source(
        //         bind_context,
        //         catalog_name,
        //         database_name,
        //         table_name,
        //         schema,
        //         source.clone(),
        //     )
        //     .await?;
        Ok(Plan::MergeInto((Box::new(MergeIntoPlan { join_plan }))))
    }
}
