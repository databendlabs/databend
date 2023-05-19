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

use common_ast::ast::CreateIndexStmt;
use common_ast::ast::DropIndexStmt;
use common_ast::ast::GroupBy;
use common_ast::ast::Query;
use common_ast::ast::SetExpr;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::binder::Binder;
use crate::plans::CreateIndexPlan;
use crate::plans::DropIndexPlan;
use crate::plans::Plan;
use crate::BindContext;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_index(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CreateIndexStmt,
    ) -> Result<Plan> {
        let CreateIndexStmt {
            index_type,
            if_not_exists,
            index_name,
            query,
        } = stmt;

        // check if query support index
        Self::check_index_support(query)?;

        let index_name = self.normalize_object_identifier(index_name);
        let subquery = format!("{}", query);

        self.bind_query(bind_context, query).await?;

        let tables = self.metadata.read().tables().to_vec();

        if tables.len() != 1 {
            return Err(ErrorCode::UnsupportedIndex(
                "Create Index currently only support single table",
            ));
        }

        let table = tables[0].table();

        if !table.support_index() {
            return Err(ErrorCode::UnsupportedIndex(format!(
                "Table engine {} does not support create index",
                table.engine()
            )));
        }

        let table_id = table.get_id();
        let table_desc = table.get_table_info().desc.clone();

        let plan = CreateIndexPlan {
            if_not_exists: *if_not_exists,
            index_type: *index_type,
            index_name,
            subquery,
            table_id,
            table_desc,
        };
        Ok(Plan::CreateIndex(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_index(
        &mut self,
        stmt: &DropIndexStmt,
    ) -> Result<Plan> {
        let DropIndexStmt { if_exists, index } = stmt;

        let plan = DropIndexPlan {
            if_exists: *if_exists,
            index: index.to_string(),
        };
        Ok(Plan::DropIndex(Box::new(plan)))
    }

    fn check_index_support(query: &Query) -> Result<()> {
        let err = Err(ErrorCode::UnsupportedIndex(format!(
            "Currently create index just support simple query, like: {}",
            "SELECT ... FROM ... WHERE ... GROUP BY ..."
        )));

        if query.with.is_some() || !query.order_by.is_empty() {
            return err;
        }

        if let SetExpr::Select(stmt) = &query.body {
            if stmt.having.is_some() || stmt.window_list.is_some() {
                return err;
            }
            match &stmt.group_by {
                None => {}
                Some(group_by) => match group_by {
                    GroupBy::Normal(_) => {}
                    _ => {
                        return err;
                    }
                },
            }
            for target in &stmt.select_list {
                if target.has_window() {
                    return err;
                }
            }
        } else {
            return err;
        }

        Ok(())
    }
}
