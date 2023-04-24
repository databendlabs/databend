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

        let tenant = self.ctx.get_tenant();
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

        let plan = CreateIndexPlan {
            tenant,
            if_not_exists: *if_not_exists,
            index_type: *index_type,
            index_name,
            subquery,
            table_id,
        };
        Ok(Plan::CreateIndex(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_index(
        &mut self,
        stmt: &DropIndexStmt,
    ) -> Result<Plan> {
        let DropIndexStmt { if_exists, index } = stmt;

        let tenant = self.ctx.get_tenant();

        self.ctx.get_current_catalog();

        let plan = DropIndexPlan {
            if_exists: *if_exists,
            tenant,
            catalog: self.ctx.get_current_catalog(),
            index: index.to_string(),
        };
        Ok(Plan::DropIndex(Box::new(plan)))
    }
}
