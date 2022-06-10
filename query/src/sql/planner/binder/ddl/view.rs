// Copyright 2022 Datafuse Labs.
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

use common_ast::ast::AlterViewStmt;
use common_ast::ast::CreateViewStmt;
use common_exception::Result;
use common_planners::AlterViewPlan;
use common_planners::CreateViewPlan;

use crate::sql::binder::Binder;
use crate::sql::plans::Plan;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_create_view(
        &mut self,
        stmt: &CreateViewStmt<'a>,
    ) -> Result<Plan> {
        let catalog = self.ctx.get_current_catalog();
        let db = stmt
            .database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());

        let viewname = stmt.view.name.to_lowercase();
        let subquery = format!("{}", stmt.query);

        let plan = CreateViewPlan {
            if_not_exists: stmt.if_not_exists,
            tenant: self.ctx.get_tenant(),
            catalog,
            database: db,
            viewname,
            subquery,
        };
        Ok(Plan::CreateView(Box::new(plan)))
    }

    pub(in crate::sql::planner::binder) async fn bind_alter_view(
        &mut self,
        stmt: &AlterViewStmt<'a>,
    ) -> Result<Plan> {
        let catalog = self.ctx.get_current_catalog();
        let db = stmt
            .database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());

        let viewname = stmt.view.name.to_lowercase();
        let subquery = format!("{}", stmt.query);

        let plan = AlterViewPlan {
            tenant: self.ctx.get_tenant(),
            catalog,
            db,
            viewname,
            subquery,
        };
        Ok(Plan::AlterView(Box::new(plan)))
    }
}
