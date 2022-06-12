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
        let CreateViewStmt {
            if_not_exists,
            catalog,
            database,
            view,
            query,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let viewname = view.name.to_lowercase();
        let subquery = format!("{}", query);

        let plan = CreateViewPlan {
            if_not_exists: *if_not_exists,
            tenant,
            catalog,
            database,
            viewname,
            subquery,
        };
        Ok(Plan::CreateView(Box::new(plan)))
    }

    pub(in crate::sql::planner::binder) async fn bind_alter_view(
        &mut self,
        stmt: &AlterViewStmt<'a>,
    ) -> Result<Plan> {
        let AlterViewStmt {
            catalog,
            database,
            view,
            query,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let viewname = view.name.to_lowercase();
        let subquery = format!("{}", query);

        let plan = AlterViewPlan {
            tenant,
            catalog,
            database,
            viewname,
            subquery,
        };
        Ok(Plan::AlterView(Box::new(plan)))
    }
}
