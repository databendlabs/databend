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

use databend_common_ast::ast::AlterViewStmt;
use databend_common_ast::ast::CreateViewStmt;
use databend_common_ast::ast::DropViewStmt;
use databend_common_exception::Result;
use derive_visitor::DriveMut;

use crate::binder::Binder;
use crate::planner::semantic::normalize_identifier;
use crate::plans::AlterViewPlan;
use crate::plans::CreateViewPlan;
use crate::plans::DropViewPlan;
use crate::plans::Plan;
use crate::ViewRewriter;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_view(
        &mut self,
        stmt: &CreateViewStmt,
    ) -> Result<Plan> {
        let CreateViewStmt {
            create_option,
            catalog,
            database,
            view,
            columns,
            query,
        } = stmt;
        let mut query = *query.clone();
        let tenant = self.ctx.get_tenant();
        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let column_names = columns
            .iter()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .collect::<Vec<_>>();
        let mut visitor = ViewRewriter {
            current_database: database.clone(),
        };
        query.drive_mut(&mut visitor);
        let subquery = format!("{}", query);

        let plan = CreateViewPlan {
            create_option: *create_option,
            tenant: tenant.to_string(),
            catalog,
            database,
            view_name,
            column_names,
            subquery,
        };
        Ok(Plan::CreateView(plan.into()))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_view(
        &mut self,
        stmt: &AlterViewStmt,
    ) -> Result<Plan> {
        let AlterViewStmt {
            catalog,
            database,
            view,
            columns,
            query,
        } = stmt;

        let mut query = *query.clone();
        let tenant = self.ctx.get_tenant();
        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let column_names = columns
            .iter()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .collect::<Vec<_>>();
        let mut visitor = ViewRewriter {
            current_database: database.clone(),
        };
        query.drive_mut(&mut visitor);
        let subquery = format!("{}", query);

        let plan = AlterViewPlan {
            tenant: tenant.to_string(),
            catalog,
            database,
            view_name,
            column_names,
            subquery,
        };
        Ok(Plan::AlterView(plan.into()))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_view(
        &mut self,
        stmt: &DropViewStmt,
    ) -> Result<Plan> {
        let DropViewStmt {
            if_exists,
            catalog,
            database,
            view,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let plan = DropViewPlan {
            if_exists: *if_exists,
            tenant: tenant.to_string(),
            catalog,
            database,
            view_name,
        };
        Ok(Plan::DropView(plan.into()))
    }
}
