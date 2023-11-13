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

use common_ast::ast::CreateStreamStmt;
use common_ast::ast::DropStreamStmt;
use common_ast::ast::StreamPoint;
use common_exception::Result;

use crate::binder::Binder;
use crate::normalize_identifier;
use crate::plans::CreateStreamPlan;
use crate::plans::DropStreamPlan;
use crate::plans::Plan;
use crate::plans::StreamNavigation;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_stream(
        &mut self,
        stmt: &CreateStreamStmt,
    ) -> Result<Plan> {
        let CreateStreamStmt {
            if_not_exists,
            catalog,
            database,
            stream,
            table_database,
            table,
            stream_point,
            comment,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, stream_name) =
            self.normalize_object_identifier_triple(catalog, database, stream);

        let table_database = table_database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;

        let navigation = stream_point.as_ref().map(|point| match point {
            StreamPoint::AtStream { database, name } => {
                let database = database
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let name = normalize_identifier(name, &self.name_resolution_ctx).name;
                StreamNavigation::AtStream { database, name }
            }
        });

        let plan = CreateStreamPlan {
            if_not_exists: *if_not_exists,
            tenant,
            catalog,
            database,
            stream_name,
            table_database,
            table_name,
            navigation,
            comment: comment.clone(),
        };
        Ok(Plan::CreateStream(plan.into()))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_stream(
        &mut self,
        stmt: &DropStreamStmt,
    ) -> Result<Plan> {
        let DropStreamStmt {
            if_exists,
            catalog,
            database,
            stream,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, stream_name) =
            self.normalize_object_identifier_triple(catalog, database, stream);
        let plan = DropStreamPlan {
            if_exists: *if_exists,
            tenant,
            catalog,
            database,
            stream_name,
        };
        Ok(Plan::DropStream(plan.into()))
    }
}
