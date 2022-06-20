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

use common_ast::ast::GrantLevel;
use common_ast::ast::GrantSource;
use common_ast::ast::GrantStatement;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeSet;
use common_planners::GrantPrivilegePlan;
use common_planners::GrantRolePlan;

use crate::sql::plans::Plan;
use crate::sql::Binder;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_grant(
        &mut self,
        stmt: &GrantStatement,
    ) -> Result<Plan> {
        let GrantStatement { source, principal } = stmt;

        match source {
            GrantSource::Role { role } => {
                let plan = GrantRolePlan {
                    principal: principal.clone(),
                    role: role.clone(),
                };
                Ok(Plan::GrantRole(Box::new(plan)))
            }
            GrantSource::ALL { level } => {
                // ALL PRIVILEGES have different available privileges set on different grant objects
                // Now in this case all is always true.
                let grant_object = self.convert_to_grant_object(level);
                let priv_types = grant_object.available_privileges();
                let plan = GrantPrivilegePlan {
                    principal: principal.clone(),
                    on: grant_object,
                    priv_types,
                };
                Ok(Plan::GrantPriv(Box::new(plan)))
            }
            GrantSource::Privs { privileges, level } => {
                let grant_object = self.convert_to_grant_object(level);
                let mut priv_types = UserPrivilegeSet::empty();
                for x in privileges {
                    priv_types.set_privilege(*x);
                }
                let plan = GrantPrivilegePlan {
                    principal: principal.clone(),
                    on: grant_object,
                    priv_types,
                };
                Ok(Plan::GrantPriv(Box::new(plan)))
            }
        }
    }

    //Copy from query/src/sql/statements/statement_grant.rs
    fn convert_to_grant_object(&self, source: &GrantLevel) -> GrantObject {
        // TODO fetch real catalog
        let catalog_name = self.ctx.get_current_catalog();
        match source {
            GrantLevel::Global => GrantObject::Global,
            GrantLevel::Table(database_name, table_name) => {
                let database_name = database_name
                    .clone()
                    .unwrap_or_else(|| self.ctx.get_current_database());
                GrantObject::Table(catalog_name, database_name, table_name.clone())
            }
            GrantLevel::Database(database_name) => {
                let database_name = database_name
                    .clone()
                    .unwrap_or_else(|| self.ctx.get_current_database());
                GrantObject::Database(catalog_name, database_name)
            }
        }
    }
}
