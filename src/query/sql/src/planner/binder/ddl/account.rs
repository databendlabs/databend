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

use chrono::Utc;
use databend_common_ast::ast::AccountMgrLevel;
use databend_common_ast::ast::AccountMgrSource;
use databend_common_ast::ast::AlterUserStmt;
use databend_common_ast::ast::CreateUserStmt;
use databend_common_ast::ast::GrantObjectName;
use databend_common_ast::ast::GrantStmt;
use databend_common_ast::ast::PrincipalIdentity as AstPrincipalIdentity;
use databend_common_ast::ast::RevokeStmt;
use databend_common_ast::ast::ShowObjectPrivilegesStmt;
use databend_common_ast::ast::ShowOptions;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::PrincipalIdentity;
use databend_common_meta_app::principal::UserOption;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_users::UserApiProvider;

use crate::binder::show::get_show_options;
use crate::binder::util::illegal_ident_name;
use crate::plans::AlterUserPlan;
use crate::plans::CreateUserPlan;
use crate::plans::GrantPrivilegePlan;
use crate::plans::GrantRolePlan;
use crate::plans::Plan;
use crate::plans::RevokePrivilegePlan;
use crate::plans::RevokeRolePlan;
use crate::plans::RewriteKind;
use crate::BindContext;
use crate::Binder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_grant(
        &mut self,
        stmt: &GrantStmt,
    ) -> Result<Plan> {
        let GrantStmt { source, principal } = stmt;

        match source {
            AccountMgrSource::Role { role } => {
                let plan = GrantRolePlan {
                    principal: principal.clone().into(),
                    role: role.clone(),
                };
                Ok(Plan::GrantRole(Box::new(plan)))
            }
            AccountMgrSource::ALL { level } => {
                // ALL PRIVILEGES have different available privileges set on different grant objects
                // Now in this case all is always true.
                let grant_object = self.convert_to_grant_object(level).await?;
                let priv_types = grant_object.available_privileges(false);
                let plan: GrantPrivilegePlan = GrantPrivilegePlan {
                    principal: principal.clone().into(),
                    on: grant_object,
                    priv_types,
                };
                Ok(Plan::GrantPriv(Box::new(plan)))
            }
            AccountMgrSource::Privs { privileges, level } => {
                let grant_object = self.convert_to_grant_object(level).await?;
                let mut priv_types = UserPrivilegeSet::empty();
                for x in privileges {
                    priv_types.set_privilege(x.clone().into());
                }
                let plan = GrantPrivilegePlan {
                    principal: principal.clone().into(),
                    on: grant_object,
                    priv_types,
                };
                Ok(Plan::GrantPriv(Box::new(plan)))
            }
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_revoke(
        &mut self,
        stmt: &RevokeStmt,
    ) -> Result<Plan> {
        let RevokeStmt { source, principal } = stmt;

        match source {
            AccountMgrSource::Role { role } => {
                let plan = RevokeRolePlan {
                    principal: principal.clone().into(),
                    role: role.clone(),
                };
                Ok(Plan::RevokeRole(Box::new(plan)))
            }
            AccountMgrSource::ALL { level } => {
                // ALL PRIVILEGES have different available privileges set on different grant objects
                // Now in this case all is always true.
                let grant_object = self.convert_to_revoke_grant_object(level).await?;
                // Note if old version `grant all on db.*/db.t to user`, the user will contain ownership privilege.
                // revoke all need to revoke it.
                let principal: PrincipalIdentity = principal.clone().into();
                let priv_types = match &principal {
                    PrincipalIdentity::User(_) => grant_object[0].available_privileges(true),
                    PrincipalIdentity::Role(_) => grant_object[0].available_privileges(false),
                };
                let plan = RevokePrivilegePlan {
                    principal: principal.clone(),
                    on: grant_object,
                    priv_types,
                };
                Ok(Plan::RevokePriv(Box::new(plan)))
            }
            AccountMgrSource::Privs { privileges, level } => {
                let grant_object = self.convert_to_revoke_grant_object(level).await?;
                let mut priv_types = UserPrivilegeSet::empty();
                for x in privileges {
                    priv_types.set_privilege(x.clone().into());
                }
                let plan = RevokePrivilegePlan {
                    principal: principal.clone().into(),
                    on: grant_object,
                    priv_types,
                };
                Ok(Plan::RevokePriv(Box::new(plan)))
            }
        }
    }

    pub(in crate::planner::binder) async fn convert_to_grant_object(
        &self,
        source: &AccountMgrLevel,
    ) -> Result<GrantObject> {
        // TODO fetch real catalog
        let catalog_name = self.ctx.get_current_catalog();
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(&catalog_name).await?;
        match source {
            AccountMgrLevel::Global => Ok(GrantObject::Global),
            AccountMgrLevel::Table(database_name, table_name) => {
                let database_name = database_name
                    .clone()
                    .unwrap_or_else(|| self.ctx.get_current_database());
                if self
                    .ctx
                    .is_temp_table(&catalog_name, &database_name, table_name)
                {
                    return Err(ErrorCode::StorageOther(format!(
                        "{}.{}.{} is a temporary table, cannot grant privileges on it",
                        catalog_name, database_name, table_name
                    )));
                }
                let db_id = catalog
                    .get_database(&tenant, &database_name)
                    .await?
                    .get_db_info()
                    .database_id
                    .db_id;
                let table_id = catalog
                    .get_table(&tenant, &database_name, table_name)
                    .await?
                    .get_id();
                Ok(GrantObject::TableById(catalog_name, db_id, table_id))
            }
            AccountMgrLevel::Database(database_name) => {
                let database_name = database_name
                    .clone()
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let db_id = catalog
                    .get_database(&tenant, &database_name)
                    .await?
                    .get_db_info()
                    .database_id
                    .db_id;
                Ok(GrantObject::DatabaseById(catalog_name, db_id))
            }
            AccountMgrLevel::UDF(udf) => Ok(GrantObject::UDF(udf.clone())),
            AccountMgrLevel::Stage(stage) => Ok(GrantObject::Stage(stage.clone())),
            AccountMgrLevel::Warehouse(w) => Ok(GrantObject::Warehouse(w.clone())),
        }
    }

    // Some old query version use GrantObject::Table store table name.
    // So revoke need compat the old version.
    pub(in crate::planner::binder) async fn convert_to_revoke_grant_object(
        &self,
        source: &AccountMgrLevel,
    ) -> Result<Vec<GrantObject>> {
        // TODO fetch real catalog
        let catalog_name = self.ctx.get_current_catalog();
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(&catalog_name).await?;
        match source {
            AccountMgrLevel::Global => Ok(vec![GrantObject::Global]),
            AccountMgrLevel::Table(database_name, table_name) => {
                let database_name = database_name
                    .clone()
                    .unwrap_or_else(|| self.ctx.get_current_database());
                if self
                    .ctx
                    .is_temp_table(&catalog_name, &database_name, table_name)
                {
                    return Err(ErrorCode::StorageOther(format!(
                        "{}.{}.{} is a temporary table, cannot revoke privileges on it",
                        catalog_name, database_name, table_name
                    )));
                }
                let db_id = catalog
                    .get_database(&tenant, &database_name)
                    .await?
                    .get_db_info()
                    .database_id
                    .db_id;
                let table_id = catalog
                    .get_table(&tenant, &database_name, table_name)
                    .await?
                    .get_id();
                Ok(vec![
                    GrantObject::TableById(catalog_name.clone(), db_id, table_id),
                    GrantObject::Table(catalog_name.clone(), database_name, table_name.clone()),
                ])
            }
            AccountMgrLevel::Database(database_name) => {
                let database_name = database_name
                    .clone()
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let db_id = catalog
                    .get_database(&tenant, &database_name)
                    .await?
                    .get_db_info()
                    .database_id
                    .db_id;
                Ok(vec![
                    GrantObject::DatabaseById(catalog_name.clone(), db_id),
                    GrantObject::Database(catalog_name.clone(), database_name),
                ])
            }
            AccountMgrLevel::UDF(udf) => Ok(vec![GrantObject::UDF(udf.clone())]),
            AccountMgrLevel::Stage(stage) => Ok(vec![GrantObject::Stage(stage.clone())]),
            AccountMgrLevel::Warehouse(w) => Ok(vec![GrantObject::Warehouse(w.clone())]),
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_user(
        &mut self,
        stmt: &CreateUserStmt,
    ) -> Result<Plan> {
        let CreateUserStmt {
            create_option,
            user,
            auth_option,
            user_options,
        } = stmt;
        if illegal_ident_name(&user.username) {
            return Err(ErrorCode::IllegalUser(format!(
                "Illegal Username: Illegal user name [{}], not support username contain ' or \" \\b or \\f",
                user.username
            )));
        }
        let mut user_option = UserOption::default();
        for option in user_options {
            user_option.apply(option);
        }
        UserApiProvider::instance()
            .verify_password(
                &self.ctx.get_tenant(),
                &user_option,
                auth_option,
                None,
                None,
            )
            .await?;

        // if `must_change_password` is set, user need to change password first
        let need_change = user_option
            .must_change_password()
            .cloned()
            .unwrap_or_default();

        let plan = CreateUserPlan {
            create_option: create_option.clone().into(),
            user: user.clone().into(),
            auth_info: AuthInfo::create2(
                &auth_option.auth_type.clone().map(Into::into),
                &auth_option.password,
                need_change,
            )?,
            user_option,
            password_update_on: Some(Utc::now()),
        };
        Ok(Plan::CreateUser(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_user(
        &mut self,
        stmt: &AlterUserStmt,
    ) -> Result<Plan> {
        let AlterUserStmt {
            user,
            auth_option,
            user_options,
        } = stmt;
        // None means current user
        let user_info = if user.is_none() {
            self.ctx.get_current_user()?
        } else {
            UserApiProvider::instance()
                .get_user(&self.ctx.get_tenant(), user.clone().unwrap().into())
                .await?
        };

        // TODO: Only user with OWNERSHIP privilege can change user options.
        let mut user_option = user_info.option.clone();
        for option in user_options {
            user_option.apply(option);
        }

        // If `must_change_password` is set, user need to change password first when login.
        let need_change = user_option
            .must_change_password()
            .cloned()
            .unwrap_or_default();

        // None means auth info is not changed.
        let new_auth_info = if let Some(auth_option) = &auth_option {
            // If user is changing self password, always set `need_change` as false,
            // because after this operation, the password is changed.
            // And if user is changing other user's password,
            // set `need_change` same as `must_change_password` option.
            let need_change = if user.is_none() { false } else { need_change };
            let auth_info = user_info.auth_info.alter2(
                &auth_option.auth_type.clone().map(Into::into),
                &auth_option.password,
                need_change,
            )?;
            // verify the password if changed
            UserApiProvider::instance()
                .verify_password(
                    &self.ctx.get_tenant(),
                    &user_option,
                    auth_option,
                    Some(&user_info),
                    Some(&auth_info),
                )
                .await?;
            if user_info.auth_info == auth_info {
                None
            } else {
                Some(auth_info)
            }
        } else if need_change != user_info.auth_info.get_need_change() {
            // If password is not changed, set `need_change` same as `must_change_password` option.
            let new_auth_info = user_info.auth_info.create_with_need_change(need_change);
            Some(new_auth_info)
        } else {
            None
        };

        let new_user_option = if user_option == user_info.option {
            None
        } else {
            Some(user_option)
        };
        let plan = AlterUserPlan {
            user: user_info.identity(),
            auth_info: new_auth_info,
            user_option: new_user_option,
        };

        Ok(Plan::AlterUser(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_account_grants(
        &mut self,
        bind_context: &mut BindContext,
        principal: &Option<AstPrincipalIdentity>,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let query = if let Some(principal) = principal {
            match principal {
                AstPrincipalIdentity::User(user) => {
                    format!("SELECT * FROM show_grants('user', '{}')", user.username)
                }
                AstPrincipalIdentity::Role(role) => {
                    format!("SELECT * FROM show_grants('role', '{}')", role)
                }
            }
        } else {
            let name = self.ctx.get_current_user()?.name;
            format!("SELECT * FROM show_grants('user', '{}')", name)
        };

        let (show_limit, limit_str) =
            get_show_options(show_options, Some("object_name".to_string()));
        let query = format!("{} {} {}", query, show_limit, limit_str,);

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowGrants)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_object_privileges(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowObjectPrivilegesStmt,
    ) -> Result<Plan> {
        let ShowObjectPrivilegesStmt {
            object,
            show_option,
        } = stmt;

        let catalog = self.ctx.get_current_catalog();
        let query = match object {
            GrantObjectName::Database(db) => {
                format!(
                    "SELECT * FROM show_grants('database', '{}', '{}')",
                    db, catalog
                )
            }
            GrantObjectName::Table(db, tb) => {
                let db = if let Some(db) = db {
                    db.to_string()
                } else {
                    self.ctx.get_current_database()
                };
                format!(
                    "SELECT * FROM show_grants('table', '{}', '{}', '{}')",
                    tb, catalog, db
                )
            }
            GrantObjectName::UDF(name) => {
                format!("SELECT * FROM show_grants('udf', '{}')", name)
            }
            GrantObjectName::Stage(name) => {
                format!("SELECT * FROM show_grants('stage', '{}')", name)
            }
            GrantObjectName::Warehouse(name) => {
                format!("SELECT * FROM show_grants('warehouse', '{}')", name)
            }
        };

        let (show_limit, limit_str) = get_show_options(show_option, Some("name".to_string()));
        let query = format!("{} {} {}", query, show_limit, limit_str,);

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowGrants)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_roles(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) = get_show_options(show_options, None);
        let query = format!("SELECT * FROM show_roles() {} {}", show_limit, limit_str);
        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowRoles)
            .await
    }
}
