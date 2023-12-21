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

use databend_common_ast::ast::AccountMgrLevel;
use databend_common_ast::ast::AccountMgrSource;
use databend_common_ast::ast::AlterUserStmt;
use databend_common_ast::ast::AuthOption;
use databend_common_ast::ast::CreateUserStmt;
use databend_common_ast::ast::GrantStmt;
use databend_common_ast::ast::RevokeStmt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserOption;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_users::UserApiProvider;
use passwords::analyzer;

use crate::plans::AlterUserPlan;
use crate::plans::CreateUserPlan;
use crate::plans::GrantPrivilegePlan;
use crate::plans::GrantRolePlan;
use crate::plans::Plan;
use crate::plans::RevokePrivilegePlan;
use crate::plans::RevokeRolePlan;
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
                    principal: principal.clone(),
                    role: role.clone(),
                };
                Ok(Plan::GrantRole(Box::new(plan)))
            }
            AccountMgrSource::ALL { level } => {
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
            AccountMgrSource::Privs { privileges, level } => {
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

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_revoke(
        &mut self,
        stmt: &RevokeStmt,
    ) -> Result<Plan> {
        let RevokeStmt { source, principal } = stmt;

        match source {
            AccountMgrSource::Role { role } => {
                let plan = RevokeRolePlan {
                    principal: principal.clone(),
                    role: role.clone(),
                };
                Ok(Plan::RevokeRole(Box::new(plan)))
            }
            AccountMgrSource::ALL { level } => {
                // ALL PRIVILEGES have different available privileges set on different grant objects
                // Now in this case all is always true.
                let grant_object = self.convert_to_grant_object(level);
                let priv_types = grant_object.available_privileges();
                let plan = RevokePrivilegePlan {
                    principal: principal.clone(),
                    on: grant_object,
                    priv_types,
                };
                Ok(Plan::RevokePriv(Box::new(plan)))
            }
            AccountMgrSource::Privs { privileges, level } => {
                let grant_object = self.convert_to_grant_object(level);
                let mut priv_types = UserPrivilegeSet::empty();
                for x in privileges {
                    priv_types.set_privilege(*x);
                }
                let plan = RevokePrivilegePlan {
                    principal: principal.clone(),
                    on: grant_object,
                    priv_types,
                };
                Ok(Plan::RevokePriv(Box::new(plan)))
            }
        }
    }

    pub(in crate::planner::binder) fn convert_to_grant_object(
        &self,
        source: &AccountMgrLevel,
    ) -> GrantObject {
        // TODO fetch real catalog
        let catalog_name = self.ctx.get_current_catalog();
        match source {
            AccountMgrLevel::Global => GrantObject::Global,
            AccountMgrLevel::Table(database_name, table_name) => {
                let database_name = database_name
                    .clone()
                    .unwrap_or_else(|| self.ctx.get_current_database());
                GrantObject::Table(catalog_name, database_name, table_name.clone())
            }
            AccountMgrLevel::Database(database_name) => {
                let database_name = database_name
                    .clone()
                    .unwrap_or_else(|| self.ctx.get_current_database());
                GrantObject::Database(catalog_name, database_name)
            }
            AccountMgrLevel::UDF(udf) => GrantObject::UDF(udf.clone()),
            AccountMgrLevel::Stage(stage) => GrantObject::Stage(stage.clone()),
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_user(
        &mut self,
        stmt: &CreateUserStmt,
    ) -> Result<Plan> {
        let CreateUserStmt {
            if_not_exists,
            user,
            auth_option,
            user_options,
        } = stmt;
        let mut user_option = UserOption::default();
        for option in user_options {
            option.apply(&mut user_option);
        }
        self.verify_password(&user_option, auth_option).await?;

        let plan = CreateUserPlan {
            user: user.clone(),
            auth_info: AuthInfo::create2(&auth_option.auth_type, &auth_option.password)?,
            user_option,
            if_not_exists: *if_not_exists,
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
                .get_user(&self.ctx.get_tenant(), user.clone().unwrap())
                .await?
        };

        let mut user_option = user_info.option.clone();
        for option in user_options {
            option.apply(&mut user_option);
        }

        // None means no change to make
        let new_auth_info = if let Some(auth_option) = &auth_option {
            // verify the password if changed
            self.verify_password(&user_option, auth_option).await?;
            let auth_info = user_info
                .auth_info
                .alter2(&auth_option.auth_type, &auth_option.password)?;
            if user_info.auth_info == auth_info {
                None
            } else {
                Some(auth_info)
            }
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

    // Verify the password according to the options of the password policy
    #[async_backtrace::framed]
    async fn verify_password(
        &mut self,
        user_option: &UserOption,
        auth_option: &AuthOption,
    ) -> Result<()> {
        if let (Some(name), Some(password)) = (user_option.password_policy(), &auth_option.password)
        {
            if let Ok(password_policy) = UserApiProvider::instance()
                .get_password_policy(&self.ctx.get_tenant(), name)
                .await
            {
                let analyzed = analyzer::analyze(password);

                let mut invalids = Vec::new();
                if analyzed.length() < password_policy.min_length as usize
                    || analyzed.length() > password_policy.max_length as usize
                {
                    invalids.push(format!(
                        "expect length range {} to {}, but got {}",
                        password_policy.min_length,
                        password_policy.max_length,
                        analyzed.length()
                    ));
                }
                if analyzed.uppercase_letters_count()
                    < password_policy.min_upper_case_chars as usize
                {
                    invalids.push(format!(
                        "expect {} uppercase chars, but got {}",
                        password_policy.min_upper_case_chars,
                        analyzed.uppercase_letters_count()
                    ));
                }
                if analyzed.lowercase_letters_count()
                    < password_policy.min_lower_case_chars as usize
                {
                    invalids.push(format!(
                        "expect {} lowercase chars, but got {}",
                        password_policy.min_lower_case_chars,
                        analyzed.lowercase_letters_count()
                    ));
                }
                if analyzed.numbers_count() < password_policy.min_numeric_chars as usize {
                    invalids.push(format!(
                        "expect {} numeric chars, but got {}",
                        password_policy.min_numeric_chars,
                        analyzed.numbers_count()
                    ));
                }
                if analyzed.symbols_count() < password_policy.min_special_chars as usize {
                    invalids.push(format!(
                        "expect {} special chars, but got {}",
                        password_policy.min_special_chars,
                        analyzed.symbols_count()
                    ));
                }
                if !invalids.is_empty() {
                    return Err(ErrorCode::InvalidPassword(format!(
                        "Invalid password: {}",
                        invalids.join(", ")
                    )));
                }
            }
        }
        Ok(())
    }
}
