use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::RoleInfo;
use common_meta_app::principal::UserInfo;
use common_meta_app::principal::UserPrivilegeType;
use common_users::BUILTIN_ROLE_PUBLIC;
use common_users::RoleCacheManager;

use crate::sessions::SessionContext;

/// SessionPrivilegeManager handles all the things related to privieges in a session.
#[async_trait::async_trait]
pub trait SessionPrivilegeManager {
    async fn set_authed_user(&self, user: UserInfo, auth_role: Option<String>) -> Result<()>;

    fn get_current_user(&self) -> Result<UserInfo>;

    fn set_current_role(&self, role: Option<RoleInfo>);

    fn get_current_role(&self) -> Option<RoleInfo>;

    fn get_available_roles(&self) -> Result<RoleInfo>;

    // fn show_grants(&self);

    async fn validate_privilege(object: &GrantObject, privilege: Vec<UserPrivilegeType>) -> Result<()>;

    async fn validate_owner(object: &GrantObject, user: &UserInfo) -> Result<()>;

    async fn validate_available_role(&self, role_name: &str) -> Result<RoleInfo>;

    async fn check_visible(object: &GrantObject) -> Result<bool>;
}

pub struct SessionPrivilegeManagerImpl {
    session_ctx: Arc<SessionContext>,
}

impl SessionPrivilegeManagerImpl {
    pub fn new(session_ctx: Arc<SessionContext>) -> Self {
        Self { session_ctx }
    }

    async fn ensure_current_role(&self) -> Result<()> {
        let tenant = self.session_ctx.get_current_tenant();
        let public_role = RoleCacheManager::instance()
            .find_role(&tenant, BUILTIN_ROLE_PUBLIC)
            .await?
            .unwrap_or_else(|| RoleInfo::new(BUILTIN_ROLE_PUBLIC));

        // if CURRENT ROLE is not set, take current session's AUTH ROLE
        let mut current_role_name = self.get_current_role().map(|r| r.name);
        if current_role_name.is_none() {
            current_role_name = self.session_ctx.get_auth_role();
        }

        // if CURRENT ROLE and AUTH ROLE are not set, take current user's DEFAULT ROLE
        if current_role_name.is_none() {
            current_role_name = self
                .session_ctx
                .get_current_user()
                .map(|u| u.option.default_role().cloned())
                .unwrap_or(None)
        };

        // if CURRENT ROLE, AUTH ROLE and DEFAULT ROLE are not set, take PUBLIC role
        let current_role_name =
            current_role_name.unwrap_or_else(|| BUILTIN_ROLE_PUBLIC.to_string());

        // I can not use the CURRENT ROLE, reset to PUBLIC role
        let role = self
            .validate_available_role(&current_role_name)
            .await
            .or_else(|e| {
                if e.code() == ErrorCode::INVALID_ROLE {
                    Ok(public_role)
                } else {
                    Err(e)
                }
            })?;
        self.session_ctx.set_current_role(Some(role));
        Ok(())
    }
}

#[async_trait::async_trait]
impl SessionPrivilegeManager for SessionPrivilegeManagerImpl {
    async fn set_authed_user(&self, user: UserInfo, auth_role: Option<String>) -> Result<()> {
        todo!()
    }

    fn set_current_role(&self, role: Option<RoleInfo>) {
        todo!()
    }

    fn get_current_user(&self) -> Result<UserInfo> {
        todo!()
    }

    fn get_current_role(&self) -> Option<RoleInfo> {
        todo!()
    }

    fn get_available_roles(&self) -> Result<RoleInfo> {
        todo!()
    }

    async fn validate_privilege(object: &GrantObject, privilege: Vec<UserPrivilegeType>) -> Result<()> {
        todo!()
    }

    async fn validate_owner(object: &GrantObject, user: &UserInfo) -> Result<()> {
        todo!()
    }

    async fn validate_available_role(&self, role_name: &str) -> Result<RoleInfo> {
        todo!()
    }

    async fn check_visible(object: &GrantObject) -> Result<bool> {
        todo!()
    }
}