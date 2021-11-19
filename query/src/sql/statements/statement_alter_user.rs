use common_exception::Result;
use common_meta_types::AuthType;
use common_planners::AlterUserPlan;
use common_planners::PlanNode;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfAlterUser {
    pub if_current_user: bool,
    /// User name
    pub name: String,
    pub hostname: String,
    pub new_auth_type: AuthType,
    pub new_password: String,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfAlterUser {
    async fn analyze(&self, _: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        Ok(AnalyzedResult::SimpleQuery(PlanNode::AlterUser(
            AlterUserPlan {
                if_current_user: self.if_current_user,
                name: self.name.clone(),
                new_password: Vec::from(self.new_password.clone()),
                hostname: self.hostname.clone(),
                new_auth_type: self.new_auth_type.clone(),
            },
        )))
    }
}
