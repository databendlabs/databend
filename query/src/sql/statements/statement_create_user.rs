use common_exception::Result;
use common_meta_types::AuthType;
use common_planners::CreateUserPlan;
use common_planners::PlanNode;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateUser {
    pub if_not_exists: bool,
    /// User name
    pub name: String,
    pub hostname: String,
    pub auth_type: AuthType,
    pub password: String,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCreateUser {
    async fn analyze(&self, _ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        Ok(AnalyzedResult::SimpleQuery(PlanNode::CreateUser(
            CreateUserPlan {
                name: self.name.clone(),
                password: Vec::from(self.password.clone()),
                hostname: self.hostname.clone(),
                auth_type: self.auth_type.clone(),
            },
        )))
    }
}
