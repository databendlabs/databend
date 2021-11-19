use common_exception::Result;
use common_meta_types::UserPrivilege;
use common_planners::GrantPrivilegePlan;
use common_planners::PlanNode;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfGrantStatement {
    pub name: String,
    pub hostname: String,
    pub priv_types: UserPrivilege,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfGrantStatement {
    async fn analyze(&self, _: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        Ok(AnalyzedResult::SimpleQuery(PlanNode::GrantPrivilege(
            GrantPrivilegePlan {
                name: self.name.clone(),
                hostname: self.hostname.clone(),
                priv_types: self.priv_types,
            },
        )))
    }
}
