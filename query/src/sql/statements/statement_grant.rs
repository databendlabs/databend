use common_meta_types::UserPrivilege;
use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use common_exception::Result;
use common_planners::{GrantPrivilegePlan, PlanNode};

#[derive(Debug, Clone, PartialEq)]
pub struct DfGrantStatement {
    pub name: String,
    pub hostname: String,
    pub priv_types: UserPrivilege,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfGrantStatement {
    async fn analyze(&self, _: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        Ok(AnalyzedResult::SimpleQuery(PlanNode::GrantPrivilege(GrantPrivilegePlan {
            name: self.name.clone(),
            hostname: self.hostname.clone(),
            priv_types: self.priv_types,
        })))
    }
}
