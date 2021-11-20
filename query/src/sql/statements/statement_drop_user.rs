use common_exception::Result;
use common_planners::DropUserPlan;
use common_planners::PlanNode;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfDropUser {
    pub if_exists: bool,
    /// User name
    pub name: String,
    pub hostname: String,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfDropUser {
    async fn analyze(&self, _: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        Ok(AnalyzedResult::SimpleQuery(PlanNode::DropUser(
            DropUserPlan {
                if_exists: self.if_exists,
                name: self.name.clone(),
                hostname: self.hostname.clone(),
            },
        )))
    }
}
