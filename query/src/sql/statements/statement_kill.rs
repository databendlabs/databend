use common_exception::Result;
use common_planners::KillPlan;
use common_planners::PlanNode;
use sqlparser::ast::Ident;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfKillStatement {
    pub object_id: Ident,
    pub kill_query: bool,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfKillStatement {
    async fn analyze(&self, _ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let id = self.object_id.value.clone();
        let kill_connection = !self.kill_query;
        Ok(AnalyzedResult::SimpleQuery(PlanNode::Kill(KillPlan {
            id,
            kill_connection,
        })))
    }
}
