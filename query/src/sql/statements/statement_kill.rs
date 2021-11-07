use sqlparser::ast::Ident;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use crate::sessions::DatabendQueryContextRef;
use common_exception::Result;
use common_planners::{PlanNode, KillPlan};

#[derive(Debug, Clone, PartialEq)]
pub struct DfKillStatement {
    pub object_id: Ident,
    pub kill_query: bool,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfKillStatement {
    async fn analyze(self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let id = self.object_id.value.clone();
        let kill_connection = !self.kill_query;
        Ok(AnalyzedResult::SimpleQuery(
            PlanNode::Kill(
                KillPlan { id, kill_connection }
            )
        ))
    }
}
