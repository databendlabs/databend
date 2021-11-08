use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use crate::sessions::DatabendQueryContextRef;
use crate::sql::PlanParser;
use common_exception::Result;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowMetrics;

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowMetrics {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let rewritten_query = "SELECT * FROM system.metrics";
        let rewritten_query_plan = PlanParser::parse(rewritten_query, ctx);
        Ok(AnalyzedResult::SimpleQuery(rewritten_query_plan.await?))
    }
}

