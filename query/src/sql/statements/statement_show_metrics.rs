use crate::sql::analyzer::{AnalyzableStatement, AnalyzedResult};
use crate::sessions::DatabendQueryContextRef;
use crate::sql::PlanParser;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowMetrics;

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowMetrics {
    async fn analyze(self, ctx: DatabendQueryContextRef) -> common_exception::Result<AnalyzedResult> {
        let rewritten_query = "SELECT * FROM system.metrics";
        let rewritten_query_plan = PlanParser::build_from_sql_new(rewritten_query, ctx);
        Ok(AnalyzedResult::SimpleQuery(rewritten_query_plan.await?))
    }
}

