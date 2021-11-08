use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use crate::sessions::DatabendQueryContextRef;
use crate::sql::PlanParser;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowProcessList;

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowProcessList {
    async fn analyze(self, ctx: DatabendQueryContextRef) -> common_exception::Result<AnalyzedResult> {
        let rewritten_query = "SELECT name, value FROM system.settings ORDER BY name";
        let rewritten_query_plan = PlanParser::parse(rewritten_query, ctx);
        Ok(AnalyzedResult::SimpleQuery(rewritten_query_plan.await?))
    }
}

