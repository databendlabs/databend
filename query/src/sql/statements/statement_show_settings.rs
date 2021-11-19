use common_exception::Result;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::PlanParser;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowSettings;

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowSettings {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let rewritten_query = "SELECT name, value FROM system.settings ORDER BY name";
        let rewritten_query_plan = PlanParser::parse(rewritten_query, ctx);
        Ok(AnalyzedResult::SimpleQuery(rewritten_query_plan.await?))
    }
}
