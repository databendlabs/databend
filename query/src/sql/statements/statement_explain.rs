use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::ExplainType;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfQueryStatement;
use crate::sql::DfStatement;

#[derive(Debug, Clone, PartialEq)]
pub struct DfExplain {
    pub typ: ExplainType,
    pub statement: Box<DfStatement>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfExplain {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        match self.statement.as_ref() {
            DfStatement::Query(v) => Self::analyze_explain(ctx, v).await,
            _ => Err(ErrorCode::SyntaxException("Only support EXPLAIN SELECT")),
        }
    }
}

impl DfExplain {
    async fn analyze_explain(
        ctx: DatabendQueryContextRef,
        v: &DfQueryStatement,
    ) -> Result<AnalyzedResult> {
        match v.analyze(ctx).await? {
            // AnalyzedResult::SelectQuery(v) => Ok(AnalyzedResult::ExplainQuery(v)),
            _ => Err(ErrorCode::LogicalError(
                "Logical error: analyze select must be return select query analyze result.",
            )),
        }
    }
}
