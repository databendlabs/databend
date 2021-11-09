use common_planners::ExplainType;

use crate::sql::DfStatement;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult, DfQueryStatement};
use crate::sessions::DatabendQueryContextRef;
use common_exception::{Result, ErrorCode};

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
            _ => Err(ErrorCode::SyntaxException("Only support EXPLAIN SELECT"))
        }
    }
}

impl DfExplain {
    async fn analyze_explain(ctx: DatabendQueryContextRef, v: &DfQueryStatement) -> Result<AnalyzedResult> {
        match v.analyze(ctx).await? {
            AnalyzedResult::SelectQuery(v) => Ok(AnalyzedResult::ExplainQuery(v)),
            _ => Err(ErrorCode::LogicalError("Logical error: analyze select must be return select query analyze result."))
        }
    }
}
