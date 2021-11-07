use sqlparser::ast::ObjectName;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use crate::sessions::DatabendQueryContextRef;
use common_planners::{PlanNode, UseDatabasePlan};
use common_exception::{Result, ErrorCode};

#[derive(Debug, Clone, PartialEq)]
pub struct DfUseDatabase {
    pub name: ObjectName,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfUseDatabase {
    async fn analyze(self, _: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        if self.name.0.is_empty() {
            return Result::Err(ErrorCode::SyntaxException("Use database name is empty"));
        }

        let db = self.name.0[0].value.clone();
        Ok(AnalyzedResult::SimpleQuery(PlanNode::UseDatabase(UseDatabasePlan { db })))
    }
}

