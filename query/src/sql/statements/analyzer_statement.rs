use common_planners::PlanNode;
use crate::sql::DfStatement;
use common_exception::Result;
use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzeQueryState;

pub enum AnalyzedResult {
    SimpleQuery(PlanNode),
    SelectQuery(AnalyzeQueryState),
    ExplainQuery(AnalyzeQueryState),
}

#[async_trait::async_trait]
pub trait AnalyzableStatement {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult>;
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfStatement {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        match self {
            DfStatement::Query(v) => v.analyze(ctx).await,
            DfStatement::Explain(v) => v.analyze(ctx).await,
            DfStatement::ShowDatabases(v) => v.analyze(ctx).await,
            DfStatement::CreateDatabase(v) => v.analyze(ctx).await,
            DfStatement::DropDatabase(v) => v.analyze(ctx).await,
            DfStatement::CreateTable(v) => v.analyze(ctx).await,
            DfStatement::DescribeTable(v) => v.analyze(ctx).await,
            DfStatement::DropTable(v) => v.analyze(ctx).await,
            DfStatement::TruncateTable(v) => v.analyze(ctx).await,
            DfStatement::UseDatabase(v) => v.analyze(ctx).await,
            DfStatement::ShowCreateTable(v) => v.analyze(ctx).await,
            DfStatement::ShowTables(v) => v.analyze(ctx).await,
            DfStatement::ShowSettings(v) => v.analyze(ctx).await,
            DfStatement::ShowProcessList(v) => v.analyze(ctx).await,
            DfStatement::ShowMetrics(v) => v.analyze(ctx).await,
            DfStatement::KillStatement(v) => v.analyze(ctx).await,
            DfStatement::InsertQuery(v) => v.analyze(ctx).await,
            DfStatement::SetVariable(v) => v.analyze(ctx).await
        }
    }
}
