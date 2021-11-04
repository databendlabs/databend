use common_planners::PlanNode;
use crate::sql::DfStatement;
use common_exception::Result;
use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzeData;

pub enum AnalyzedResult {
    SimpleQuery(PlanNode),
    SelectQuery(AnalyzeData),
}

#[async_trait::async_trait]
pub trait AnalyzableStatement {
    async fn analyze(self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult>;
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfStatement {
    async fn analyze(self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        match self {
            DfStatement::Statement(v) => self.sql_statement_to_plan(v),
            DfStatement::Explain(v) => self.sql_explain_to_plan(v),
            DfStatement::ShowDatabases(v) => v.analyze(ctx),
            DfStatement::CreateDatabase(v) => v.analyze(ctx),
            DfStatement::DropDatabase(v) => v.analyze(ctx),
            DfStatement::CreateTable(v) => v.analyze(ctx),
            DfStatement::DescribeTable(v) => v.analyze(ctx),
            DfStatement::DropTable(v) => v.analyze(ctx),
            DfStatement::TruncateTable(v) => v.analyze(ctx),
            DfStatement::UseDatabase(v) => v.analyze(ctx),
            DfStatement::ShowCreateTable(v) => v.analyze(ctx),
            DfStatement::ShowTables(v) => v.analyze(ctx),
            DfStatement::ShowSettings(v) => v.analyze(ctx),
            DfStatement::ShowProcessList(v) => v.analyze(ctx),
            DfStatement::ShowMetrics(v) => v.analyze(ctx),
            DfStatement::KillStatement(v) => v.analyze(ctx),
            DfStatement::InsertQuery(v) => v.analyze(ctx),
            DfStatement::SetVariable(v) => v.analyze(ctx)
        }
    }
}
