use common_base::tokio;
use common_exception::{Result, ErrorCode};
use crate::sql::DfParser;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult, QueryNormalizerData};
use crate::tests::try_create_context;
use crate::sql::statements::query::AnalyzeQuerySchema;

#[tokio::test]
async fn test_statement_select_analyze() -> Result<()> {
    Ok(())
}
