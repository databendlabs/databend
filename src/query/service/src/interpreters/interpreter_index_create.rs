use std::sync::Arc;

use common_base::base::tokio;
use common_exception::Result;
use common_sql::plans::CreateIndexPlan;
use common_storages_fuse::FuseTable;
use common_storages_fuse::TableContext;

use super::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CreateIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateIndexPlan,
}

impl CreateIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateIndexPlan) -> Result<Self> {
        Ok(CreateIndexInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateIndexInterpreter {
    fn name(&self) -> &str {
        "CreateIndexInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;
        let nlists = plan.nlists.unwrap();
        let ctx = self.ctx.clone();
        let column_idx = table
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == &plan.column)
            .ok_or(common_exception::ErrorCode::UnknownColumn(format!(
                "column {} not found in table {}",
                plan.column,
                table.name()
            )))?;
        // build index in background task and return immediately
        let handle = tokio::spawn(async move {
            let table = FuseTable::try_from_table(table.as_ref())?;
            table.create_vector_index(ctx, column_idx, nlists).await?;
            Ok::<(), common_exception::ErrorCode>(())
        });
        handle.await.unwrap()?;
        Ok(PipelineBuildResult::create())
    }
}
