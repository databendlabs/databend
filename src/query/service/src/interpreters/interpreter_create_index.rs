use std::sync::Arc;

use common_exception::Result;
use common_sql::plans::CreateIndexPlan;

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
        todo!()
    }
}
