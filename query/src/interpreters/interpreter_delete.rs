// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::DeletePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

/// interprets DeletePlan
pub struct DeleteInterpreter {
    ctx: Arc<QueryContext>,
    plan: DeletePlan,
}

impl DeleteInterpreter {
    /// Create the DelectInterpreter from DelectPlan
    pub fn try_create(ctx: Arc<QueryContext>, plan: DeletePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(DeleteInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for DeleteInterpreter {
    /// Get the name of current interpreter
    fn name(&self) -> &str {
        "DeleteInterpreter"
    }

    /// Get the schema of SelectPlan
    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[tracing::instrument(level = "debug", name = "delete_interpreter_execute", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        // TODO
        // 1. check privilege
        // 2. optimize the plan, at least constant folding?
        let catalog_name = self.plan.catalog_name.as_str();
        let db_name = self.plan.database_name.as_str();
        let tbl_name = self.plan.table_name.as_str();
        let tbl = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;
        tbl.delete(self.ctx.clone(), self.plan.clone()).await?;

        Ok(Box::pin(DataBlockStream::create(
            Arc::new(DataSchema::empty()),
            None,
            vec![],
        )))
    }
}
