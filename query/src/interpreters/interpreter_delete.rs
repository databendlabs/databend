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

use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeType;
use common_planners::DeletePlan;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct DeleteInterpreter {
    ctx: Arc<QueryContext>,
    delete: DeletePlan,
}

impl DeleteInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, delete: DeletePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(DeleteInterpreter { ctx, delete }))
    }
}

#[async_trait::async_trait]
impl Interpreter for DeleteInterpreter {
    fn name(&self) -> &str {
        "SelectInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.delete.schema()
    }

    #[tracing::instrument(level = "debug", name = "delete_interpreter_execute", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let db = self.delete.database_name.as_str();
        let tbl = self.delete.table_name.as_str();
        self.ctx
            .get_current_session()
            .validate_privilege(
                &GrantObject::Table(db.to_owned(), tbl.to_owned()),
                UserPrivilegeType::Delete,
            )
            .await?;

        let table = self.ctx.get_table(db, tbl).await?;

        let operation_log = table.delete_from(&self.delete.selection);
        todo!()
    }
}
