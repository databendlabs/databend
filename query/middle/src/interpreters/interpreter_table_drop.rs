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

use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeType;
use common_planners::DropTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct DropTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTablePlan,
}

impl DropTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTablePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(DropTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableInterpreter {
    fn name(&self) -> &str {
        "DropTableInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let db_name = self.plan.db.as_str();
        let tbl_name = self.plan.table.as_str();
        let tbl = self.ctx.get_table(db_name, tbl_name).await.ok();

        self.ctx
            .get_current_session()
            .validate_privilege(
                &GrantObject::Database(db_name.into()),
                UserPrivilegeType::Drop,
            )
            .await?;

        let catalog = self.ctx.get_catalog();
        catalog.drop_table(self.plan.clone().into()).await?;

        // `drop_table` throws several types of exceptions
        // thus `optimize` operation is executed after it.
        if let Some(tbl) = tbl {
            let keep_last_snapshot = false;
            tbl.optimize(self.ctx.clone(), keep_last_snapshot).await?;
        }

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
