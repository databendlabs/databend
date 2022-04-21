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
use common_meta_types::RenameDatabaseReq;
use common_planners::RenameDatabasePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct RenameDatabaseInterpreter {
    ctx: Arc<QueryContext>,
    plan: RenameDatabasePlan,
}

impl RenameDatabaseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RenameDatabasePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(RenameDatabaseInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for RenameDatabaseInterpreter {
    fn name(&self) -> &str {
        "RenameDatabaseInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let catalog = self.ctx.get_catalog();
        for entity in &self.plan.entities {
            let tenant = self.plan.tenant.clone();
            catalog
                .rename_database(RenameDatabaseReq {
                    tenant,
                    if_exists: entity.if_exists,
                    db_name: entity.db.clone(),
                    new_db_name: entity.new_db.clone(),
                })
                .await?;
        }

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
