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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::ShowCreateDatabasePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct ShowCreateDatabaseInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowCreateDatabasePlan,
}

impl ShowCreateDatabaseInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: ShowCreateDatabasePlan,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(ShowCreateDatabaseInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowCreateDatabaseInterpreter {
    fn name(&self) -> &str {
        "ShowCreateDatabaseInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let tenant = self.ctx.get_tenant();
        let calalog = self.ctx.get_catalog();
        let db = calalog.get_database(tenant.as_str(), &self.plan.db).await?;
        let name = db.name();
        let mut info = format!("CREATE DATABASE `{}`", name);
        if !db.engine().is_empty() {
            let engine = format!(" ENGINE={}", db.engine().to_uppercase());
            let engine_options = db
                .engine_options()
                .iter()
                .map(|(k, v)| format!("{}='{}'", k, v))
                .collect::<Vec<_>>()
                .join(", ");
            if !engine_options.is_empty() {
                info.push_str(&format!("{}({})", engine, engine_options));
            } else {
                info.push_str(&engine);
            }
        }
        let schema = self.plan.schema();
        let block = DataBlock::create(schema.clone(), vec![
            Series::from_data(vec![name.as_bytes()]),
            Series::from_data(vec![info.into_bytes()]),
        ]);
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }
}
