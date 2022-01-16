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
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::planners::ShowUDFPlan;
use crate::sessions::QueryContext;

pub struct ShowUDFInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowUDFPlan,
}

impl ShowUDFInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowUDFPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ShowUDFInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowUDFInterpreter {
    fn name(&self) -> &str {
        "ShowUDFInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();
        let user_mgr = self.ctx.get_user_manager();
        let udf = user_mgr.get_udf(&tenant, &plan.name).await?;

        let show_fields = vec![
            DataField::new("name", DataType::String, false),
            DataField::new("parameters", DataType::String, false),
            DataField::new("definition", DataType::String, false),
            DataField::new("description", DataType::String, false),
        ];
        let show_schema = DataSchemaRefExt::create(show_fields);

        let block = DataBlock::create_by_array(show_schema.clone(), vec![
            Series::new(vec![udf.name.as_bytes()]),
            Series::new(vec![udf.parameters.join(", ").as_bytes()]),
            Series::new(vec![udf.definition.as_bytes()]),
            Series::new(vec![udf.description.as_bytes()]),
        ]);
        tracing::debug!("Show create udf executor result: {:?}", block);

        Ok(Box::pin(DataBlockStream::create(show_schema, None, vec![
            block,
        ])))
    }
}
