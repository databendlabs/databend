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
use std::sync::RwLock;

use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::CallPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use super::Interpreter;
use crate::procedures::ProcedureFactory;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct CallInterpreter {
    ctx: Arc<QueryContext>,
    plan: CallPlan,
    schema: RwLock<Option<DataSchemaRef>>,
}

impl CallInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CallPlan) -> Result<Self> {
        Ok(CallInterpreter {
            ctx,
            plan,
            schema: RwLock::new(None),
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for CallInterpreter {
    fn name(&self) -> &str {
        "CallInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.schema.read().unwrap().as_ref().unwrap().clone()
    }

    #[tracing::instrument(level = "debug", name = "call_interpreter_execute", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        mut _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = &self.plan;

        let name = plan.name.clone();
        let func = ProcedureFactory::instance().get(name)?;
        {
            let mut schema = self.schema.write().unwrap();
            *schema = Some(func.schema());
        }
        let blocks = func.eval(self.ctx.clone(), plan.args.clone()).await?;
        Ok(Box::pin(DataBlockStream::create(
            self.schema(),
            None,
            vec![blocks],
        )))
    }
}
