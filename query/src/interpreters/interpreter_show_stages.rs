// Copyright 2022 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::ShowStagesPlan;
use common_streams::SendableDataBlockStream;

use super::SelectInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::sessions::QueryContext;
use crate::sql::PlanParser;

pub struct ShowStagesInterpreter {
    ctx: Arc<QueryContext>,
}

impl ShowStagesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, _plan: ShowStagesPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ShowStagesInterpreter { ctx }))
    }

    fn build_query(&self) -> Result<String> {
        Ok("SELECT name, stage_type, comment FROM system.stages ORDER BY name".to_string())
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowStagesInterpreter {
    fn name(&self) -> &str {
        "ShowStagesInterpreter"
    }

    async fn execute(
        &self,
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let query = self.build_query()?;
        let plan = PlanParser::parse(self.ctx.clone(), &query).await?;
        let optimized = Optimizers::create(self.ctx.clone()).optimize(&plan)?;

        if let PlanNode::Select(plan) = optimized {
            let interpreter = SelectInterpreter::try_create(self.ctx.clone(), plan)?;
            interpreter.execute(input_stream).await
        } else {
            return Err(ErrorCode::LogicalError("Show stages build query error"));
        }
    }
}
