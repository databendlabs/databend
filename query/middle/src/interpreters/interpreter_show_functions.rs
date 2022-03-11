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
use common_planners::PlanShowKind;
use common_planners::ShowFunctionsPlan;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::SelectInterpreter;
use crate::optimizers::Optimizers;
use crate::sessions::QueryContext;
use crate::sql::PlanParser;

pub struct ShowFunctionsInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowFunctionsPlan,
}

impl ShowFunctionsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowFunctionsPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ShowFunctionsInterpreter { ctx, plan }))
    }

    fn build_query(&self) -> Result<String> {
        return match &self.plan.kind {
            PlanShowKind::All => Ok(
                "SELECT name, is_builtin, is_aggregate, definition, description FROM system.functions ORDER BY name"
                    .to_string(),
            ),
            PlanShowKind::Like(expr) => Ok(format!(
                "SELECT name, is_builtin, is_aggregate, definition, description FROM system.functions WHERE name LIKE {} ORDER BY name",
                expr
            )),
            PlanShowKind::Where(v) => Ok(format!(
                "SELECT name, is_builtin, is_aggregate, definition, description FROM system.functions WHERE {} ORDER BY name",
                v
            )),
            kind => Err(ErrorCode::UnImplement(format!(
                "Show functions unsupported: {:?}",
                kind
            ))),
        };
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowFunctionsInterpreter {
    fn name(&self) -> &str {
        "ShowFunctionsInterpreter"
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
            return Err(ErrorCode::LogicalError("Show functions build query error"));
        }
    }
}
