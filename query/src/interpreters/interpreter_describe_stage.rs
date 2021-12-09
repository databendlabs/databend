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
use common_datavalues::series::Series;
use common_exception::Result;
use common_planners::DescribeStagePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct DescribeStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescribeStagePlan,
}

impl DescribeStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescribeStagePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(DescribeStageInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for DescribeStageInterpreter {
    fn name(&self) -> &str {
        "DescribeStageInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let schema = self.plan.schema();
        let stage = self.ctx
        .get_sessions_manager()
        .get_user_manager()
        .get_stage(self.plan.name.as_str())
        .await?;

        let mut names: Vec<&str> = vec![];
        let mut values: Vec<&str> = vec![];

        names.push("name");
        names.push("name");
        names.push("name");
        names.push("name");

        values.push(stage.stage_name.as_str());

        let block = DataBlock::create_by_array(schema.clone(), vec![
            Series::new(names),
            Series::new(values),
        ]);

        Ok(Box::pin(DataBlockStream::create(schema, None, vec![
            block,
        ])))
    }
}
