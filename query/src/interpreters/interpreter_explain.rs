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
use common_planners::ExplainPlan;
use common_planners::ExplainType;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::plan_schedulers;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::QueryContext;

pub struct ExplainInterpreter {
    ctx: Arc<QueryContext>,
    explain: ExplainPlan,
}

#[async_trait::async_trait]
impl Interpreter for ExplainInterpreter {
    fn name(&self) -> &str {
        "ExplainInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let schema = self.schema();

        let block = match self.explain.typ {
            ExplainType::Graph => self.explain_graph(),
            ExplainType::Syntax => self.explain_syntax(),
            ExplainType::Pipeline => self.explain_pipeline(),
        }?;

        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }

    fn schema(&self) -> DataSchemaRef {
        self.explain.schema()
    }
}

impl ExplainInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, explain: ExplainPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ExplainInterpreter { ctx, explain }))
    }

    fn explain_graph(&self) -> Result<DataBlock> {
        let schema = self.schema();
        let plan = plan_schedulers::apply_plan_rewrite(
            Optimizers::create(self.ctx.clone()),
            &self.explain.input,
        )?;
        let formatted_plan = Series::from_data(
            format!("{}", plan.display_graphviz())
                .lines()
                .map(|s| s.as_bytes())
                .collect::<Vec<_>>(),
        );
        Ok(DataBlock::create(schema, vec![formatted_plan]))
    }

    fn explain_syntax(&self) -> Result<DataBlock> {
        let schema = self.schema();
        let plan = plan_schedulers::apply_plan_rewrite(
            Optimizers::create(self.ctx.clone()),
            &self.explain.input,
        )?;
        let formatted_plan = Series::from_data(
            format!("{:?}", plan)
                .lines()
                .map(|s| s.as_bytes())
                .collect::<Vec<_>>(),
        );
        Ok(DataBlock::create(schema, vec![formatted_plan]))
    }

    fn explain_pipeline(&self) -> Result<DataBlock> {
        let schema = self.schema();
        let optimizer = Optimizers::without_scatters(self.ctx.clone());
        let plan = plan_schedulers::apply_plan_rewrite(optimizer, &self.explain.input)?;

        let pipeline_builder = PipelineBuilder::create(self.ctx.clone());
        let pipeline = pipeline_builder.build(&plan)?;
        let formatted_pipeline = Series::from_data(
            format!("{:?}", pipeline)
                .lines()
                .map(|s| s.as_bytes())
                .collect::<Vec<_>>(),
        );
        Ok(DataBlock::create(schema, vec![formatted_pipeline]))
    }
}
