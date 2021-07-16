// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::ExplainPlan;
use common_planners::ExplainType;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::FuseQueryContextRef;

pub struct ExplainInterpreter {
    ctx: FuseQueryContextRef,
    explain: ExplainPlan,
}

#[async_trait::async_trait]
impl Interpreter for ExplainInterpreter {
    fn name(&self) -> &str {
        "ExplainInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
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
    pub fn try_create(ctx: FuseQueryContextRef, explain: ExplainPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ExplainInterpreter { ctx, explain }))
    }

    fn explain_graph(&self) -> Result<DataBlock> {
        let schema = self.schema();
        let plan = Optimizers::create(self.ctx.clone()).optimize(&self.explain.input)?;
        let formatted_plan = Series::new(vec![format!("{}", plan.display_graphviz()).as_str()]);
        Ok(DataBlock::create_by_array(schema, vec![formatted_plan]))
    }

    fn explain_syntax(&self) -> Result<DataBlock> {
        let schema = self.schema();
        let plan = Optimizers::create(self.ctx.clone()).optimize(&self.explain.input)?;
        let formatted_plan = Series::new(vec![format!("{:?}", plan).as_str()]);
        Ok(DataBlock::create_by_array(schema, vec![formatted_plan]))
    }

    fn explain_pipeline(&self) -> Result<DataBlock> {
        let schema = self.schema();
        let plan = Optimizers::without_scatters(self.ctx.clone()).optimize(&self.explain.input)?;
        let pipeline_builder = PipelineBuilder::create(self.ctx.clone());
        let pipeline = pipeline_builder.build(&plan)?;
        let formatted_pipeline = Series::new(vec![format!("{:?}", pipeline).as_str()]);
        Ok(DataBlock::create_by_array(schema, vec![formatted_pipeline]))
    }
}
