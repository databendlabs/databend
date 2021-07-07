// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::ExplainPlan;
use common_planners::ExplainType;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use log::debug;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::FuseQueryContextRef;

pub struct ExplainInterpreter {
    ctx: FuseQueryContextRef,
    explain: ExplainPlan,
}

impl ExplainInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, explain: ExplainPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ExplainInterpreter { ctx, explain }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ExplainInterpreter {
    fn name(&self) -> &str {
        "ExplainInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let schema =
            DataSchemaRefExt::create(vec![DataField::new("explain", DataType::Utf8, false)]);

        let plan = Optimizers::create(self.ctx.clone())
            .optimize(&self.explain.input)
            .await?;
        let result = match self.explain.typ {
            ExplainType::Graph => {
                format!("{}", plan.display_graphviz())
            }
            ExplainType::Pipeline => {
                let pipeline =
                    PipelineBuilder::create(self.ctx.clone(), HashMap::<String, bool>::new(), plan)
                        .build()?;
                format!("{:?}", pipeline)
            }
            _ => format!("{:?}", plan),
        };
        let block =
            DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![result.as_str()])]);
        debug!("Explain executor result: {:?}", block);

        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }

    fn schema(&self) -> DataSchemaRef {
        self.explain.schema()
    }
}
