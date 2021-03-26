// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use common_datablocks::DataBlock;
use common_datavalues::{DataField, DataSchema, DataType, StringArray};
use common_planners::{DfExplainType, ExplainPlan, PlanNode};
use log::debug;

use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::error::FuseQueryResult;
use crate::interpreters::IInterpreter;
use crate::optimizers::Optimizer;
use crate::processors::PipelineBuilder;
use crate::sessions::FuseQueryContextRef;

pub struct ExplainInterpreter {
    ctx: FuseQueryContextRef,
    explain: ExplainPlan,
}

impl ExplainInterpreter {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        explain: ExplainPlan,
    ) -> FuseQueryResult<Arc<dyn IInterpreter>> {
        Ok(Arc::new(ExplainInterpreter { ctx, explain }))
    }
}

#[async_trait]
impl IInterpreter for ExplainInterpreter {
    fn name(&self) -> &str {
        "ExplainInterpreter"
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let schema = Arc::new(DataSchema::new(vec![DataField::new(
            "explain",
            DataType::Utf8,
            false,
        )]));

        let plan = Optimizer::create(self.ctx.clone()).optimize(&self.explain.input)?;
        let result = match self.explain.typ {
            DfExplainType::Graph => {
                format!("{}", plan.display_graphviz())
            }
            DfExplainType::Pipeline => {
                let pipeline = PipelineBuilder::create(self.ctx.clone(), plan).build()?;
                format!("{:?}", pipeline)
            }
            _ => format!("{:?}", PlanNode::Explain(self.explain.clone())),
        };
        let block = DataBlock::create(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![result.as_str()]))],
        );
        debug!("Explain executor result: {:?}", block);

        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }
}
