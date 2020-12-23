// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use log::debug;
use std::sync::Arc;

use crate::contexts::FuseQueryContextRef;
use crate::datablocks::DataBlock;
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::datavalues::{DataField, DataSchema, DataType, StringArray};
use crate::error::FuseQueryResult;
use crate::executors::IExecutor;
use crate::optimizers::Optimizer;
use crate::planners::{ExplainPlan, PlanNode};
use crate::processors::PipelineBuilder;

pub struct ExplainExecutor {
    ctx: FuseQueryContextRef,
    explain: ExplainPlan,
}

impl ExplainExecutor {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        explain: ExplainPlan,
    ) -> FuseQueryResult<Arc<dyn IExecutor>> {
        Ok(Arc::new(ExplainExecutor { ctx, explain }))
    }
}

#[async_trait]
impl IExecutor for ExplainExecutor {
    fn name(&self) -> &str {
        "ExplainExecutor"
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let schema = Arc::new(DataSchema::new(vec![DataField::new(
            "explain",
            DataType::Utf8,
            false,
        )]));

        let plan = Optimizer::create().optimize(&self.explain.plan)?;
        let pipeline = PipelineBuilder::create(self.ctx.clone(), plan).build()?;
        let block = DataBlock::create(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                format!("{:?}", PlanNode::Explain(self.explain.clone())).as_str(),
                format!("{:?}", pipeline).as_str(),
            ]))],
        );
        debug!("Explain executor result: {:?}", block);

        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }
}
