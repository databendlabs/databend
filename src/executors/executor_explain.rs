// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use log::debug;
use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::datablocks::DataBlock;
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::datavalues::{DataField, DataSchema, DataType, StringArray};
use crate::error::FuseQueryResult;
use crate::executors::IExecutor;
use crate::planners::ExplainPlan;
use crate::processors::PipelineBuilder;

pub struct ExplainExecutor {
    ctx: Arc<FuseQueryContext>,
    explain: ExplainPlan,
}

impl ExplainExecutor {
    pub fn try_create(
        ctx: Arc<FuseQueryContext>,
        explain: ExplainPlan,
    ) -> FuseQueryResult<Arc<dyn IExecutor>> {
        Ok(Arc::new(ExplainExecutor { ctx, explain }))
    }
}

#[async_trait]
impl IExecutor for ExplainExecutor {
    fn name(&self) -> &'static str {
        "ExplainExecutor"
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let schema = Arc::new(DataSchema::new(vec![DataField::new(
            "explain",
            DataType::Utf8,
            false,
        )]));

        let pipeline =
            PipelineBuilder::create(self.ctx.clone(), self.explain.plan.clone()).build()?;

        let block = DataBlock::create(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                format!("{:?}", self.explain).as_str(),
                format!("{:?}", pipeline).as_str(),
            ]))],
        );
        debug!("Explain executor result: {:?}", block);

        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }
}
