// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;
use crate::executors::IExecutor;
use crate::optimizers::Optimizer;
use crate::planners::SelectPlan;
use crate::processors::PipelineBuilder;

pub struct SelectExecutor {
    ctx: Arc<FuseQueryContext>,
    select: SelectPlan,
}

impl SelectExecutor {
    pub fn try_create(
        ctx: Arc<FuseQueryContext>,
        select: SelectPlan,
    ) -> FuseQueryResult<Arc<dyn IExecutor>> {
        Ok(Arc::new(SelectExecutor { ctx, select }))
    }
}

#[async_trait]
impl IExecutor for SelectExecutor {
    fn name(&self) -> &str {
        "SelectExecutor"
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let plan = Optimizer::create().optimize(&self.select.plan)?;
        PipelineBuilder::create(self.ctx.clone(), plan)
            .build()?
            .execute()
            .await
    }
}
