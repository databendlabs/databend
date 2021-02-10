// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::contexts::FuseQueryContextRef;
use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;
use crate::interpreters::IInterpreter;
use crate::optimizers::Optimizer;
use crate::planners::SelectPlan;
use crate::processors::PipelineBuilder;

pub struct SelectInterpreter {
    ctx: FuseQueryContextRef,
    select: SelectPlan,
}

impl SelectInterpreter {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        select: SelectPlan,
    ) -> FuseQueryResult<Arc<dyn IInterpreter>> {
        Ok(Arc::new(SelectInterpreter { ctx, select }))
    }
}

#[async_trait]
impl IInterpreter for SelectInterpreter {
    fn name(&self) -> &str {
        "SelectInterpreter"
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let plan = Optimizer::create(self.ctx.clone()).optimize(&self.select.plan)?;
        PipelineBuilder::create(self.ctx.clone(), plan)
            .build()?
            .execute()
            .await
    }
}
