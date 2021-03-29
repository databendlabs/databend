// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use common_planners::SelectPlan;

use crate::datastreams::SendableDataBlockStream;
use crate::interpreters::IInterpreter;
use crate::optimizers::Optimizer;
use crate::processors::PipelineBuilder;
use crate::sessions::FuseQueryContextRef;

pub struct SelectInterpreter {
    ctx: FuseQueryContextRef,
    select: SelectPlan,
}

impl SelectInterpreter {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        select: SelectPlan,
    ) -> Result<Arc<dyn IInterpreter>> {
        Ok(Arc::new(SelectInterpreter { ctx, select }))
    }
}

#[async_trait]
impl IInterpreter for SelectInterpreter {
    fn name(&self) -> &str {
        "SelectInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let plan = Optimizer::create(self.ctx.clone()).optimize(&self.select.input)?;
        PipelineBuilder::create(self.ctx.clone(), plan)
            .build()?
            .execute()
            .await
    }
}
