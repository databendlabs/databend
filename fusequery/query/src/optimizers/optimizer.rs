// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_planners::PlanNode;
use common_tracing::tracing;

use crate::optimizers::optimizer_scatters::ScattersOptimizer;
use crate::optimizers::ProjectionPushDownOptimizer;
use crate::optimizers::StatisticsExactOptimizer;
use crate::sessions::FuseQueryContextRef;

#[async_trait::async_trait]
pub trait Optimizer: Send + Sync {
    fn name(&self) -> &str;
    async fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode>;
}

pub struct Optimizers {
    inner: Vec<Box<dyn Optimizer>>,
}

impl Optimizers {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        let optimizers: Vec<Box<dyn Optimizer>> = vec![
            Box::new(ProjectionPushDownOptimizer::create(ctx.clone())),
            Box::new(ScattersOptimizer::create(ctx.clone())),
            Box::new(StatisticsExactOptimizer::create(ctx)),
        ];
        Optimizers { inner: optimizers }
    }

    pub async fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut plan = plan.clone();
        for optimizer in self.inner.iter_mut() {
            tracing::debug!("Before {} \n{:?}", optimizer.name(), plan);
            plan = optimizer.optimize(&plan).await?;
            tracing::debug!("After {} \n{:?}", optimizer.name(), plan);
        }
        Ok(plan)
    }
}
