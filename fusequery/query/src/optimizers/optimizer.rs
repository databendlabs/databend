// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_planners::PlanNode;

use crate::optimizers::AliasPushDownOptimizer;
use crate::optimizers::LimitPushDownOptimizer;
use crate::optimizers::ProjectionPushDownOptimizer;
use crate::sessions::FuseQueryContextRef;
use crate::optimizers::optimizer_scatters::ScattersOptimizer;

pub trait IOptimizer {
    fn name(&self) -> &str;
    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode>;
}

pub struct Optimizer {
    optimizers: Vec<Box<dyn IOptimizer>>
}

impl Optimizer {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        let optimizers: Vec<Box<dyn IOptimizer>> = vec![
            Box::new(AliasPushDownOptimizer::create(ctx.clone())),
            Box::new(LimitPushDownOptimizer::create(ctx.clone())),
            Box::new(ProjectionPushDownOptimizer::create(ctx)),
            Box::new(ScattersOptimizer::create(ctx.clone()))
        ];
        Optimizer { optimizers }
    }

    pub fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut plan = plan.clone();
        for optimizer in self.optimizers.iter_mut() {
            plan = optimizer.optimize(&plan)?;
        }
        Ok(plan)
    }
}
