// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::time::Instant;

use common_exception::Result;
use common_planners::PlanNode;
use common_tracing::tracing;
use metrics::histogram;

use crate::optimizers::optimizer_scatters::ScattersOptimizer;
use crate::optimizers::ConstantFoldingOptimizer;
use crate::optimizers::ExprTransformOptimizer;
use crate::optimizers::ProjectionPushDownOptimizer;
use crate::optimizers::StatisticsExactOptimizer;
use crate::optimizers::TopNPushDownOptimizer;
use crate::sessions::DatabendQueryContextRef;

pub trait Optimizer {
    fn name(&self) -> &str;
    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode>;
}

pub struct Optimizers {
    inner: Vec<Box<dyn Optimizer>>,
}

impl Optimizers {
    pub fn create(ctx: DatabendQueryContextRef) -> Self {
        let mut optimizers = Self::without_scatters(ctx.clone());
        optimizers
            .inner
            .push(Box::new(ScattersOptimizer::create(ctx)));
        optimizers
    }

    pub fn without_scatters(ctx: DatabendQueryContextRef) -> Self {
        Optimizers {
            inner: vec![
                Box::new(ConstantFoldingOptimizer::create(ctx.clone())),
                Box::new(ExprTransformOptimizer::create(ctx.clone())),
                Box::new(ProjectionPushDownOptimizer::create(ctx.clone())),
                Box::new(TopNPushDownOptimizer::create(ctx.clone())),
                Box::new(StatisticsExactOptimizer::create(ctx)),
            ],
        }
    }

    pub fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let start = Instant::now();
        let mut plan = plan.clone();
        for optimizer in self.inner.iter_mut() {
            tracing::debug!("Before {} \n{:?}", optimizer.name(), plan);
            plan = optimizer.optimize(&plan)?;
            tracing::debug!("After {} \n{:?}", optimizer.name(), plan);
        }
        histogram!(super::metrics::METRIC_OPTIMIZE_USEDTIME, start.elapsed());
        Ok(plan)
    }
}
