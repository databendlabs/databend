// Copyright 2021 Datafuse Labs
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

use std::any::Any;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_sql::executor::physical_plans::FragmentKind;

use super::Exchange;
use crate::physical_plans::format::BroadcastSinkFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::processors::transforms::BroadcastSinkProcessor;
use crate::pipelines::processors::transforms::BroadcastSourceProcessor;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BroadcastSource {
    pub meta: PhysicalPlanMeta,
    pub broadcast_id: u32,
}

impl IPhysicalPlan for BroadcastSource {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert!(children.is_empty());
        PhysicalPlan::new(BroadcastSource {
            meta: self.meta.clone(),
            broadcast_id: self.broadcast_id,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let receiver = builder.ctx.broadcast_source_receiver(self.broadcast_id);

        builder.main_pipeline.add_source(
            |output| {
                BroadcastSourceProcessor::create(builder.ctx.clone(), receiver.clone(), output)
            },
            1,
        )
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BroadcastSink {
    pub meta: PhysicalPlanMeta,
    pub broadcast_id: u32,
    pub input: PhysicalPlan,
}

impl IPhysicalPlan for BroadcastSink {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRef::default())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(BroadcastSinkFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        PhysicalPlan::new(BroadcastSink {
            meta: self.meta.clone(),
            broadcast_id: self.broadcast_id,
            input: children.pop().unwrap(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        builder.main_pipeline.resize(1, true)?;
        builder.main_pipeline.add_sink(|input| {
            BroadcastSinkProcessor::create(
                input,
                builder.ctx.broadcast_sink_sender(self.broadcast_id),
            )
        })
    }
}

pub fn build_broadcast_plan(broadcast_id: u32) -> Result<PhysicalPlan> {
    let broadcast_source: PhysicalPlan = PhysicalPlan::new(BroadcastSource {
        meta: PhysicalPlanMeta::new("BroadcastSource"),
        broadcast_id,
    });

    let exchange = PhysicalPlan::new(Exchange {
        input: broadcast_source,
        kind: FragmentKind::Expansive,
        keys: vec![],
        allow_adjust_parallelism: true,
        ignore_exchange: false,
        meta: PhysicalPlanMeta::new("Exchange"),
    });

    Ok(PhysicalPlan::new(BroadcastSink {
        broadcast_id,
        input: exchange,
        meta: PhysicalPlanMeta::new("BroadcastSink"),
    }))
}

pub fn build_broadcast_plans(ctx: &dyn TableContext) -> Result<Vec<PhysicalPlan>> {
    let mut plans = vec![];
    let next_broadcast_id = ctx.get_next_broadcast_id();
    ctx.reset_broadcast_id();
    for broadcast_id in 0..next_broadcast_id {
        plans.push(build_broadcast_plan(broadcast_id)?);
    }
    Ok(plans)
}

crate::register_physical_plan!(BroadcastSink => crate::physical_plans::physical_broadcast::BroadcastSink);

crate::register_physical_plan!(BroadcastSource => crate::physical_plans::physical_broadcast::BroadcastSource);
