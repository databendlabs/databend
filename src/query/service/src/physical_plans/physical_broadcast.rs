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

use databend_common_ast::ast::FormatTreeNode;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;

use super::Exchange;
use super::FragmentKind;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::{BroadcastSinkProcessor, BroadcastSourceProcessor};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BroadcastSource {
    pub meta: PhysicalPlanMeta,
    pub broadcast_id: u32,
}

#[typetag::serde]
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

    fn derive(&self, children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        assert!(children.is_empty());
        Box::new(self.clone())
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let receiver = builder.ctx.broadcast_source_receiver(self.broadcast_id);

        builder.main_pipeline.add_source(
            |output| BroadcastSourceProcessor::create(builder.ctx.clone(), receiver.clone(), output),
            1,
        )
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BroadcastSink {
    pub meta: PhysicalPlanMeta,
    pub broadcast_id: u32,
    pub input: Box<dyn IPhysicalPlan>,
}

#[typetag::serde]
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

    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRef::default())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn to_format_node(
        &self,
        _ctx: &mut FormatContext<'_>,
        _: Vec<FormatTreeNode<String>>,
    ) -> Result<FormatTreeNode<String>> {
        // ignore children
        Ok(FormatTreeNode::new("RuntimeFilterSink".to_string()))
    }

    fn derive(&self, mut children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        let mut new_physical_plan = self.clone();
        assert_eq!(children.len(), 1);
        new_physical_plan.input = children.pop().unwrap();
        Box::new(new_physical_plan)
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        builder.main_pipeline.resize(1, true)?;
        builder.main_pipeline.add_sink(|input| {
            BroadcastSinkProcessor::create(input, builder.ctx.broadcast_sink_sender(self.broadcast_id))
        })
    }
}

pub fn build_broadcast_plan(broadcast_id: u32) -> Result<Box<dyn IPhysicalPlan>> {
    let broadcast_source: Box<dyn IPhysicalPlan> = Box::new(BroadcastSource {
        meta: PhysicalPlanMeta::new("BroadcastSource"),
        broadcast_id,
    });

    let exchange = Box::new(Exchange {
        input: broadcast_source,
        kind: FragmentKind::Expansive,
        keys: vec![],
        allow_adjust_parallelism: true,
        ignore_exchange: false,
        meta: PhysicalPlanMeta::new("Exchange"),
    });

    Ok(Box::new(BroadcastSink {
        broadcast_id,
        input: exchange,
        meta: PhysicalPlanMeta::new("BroadcastSink"),
    }))
}

pub fn build_broadcast_plans(ctx: &dyn TableContext) -> Result<Vec<Box<dyn IPhysicalPlan>>> {
    let mut plans = vec![];
    let next_broadcast_id = ctx.get_next_broadcast_id();
    ctx.reset_broadcast_id();
    for broadcast_id in 0..next_broadcast_id {
        plans.push(build_broadcast_plan(broadcast_id)?);
    }
    Ok(plans)
}
