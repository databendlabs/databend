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
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_core::PlanScope;

use crate::physical_plans::format::format_output_columns;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::physical_plan::DeriveHandle;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExchangeSource {
    pub meta: PhysicalPlanMeta,

    // Output schema of exchanged data
    pub schema: DataSchemaRef,

    // Fragment ID of source fragment
    pub source_fragment_id: usize,
    pub query_id: String,
}

#[typetag::serde]
impl IPhysicalPlan for ExchangeSource {
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
        Ok(self.schema.clone())
    }

    fn to_format_node(
        &self,
        ctx: &mut FormatContext<'_>,
        children: Vec<FormatTreeNode<String>>,
    ) -> Result<FormatTreeNode<String>> {
        let mut node_children = vec![FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(self.output_schema()?, &ctx.metadata, true)
        ))];

        node_children.push(FormatTreeNode::new(format!(
            "source fragment: [{}]",
            self.source_fragment_id
        )));

        node_children.extend(children);
        Ok(FormatTreeNode::with_children(
            "ExchangeSource".to_string(),
            node_children,
        ))
    }

    fn is_distributed_plan(&self) -> bool {
        true
    }

    fn display_in_profile(&self) -> bool {
        false
    }

    fn derive(&self, children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        assert!(children.is_empty());
        Box::new(self.clone())
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let exchange_manager = builder.ctx.get_exchange_manager();
        let build_res = exchange_manager.get_fragment_source(
            &self.query_id,
            self.source_fragment_id,
            builder.exchange_injector.clone(),
        )?;

        let plan_scope = PlanScope::get_plan_scope();
        let build_pipeline = build_res.main_pipeline.finalize(plan_scope);

        // add sharing data
        builder.join_state = build_res.builder_data.input_join_state;
        builder.merge_into_probe_data_fields = build_res.builder_data.input_probe_schema;

        // Merge pipeline
        assert_eq!(builder.main_pipeline.output_len(), 0);
        let sinks = builder.main_pipeline.merge(build_pipeline)?;
        builder.main_pipeline.extend_sinks(sinks);
        builder.pipelines.extend(build_res.sources_pipelines);
        Ok(())
    }
}
