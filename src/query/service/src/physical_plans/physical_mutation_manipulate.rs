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
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_core::Pipe;
use databend_common_sql::binder::MutationStrategy;
use databend_common_sql::executor::physical_plans::MatchExpr;
use databend_common_storages_fuse::operations::MatchedSplitProcessor;
use databend_common_storages_fuse::operations::MergeIntoNotMatchedProcessor;
use itertools::Itertools;

use crate::physical_plans::format::FormatContext;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MutationManipulate {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub table_info: TableInfo,
    // (DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>,Vec<usize>) => (source_schema, condition, value_exprs)
    pub unmatched: Vec<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>,
    // the first option stands for the condition
    // the second option stands for update/delete
    pub matched: MatchExpr,
    // used to record the index of target table's field in merge_source_schema
    pub field_index_of_input_schema: HashMap<FieldIndex, usize>,
    pub strategy: MutationStrategy,
    pub row_id_idx: usize,
    pub can_try_update_column_only: bool,
    pub unmatched_schema: DataSchemaRef,
    pub target_table_index: usize,
}

#[typetag::serde]
impl IPhysicalPlan for MutationManipulate {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
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
        ctx: &mut FormatContext<'_>,
        children: Vec<FormatTreeNode<String>>,
    ) -> Result<FormatTreeNode<String>> {
        let table_entry = ctx.metadata.table(self.target_table_index).clone();
        let target_schema = table_entry.table().schema_with_stream();

        // Matched clauses.
        let mut matched_children = Vec::with_capacity(self.matched.len());
        for evaluator in &self.matched {
            let condition_format = evaluator.0.as_ref().map_or_else(
                || "condition: None".to_string(),
                |predicate| {
                    format!(
                        "condition: {}",
                        predicate.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                    )
                },
            );

            if evaluator.1.is_none() {
                matched_children.push(FormatTreeNode::new(format!(
                    "matched delete: [{}]",
                    condition_format
                )));
            } else {
                let mut update_list = evaluator.1.as_ref().unwrap().clone();
                update_list.sort_by(|a, b| a.0.cmp(&b.0));
                let update_format = update_list
                    .iter()
                    .map(|(field_idx, expr)| {
                        format!(
                            "{} = {}",
                            target_schema.field(*field_idx).name(),
                            expr.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                        )
                    })
                    .join(",");
                matched_children.push(FormatTreeNode::new(format!(
                    "matched update: [{}, update set {}]",
                    condition_format, update_format
                )));
            }
        }

        // UnMatched clauses.
        let mut unmatched_children = Vec::with_capacity(self.unmatched.len());
        for evaluator in &self.unmatched {
            let condition_format = evaluator.1.as_ref().map_or_else(
                || "condition: None".to_string(),
                |predicate| {
                    format!(
                        "condition: {}",
                        predicate.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                    )
                },
            );
            let insert_schema_format = evaluator
                .0
                .fields
                .iter()
                .map(|field| field.name())
                .join(",");

            let values_format = evaluator
                .2
                .iter()
                .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                .join(",");

            let unmatched_format = format!(
                "insert into ({}) values({})",
                insert_schema_format, values_format
            );

            unmatched_children.push(FormatTreeNode::new(format!(
                "unmatched insert: [{}, {}]",
                condition_format, unmatched_format
            )));
        }

        let mut node_children = vec![];

        node_children.extend(matched_children);
        node_children.extend(unmatched_children);
        node_children.extend(children);

        Ok(FormatTreeNode::with_children(
            "MutationManipulate".to_string(),
            node_children,
        ))
    }

    fn derive(&self, mut children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        let mut new_physical_plan = self.clone();
        assert_eq!(children.len(), 1);
        new_physical_plan.input = children.pop().unwrap();
        Box::new(new_physical_plan)
    }

    // Handle matched and unmatched data separately.
    // This is a complete pipeline with matched and not matched clauses, for matched only or unmatched only
    // we will delicate useless pipeline and processor
    //                                                                                 +-----------------------------+-+
    //                                    +-----------------------+     Matched        |                             +-+
    //                                    |                       +---+--------------->|    MatchedSplitProcessor    |
    //                                    |                       |   |                |                             +-+
    // +----------------------+           |                       +---+                +-----------------------------+-+
    // |      MergeInto       +---------->|MutationSplitProcessor |
    // +----------------------+           |                       +---+                +-----------------------------+
    //                                    |                       |   | NotMatched     |                             +-+
    //                                    |                       +---+--------------->| MergeIntoNotMatchedProcessor| |
    //                                    +-----------------------+                    |                             +-+
    //                                                                                 +-----------------------------+
    // Note: here the output_port of MatchedSplitProcessor are arranged in the following order
    // (0) -> output_port_row_id
    // (1) -> output_port_updated

    // Outputs from MatchedSplitProcessor's output_port_updated and MergeIntoNotMatchedProcessor's output_port are merged and processed uniformly by the subsequent ResizeProcessor
    // receive matched data and not matched data parallelly.
    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let (step, need_match, need_unmatch) = match self.strategy {
            MutationStrategy::MatchedOnly => (1, true, false),
            MutationStrategy::NotMatchedOnly => (1, false, true),
            MutationStrategy::MixedMatched => (2, true, true),
            MutationStrategy::Direct => unreachable!(),
        };

        let tbl = builder
            .ctx
            .build_table_by_table_info(&self.table_info, None)?;

        let input_schema = self.input.output_schema()?;
        let mut pipe_items = Vec::with_capacity(builder.main_pipeline.output_len());
        for _ in (0..builder.main_pipeline.output_len()).step_by(step) {
            if need_match {
                let matched_split_processor = MatchedSplitProcessor::create(
                    builder.ctx.clone(),
                    self.row_id_idx,
                    self.matched.clone(),
                    self.field_index_of_input_schema.clone(),
                    input_schema.clone(),
                    Arc::new(DataSchema::from(tbl.schema_with_stream())),
                    false,
                    self.can_try_update_column_only,
                )?;
                pipe_items.push(matched_split_processor.into_pipe_item());
            }

            if need_unmatch {
                let merge_into_not_matched_processor = MergeIntoNotMatchedProcessor::create(
                    self.unmatched.clone(),
                    self.unmatched_schema.clone(),
                    builder.func_ctx.clone(),
                    builder.ctx.clone(),
                )?;
                pipe_items.push(merge_into_not_matched_processor.into_pipe_item());
            }
        }

        let output_len = pipe_items.iter().map(|item| item.outputs_port.len()).sum();
        builder.main_pipeline.add_pipe(Pipe::create(
            builder.main_pipeline.output_len(),
            output_len,
            pipe_items.clone(),
        ));

        Ok(())
    }
}
