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

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_transforms::blocks::TransformCastSchema;
use databend_common_pipeline_transforms::build_compact_block_pipeline;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::OnConflictField;
use databend_common_sql::ColumnBinding;
use databend_common_storages_fuse::operations::ReplaceIntoProcessor;
use databend_common_storages_fuse::operations::UnbranchedReplaceIntoProcessor;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::ColumnStatistics;

use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReplaceDeduplicate {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub on_conflicts: Vec<OnConflictField>,
    pub bloom_filter_column_indexes: Vec<FieldIndex>,
    pub table_is_empty: bool,
    pub table_info: TableInfo,
    pub target_schema: TableSchemaRef,
    pub select_ctx: Option<ReplaceSelectCtx>,
    pub table_level_range_index: HashMap<ColumnId, ColumnStatistics>,
    pub need_insert: bool,
    pub delete_when: Option<(RemoteExpr, String)>,
}

impl IPhysicalPlan for ReplaceDeduplicate {
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

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(ReplaceDeduplicate {
            meta: self.meta.clone(),
            input,
            on_conflicts: self.on_conflicts.clone(),
            bloom_filter_column_indexes: self.bloom_filter_column_indexes.clone(),
            table_is_empty: self.table_is_empty,
            table_info: self.table_info.clone(),
            target_schema: self.target_schema.clone(),
            select_ctx: self.select_ctx.clone(),
            table_level_range_index: self.table_level_range_index.clone(),
            need_insert: self.need_insert,
            delete_when: self.delete_when.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let tbl = builder
            .ctx
            .build_table_by_table_info(&self.table_info, None)?;
        let table = FuseTable::try_from_table(tbl.as_ref())?;

        let mut delete_column_idx = 0;
        let mut modified_schema = DataSchema::from(self.target_schema.clone()).into();
        if let Some(ReplaceSelectCtx {
            select_column_bindings,
            select_schema,
        }) = &self.select_ctx
        {
            PipelineBuilder::build_result_projection(
                &builder.func_ctx,
                self.input.output_schema()?,
                select_column_bindings,
                &mut builder.main_pipeline,
                false,
            )?;

            let mut target_schema: DataSchema = self.target_schema.clone().into();
            if let Some((_, delete_column)) = &self.delete_when {
                delete_column_idx = select_schema.index_of(delete_column.as_str())?;
                let delete_column = select_schema.field(delete_column_idx).clone();
                target_schema
                    .fields
                    .insert(delete_column_idx, delete_column);
                modified_schema = Arc::new(target_schema.clone());
            }
            let target_schema = Arc::new(target_schema.clone());
            if target_schema.fields().len() != select_schema.fields().len() {
                return Err(ErrorCode::BadArguments(
                    "The number of columns in the target table is different from the number of columns in the SELECT clause",
                ));
            }
            if PipelineBuilder::check_schema_cast(select_schema.clone(), target_schema.clone())? {
                builder.main_pipeline.try_add_transformer(|| {
                    TransformCastSchema::try_new(
                        select_schema.clone(),
                        target_schema.clone(),
                        builder.func_ctx.clone(),
                    )
                })?;
            }
        }

        PipelineBuilder::fill_and_reorder_columns(
            builder.ctx.clone(),
            &mut builder.main_pipeline,
            tbl.clone(),
            Arc::new(self.target_schema.clone().into()),
        )?;

        let block_thresholds = table.get_block_thresholds();
        build_compact_block_pipeline(&mut builder.main_pipeline, block_thresholds)?;

        let _ = table.cluster_gen_for_append(
            builder.ctx.clone(),
            &mut builder.main_pipeline,
            block_thresholds,
            Some(modified_schema),
        )?;
        // 1. resize input to 1, since the UpsertTransform need to de-duplicate inputs "globally"
        builder.main_pipeline.try_resize(1)?;

        // 2. connect with ReplaceIntoProcessor

        //                      ┌──────────────────────┐
        //                      │                      ├──┐
        // ┌─────────────┐      │                      ├──┘
        // │ UpsertSource├─────►│ ReplaceIntoProcessor │
        // └─────────────┘      │                      ├──┐
        //                      │                      ├──┘
        //                      └──────────────────────┘
        // NOTE: here the pipe items of last pipe are arranged in the following order
        // (0) -> output_port_append_data
        // (1) -> output_port_merge_into_action
        //    the "downstream" is supposed to be connected with a processor which can process MergeIntoOperations
        //    in our case, it is the broadcast processor
        let delete_when = if let Some((remote_expr, delete_column)) = &self.delete_when {
            Some((
                remote_expr.as_expr(&BUILTIN_FUNCTIONS),
                delete_column.clone(),
            ))
        } else {
            None
        };
        let cluster_keys = table.linear_cluster_keys(builder.ctx.clone());
        if self.need_insert {
            let replace_into_processor = ReplaceIntoProcessor::create(
                builder.ctx.clone(),
                self.on_conflicts.clone(),
                cluster_keys,
                self.bloom_filter_column_indexes.clone(),
                &table.schema(),
                self.table_is_empty,
                self.table_level_range_index.clone(),
                delete_when.map(|(expr, _)| (expr, delete_column_idx)),
            )?;
            builder
                .main_pipeline
                .add_pipe(replace_into_processor.into_pipe());
        } else {
            let replace_into_processor = UnbranchedReplaceIntoProcessor::create(
                builder.ctx.as_ref(),
                self.on_conflicts.clone(),
                cluster_keys,
                self.bloom_filter_column_indexes.clone(),
                &table.schema(),
                self.table_is_empty,
                self.table_level_range_index.clone(),
                delete_when.map(|_| delete_column_idx),
            )?;
            builder
                .main_pipeline
                .add_pipe(replace_into_processor.into_pipe());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReplaceSelectCtx {
    pub select_column_bindings: Vec<ColumnBinding>,
    pub select_schema: DataSchemaRef,
}
