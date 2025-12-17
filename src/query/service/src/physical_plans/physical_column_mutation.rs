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

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::physical_plans::format::ColumnMutationFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ColumnMutation {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub table_info: TableInfo,
    pub mutation_expr: Option<Vec<(usize, RemoteExpr)>>,
    pub computed_expr: Option<Vec<(usize, RemoteExpr)>>,
    pub mutation_kind: MutationKind,
    pub field_id_to_schema_index: HashMap<usize, usize>,
    pub input_num_columns: usize,
    pub has_filter_column: bool,
    pub table_meta_timestamps: TableMetaTimestamps,
    pub udf_col_num: usize,
}

impl IPhysicalPlan for ColumnMutation {
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
        Ok(ColumnMutationFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);

        PhysicalPlan::new(ColumnMutation {
            input: children.remove(0),
            meta: self.meta.clone(),
            table_info: self.table_info.clone(),
            mutation_expr: self.mutation_expr.clone(),
            computed_expr: self.computed_expr.clone(),
            mutation_kind: self.mutation_kind,
            field_id_to_schema_index: self.field_id_to_schema_index.clone(),
            input_num_columns: self.input_num_columns,
            has_filter_column: self.has_filter_column,
            table_meta_timestamps: self.table_meta_timestamps,
            udf_col_num: self.udf_col_num,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let mut field_id_to_schema_index = self.field_id_to_schema_index.clone();
        if let Some(mutation_expr) = &self.mutation_expr {
            let mut block_operators = Vec::new();
            let mut next_column_offset = self.input_num_columns;
            let mut schema_offset_to_new_offset = HashMap::new();

            // Build update expression BlockOperator.
            let mut exprs = Vec::with_capacity(mutation_expr.len());
            for (id, remote_expr) in mutation_expr {
                let expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);
                let schema_index = field_id_to_schema_index.get(id).unwrap();
                schema_offset_to_new_offset.insert(*schema_index, next_column_offset);
                field_id_to_schema_index
                    .entry(*id)
                    .and_modify(|e| *e = next_column_offset);
                next_column_offset += 1;
                exprs.push(expr);
            }
            if !exprs.is_empty() {
                block_operators.push(BlockOperator::Map {
                    exprs,
                    projections: None,
                });
            }

            // Build computed expression BlockOperator.
            if let Some(computed_expr) = &self.computed_expr
                && !computed_expr.is_empty()
            {
                let mut exprs = Vec::with_capacity(computed_expr.len());
                for (id, remote_expr) in computed_expr.iter() {
                    let expr =
                        remote_expr
                            .as_expr(&BUILTIN_FUNCTIONS)
                            .project_column_ref(|index| {
                                schema_offset_to_new_offset
                                    .get(index)
                                    .ok_or_else(|| {
                                        ErrorCode::BadArguments(format!(
                                            "Unable to get field named \"{}\"",
                                            index
                                        ))
                                    })
                                    .copied()
                            })?;
                    let schema_index = field_id_to_schema_index.get(id).unwrap();
                    schema_offset_to_new_offset.insert(*schema_index, next_column_offset);
                    field_id_to_schema_index
                        .entry(*id)
                        .and_modify(|e| *e = next_column_offset);
                    next_column_offset += 1;
                    exprs.push(expr);
                }
                block_operators.push(BlockOperator::Map {
                    exprs,
                    projections: None,
                });
            }

            // Keep the original order of the columns.
            let num_output_columns =
                self.input_num_columns - self.has_filter_column as usize - self.udf_col_num;
            let mut projection = Vec::with_capacity(num_output_columns);
            for idx in 0..num_output_columns {
                if let Some(index) = schema_offset_to_new_offset.get(&idx) {
                    projection.push(*index);
                } else {
                    projection.push(idx);
                }
            }
            block_operators.push(BlockOperator::Project { projection });

            builder.main_pipeline.add_transformer(|| {
                CompoundBlockOperator::new(
                    block_operators.clone(),
                    builder.func_ctx.clone(),
                    self.input_num_columns,
                )
            });
        }

        let table = builder
            .ctx
            .build_table_by_table_info(&self.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        let block_thresholds = table.get_block_thresholds();
        let cluster_stats_gen = if matches!(self.mutation_kind, MutationKind::Delete) {
            table.get_cluster_stats_gen(builder.ctx.clone(), 0, block_thresholds, None)?
        } else {
            table.cluster_gen_for_append(
                builder.ctx.clone(),
                &mut builder.main_pipeline,
                block_thresholds,
                None,
            )?
        };

        builder.main_pipeline.add_transform(|input, output| {
            let proc = TransformSerializeBlock::try_create(
                builder.ctx.clone(),
                input,
                output,
                table,
                cluster_stats_gen.clone(),
                self.mutation_kind,
                self.table_meta_timestamps,
            )?;
            proc.into_processor()
        })
    }
}

crate::register_physical_plan!(ColumnMutation => crate::physical_plans::physical_column_mutation::ColumnMutation);
