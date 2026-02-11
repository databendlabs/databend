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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::LimitType;
use databend_common_expression::RemoteExpr;
use databend_common_expression::SortColumnDescription;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_pipeline::core::DynTransformBuilder;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSinker;
use databend_common_pipeline_transforms::AsyncTransformer;
use databend_common_pipeline_transforms::Transformer;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::columns::TransformAddComputedColumns;
use databend_common_pipeline_transforms::sorts::TransformSortPartial;
use databend_common_sql::ColumnSet;
use databend_common_sql::DefaultExprBinder;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::CommitMultiTableInsert;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::physical_plans::format::ChunkAppendDataFormatter;
use crate::physical_plans::format::ChunkCastSchemaFormatter;
use crate::physical_plans::format::ChunkEvalScalarFormatter;
use crate::physical_plans::format::ChunkFillAndReorderFormatter;
use crate::physical_plans::format::ChunkFilterFormatter;
use crate::physical_plans::format::ChunkMergeFormatter;
use crate::physical_plans::format::DuplicateFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::ShuffleFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::TransformAsyncFunction;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Duplicate {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub n: usize,
}

#[typetag::serde]
impl IPhysicalPlan for Duplicate {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(DuplicateFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(Duplicate {
            meta: self.meta.clone(),
            input,
            n: self.n,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        builder.main_pipeline.duplicate(true, self.n)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Shuffle {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub strategy: ShuffleStrategy,
}

#[typetag::serde]
impl IPhysicalPlan for Shuffle {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(ShuffleFormatter::create(self))
    }

    fn display_in_profile(&self) -> bool {
        false
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(Shuffle {
            meta: self.meta.clone(),
            input,
            strategy: self.strategy.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        builder
            .main_pipeline
            .reorder_inputs(self.strategy.shuffle(builder.main_pipeline.output_len())?);
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ShuffleStrategy {
    Transpose(usize),
}

impl ShuffleStrategy {
    pub fn shuffle(&self, total: usize) -> Result<Vec<usize>> {
        match self {
            ShuffleStrategy::Transpose(n) => {
                if total % n != 0 {
                    return Err(ErrorCode::Internal(format!(
                        "total rows {} is not divisible by n {}",
                        total, n
                    )));
                }
                let mut result = vec![0; total];
                for i in 0..*n {
                    for j in 0..total / n {
                        result[i + j * n] = i * (total / n) + j;
                    }
                }
                Ok(result)
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkFilter {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub predicates: Vec<Option<RemoteExpr>>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkFilter {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(ChunkFilterFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(ChunkFilter {
            meta: self.meta.clone(),
            input,
            predicates: self.predicates.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        if self.predicates.iter().all(|x| x.is_none()) {
            return Ok(());
        }
        let mut f: Vec<DynTransformBuilder> = Vec::with_capacity(self.predicates.len());
        let projection: ColumnSet = (0..self.input.output_schema()?.fields.len()).collect();
        for predicate in self.predicates.iter() {
            if let Some(predicate) = predicate {
                f.push(Box::new(builder.filter_transform_builder(
                    &[predicate.clone()],
                    projection.clone(),
                )?));
            } else {
                f.push(Box::new(builder.dummy_transform_builder()));
            }
        }

        builder.main_pipeline.add_transforms_by_chunk(f)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkEvalScalar {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub eval_scalars: Vec<Option<MultiInsertEvalScalar>>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkEvalScalar {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(ChunkEvalScalarFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(ChunkEvalScalar {
            meta: self.meta.clone(),
            input,
            eval_scalars: self.eval_scalars.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        if self.eval_scalars.iter().all(|x| x.is_none()) {
            return Ok(());
        }
        let num_input_columns = self.input.output_schema()?.num_fields();
        let mut f: Vec<DynTransformBuilder> = Vec::with_capacity(self.eval_scalars.len());
        for eval_scalar in self.eval_scalars.iter() {
            if let Some(eval_scalar) = eval_scalar {
                f.push(Box::new(builder.map_transform_builder(
                    num_input_columns,
                    eval_scalar.remote_exprs.clone(),
                    Some(eval_scalar.projection.clone()),
                )?));
            } else {
                f.push(Box::new(builder.dummy_transform_builder()));
            }
        }

        builder.main_pipeline.add_transforms_by_chunk(f)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MultiInsertEvalScalar {
    pub remote_exprs: Vec<RemoteExpr>,
    pub projection: ColumnSet,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkCastSchema {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub cast_schemas: Vec<Option<CastSchema>>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkCastSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(ChunkCastSchemaFormatter::create(self))
    }

    fn display_in_profile(&self) -> bool {
        false
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(ChunkCastSchema {
            meta: self.meta.clone(),
            input,
            cast_schemas: self.cast_schemas.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        if self.cast_schemas.iter().all(|x| x.is_none()) {
            return Ok(());
        }
        let mut f: Vec<DynTransformBuilder> = Vec::with_capacity(self.cast_schemas.len());
        for cast_schema in self.cast_schemas.iter() {
            if let Some(cast_schema) = cast_schema {
                f.push(Box::new(builder.cast_schema_transform_builder(
                    cast_schema.source_schema.clone(),
                    cast_schema.target_schema.clone(),
                )?));
            } else {
                f.push(Box::new(builder.dummy_transform_builder()));
            }
        }
        builder.main_pipeline.add_transforms_by_chunk(f)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CastSchema {
    pub source_schema: DataSchemaRef,
    pub target_schema: DataSchemaRef,
}

#[derive(Default)]
struct ChunkFillPlan {
    async_builder: Option<DynTransformBuilder>,
    cast_builder: Option<DynTransformBuilder>,
    resort_builder: Option<DynTransformBuilder>,
    computed_builder: Option<DynTransformBuilder>,
}

impl ChunkFillPlan {
    fn materialize_stage(
        plans: &mut [Self],
        builder: &mut PipelineBuilder,
        exists: fn(&Self) -> bool,
        take: fn(&mut Self) -> Option<DynTransformBuilder>,
    ) -> Result<()> {
        // Skip the stage entirely if no plan participates.
        if !plans.iter().any(exists) {
            return Ok(());
        }

        let mut builders = Vec::with_capacity(plans.len());
        for plan in plans.iter_mut() {
            builders
                .push(take(plan).unwrap_or_else(|| Box::new(builder.dummy_transform_builder())));
        }
        builder.main_pipeline.add_transforms_by_chunk(builders)
    }

    fn materialize_chunk_fill_plans(
        plans: &mut [Self],
        builder: &mut PipelineBuilder,
    ) -> Result<()> {
        // 1. Async default expressions (AUTO_INCREMENT, async defaults).
        Self::materialize_stage(
            plans,
            builder,
            |p| p.async_builder.is_some(),
            |p| p.async_builder.take(),
        )?;

        // 2. Cast after async defaults.
        Self::materialize_stage(
            plans,
            builder,
            |p| p.cast_builder.is_some(),
            |p| p.cast_builder.take(),
        )?;

        // 3. Reorder / fill missing columns.
        Self::materialize_stage(
            plans,
            builder,
            |p| p.resort_builder.is_some(),
            |p| p.resort_builder.take(),
        )?;

        // 4. Computed columns.
        Self::materialize_stage(
            plans,
            builder,
            |p| p.computed_builder.is_some(),
            |p| p.computed_builder.take(),
        )?;

        Ok(())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkFillAndReorder {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub fill_and_reorders: Vec<Option<FillAndReorder>>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkFillAndReorder {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(ChunkFillAndReorderFormatter::create(self))
    }

    fn display_in_profile(&self) -> bool {
        false
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(ChunkFillAndReorder {
            meta: self.meta.clone(),
            input,
            fill_and_reorders: self.fill_and_reorders.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        if self.fill_and_reorders.iter().all(|x| x.is_none()) {
            return Ok(());
        }

        let mut plans = Vec::with_capacity(self.fill_and_reorders.len());

        for fill_and_reorder in &self.fill_and_reorders {
            if let Some(fill_and_reorder) = fill_and_reorder {
                let table = builder.ctx.build_table_by_table_info(
                    &fill_and_reorder.target_table_info,
                    &None,
                    None,
                )?;

                let table_default_schema = &table.schema().remove_computed_fields();
                let table_computed_schema = &table.schema().remove_virtual_computed_fields();
                let default_schema: DataSchemaRef = Arc::new(table_default_schema.into());
                let computed_schema: DataSchemaRef = Arc::new(table_computed_schema.into());

                let mut plan = ChunkFillPlan::default();

                if fill_and_reorder.source_schema != default_schema {
                    let mut binder = DefaultExprBinder::try_new(builder.ctx.clone())?
                        .auto_increment_table_id(table.get_id());

                    if let Some((async_funcs, new_default_schema, new_default_schema_no_cast)) =
                        binder.split_async_default_exprs(
                            fill_and_reorder.source_schema.clone(),
                            default_schema.clone(),
                        )?
                    {
                        let counters =
                            TransformAsyncFunction::create_sequence_counters(async_funcs.len());
                        let ctx = builder.ctx.clone();
                        plan.async_builder = Some(Box::new(move |input, output| {
                            let transform = TransformAsyncFunction::new(
                                ctx.clone(),
                                async_funcs.clone(),
                                BTreeMap::new(),
                                counters.clone(),
                            )?;
                            Ok(ProcessorPtr::create(AsyncTransformer::create(
                                input, output, transform,
                            )))
                        }));

                        if new_default_schema != new_default_schema_no_cast {
                            plan.cast_builder =
                                Some(Box::new(builder.cast_schema_transform_builder(
                                    new_default_schema_no_cast.clone(),
                                    new_default_schema.clone(),
                                )?));
                        }

                        plan.resort_builder =
                            Some(Box::new(builder.fill_and_reorder_transform_builder(
                                table.clone(),
                                new_default_schema.clone(),
                                default_schema.clone(),
                            )));
                    } else {
                        let source_schema = fill_and_reorder.source_schema.clone();
                        plan.resort_builder =
                            Some(Box::new(builder.fill_and_reorder_transform_builder(
                                table.clone(),
                                source_schema.clone(),
                                default_schema.clone(),
                            )));
                    }
                }

                if default_schema != computed_schema {
                    let ctx = builder.ctx.clone();
                    plan.computed_builder = Some(Box::new(move |input, output| {
                        Ok(ProcessorPtr::create(Transformer::create(
                            input,
                            output,
                            TransformAddComputedColumns::try_new(
                                ctx.clone(),
                                default_schema.clone(),
                                computed_schema.clone(),
                            )?,
                        )))
                    }));
                }

                plans.push(plan);
            } else {
                plans.push(ChunkFillPlan::default());
            }
        }

        ChunkFillPlan::materialize_chunk_fill_plans(&mut plans, builder)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FillAndReorder {
    pub source_schema: DataSchemaRef,
    pub target_table_info: TableInfo,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkAppendData {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub target_tables: Vec<SerializableTable>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkAppendData {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(ChunkAppendDataFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(ChunkAppendData {
            meta: self.meta.clone(),
            input,
            target_tables: self.target_tables.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let mut compact_task_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(self.target_tables.len());
        let mut compact_transform_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(self.target_tables.len());
        let mut serialize_block_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(self.target_tables.len());
        let mut eval_cluster_key_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(self.target_tables.len());
        let mut eval_cluster_key_num = 0;
        let mut sort_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(self.target_tables.len());
        let mut sort_num = 0;

        for append_data in self.target_tables.iter() {
            let table = builder.ctx.build_table_by_table_info(
                &append_data.target_table_info,
                &None,
                None,
            )?;
            let block_thresholds = table.get_block_thresholds();
            compact_task_builders.push(Box::new(
                builder.block_compact_task_builder(block_thresholds)?,
            ));
            compact_transform_builders.push(Box::new(builder.block_compact_transform_builder()?));
            let schema: Arc<DataSchema> = DataSchema::from(table.schema()).into();
            let num_input_columns = schema.num_fields();
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            let cluster_stats_gen = fuse_table.get_cluster_stats_gen(
                builder.ctx.clone(),
                0,
                block_thresholds,
                Some(schema),
            )?;
            let operators = cluster_stats_gen.operators.clone();
            if !operators.is_empty() {
                let func_ctx2 = cluster_stats_gen.func_ctx.clone();

                eval_cluster_key_builders.push(Box::new(move |input, output| {
                    Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                        input,
                        output,
                        num_input_columns,
                        func_ctx2.clone(),
                        operators.clone(),
                    )))
                }));
                eval_cluster_key_num += 1;
            } else {
                eval_cluster_key_builders.push(Box::new(builder.dummy_transform_builder()));
            }
            let cluster_keys = &cluster_stats_gen.cluster_key_index;
            if !cluster_keys.is_empty() {
                let sort_desc: Vec<SortColumnDescription> = cluster_keys
                    .iter()
                    .map(|index| SortColumnDescription {
                        offset: *index,
                        asc: true,
                        nulls_first: false,
                    })
                    .collect();
                let sort_desc: Arc<[_]> = sort_desc.into();
                sort_builders.push(Box::new(
                    move |transform_input_port, transform_output_port| {
                        Ok(ProcessorPtr::create(TransformSortPartial::try_create(
                            transform_input_port,
                            transform_output_port,
                            LimitType::None,
                            sort_desc.clone(),
                        )?))
                    },
                ));
                sort_num += 1;
            } else {
                sort_builders.push(Box::new(builder.dummy_transform_builder()));
            }
            serialize_block_builders.push(Box::new(
                builder.with_tid_serialize_block_transform_builder(
                    table,
                    cluster_stats_gen,
                    append_data.table_meta_timestamps,
                )?,
            ));
        }
        builder
            .main_pipeline
            .add_transforms_by_chunk(compact_task_builders)?;

        builder
            .main_pipeline
            .add_transforms_by_chunk(compact_transform_builders)?;

        if eval_cluster_key_num > 0 {
            builder
                .main_pipeline
                .add_transforms_by_chunk(eval_cluster_key_builders)?;
        }

        if sort_num > 0 {
            builder
                .main_pipeline
                .add_transforms_by_chunk(sort_builders)?;
        }

        builder
            .main_pipeline
            .add_transforms_by_chunk(serialize_block_builders)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SerializableTable {
    pub target_catalog_info: Arc<CatalogInfo>,
    pub target_table_info: TableInfo,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkMerge {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub group_ids: Vec<u64>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkMerge {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(ChunkMergeFormatter::create(self))
    }

    fn display_in_profile(&self) -> bool {
        false
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(ChunkMerge {
            meta: self.meta.clone(),
            input,
            group_ids: self.group_ids.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let group_ids = &self.group_ids;
        assert_eq!(builder.main_pipeline.output_len() % group_ids.len(), 0);
        let chunk_size = builder.main_pipeline.output_len() / group_ids.len();
        let mut widths = Vec::with_capacity(group_ids.len());
        let mut last_group_id = group_ids[0];
        let mut width = 1;
        for group_id in group_ids.iter().skip(1) {
            if *group_id == last_group_id {
                width += 1;
            } else {
                widths.push(width * chunk_size);
                last_group_id = *group_id;
                width = 1;
            }
        }
        widths.push(width * chunk_size);
        builder.main_pipeline.resize_partial_one_with_width(widths)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkCommitInsert {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub update_stream_meta: Vec<UpdateStreamMetaReq>,
    pub overwrite: bool,
    pub deduplicated_label: Option<String>,
    pub targets: Vec<SerializableTable>,
}

#[typetag::serde]
impl IPhysicalPlan for ChunkCommitInsert {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
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
        PhysicalPlan::new(ChunkCommitInsert {
            meta: self.meta.clone(),
            input,
            update_stream_meta: self.update_stream_meta.clone(),
            overwrite: self.overwrite,
            deduplicated_label: self.deduplicated_label.clone(),
            targets: self.targets.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let mut table_meta_timestampss = HashMap::new();

        let mut serialize_segment_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(self.targets.len());
        let mut mutation_aggregator_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(self.targets.len());
        let mut tables = HashMap::new();

        for target in &self.targets {
            let table =
                builder
                    .ctx
                    .build_table_by_table_info(&target.target_table_info, &None, None)?;
            let block_thresholds = table.get_block_thresholds();
            serialize_segment_builders.push(Box::new(
                builder.serialize_segment_transform_builder(
                    table.clone(),
                    block_thresholds,
                    target.table_meta_timestamps,
                )?,
            ));
            mutation_aggregator_builders.push(Box::new(
                builder.mutation_aggregator_transform_builder(
                    table.clone(),
                    target.table_meta_timestamps,
                )?,
            ));
            table_meta_timestampss.insert(table.get_id(), target.table_meta_timestamps);
            tables.insert(table.get_id(), table);
        }

        builder
            .main_pipeline
            .add_transforms_by_chunk(serialize_segment_builders)?;
        builder
            .main_pipeline
            .add_transforms_by_chunk(mutation_aggregator_builders)?;
        builder.main_pipeline.try_resize(1)?;

        let catalog = CatalogManager::instance().build_catalog(
            self.targets[0].target_catalog_info.clone(),
            builder.ctx.session_state()?,
        )?;

        builder.main_pipeline.add_sink(|input| {
            Ok(ProcessorPtr::create(AsyncSinker::create(
                input,
                CommitMultiTableInsert::create(
                    tables.clone(),
                    builder.ctx.clone(),
                    self.overwrite,
                    self.update_stream_meta.clone(),
                    self.deduplicated_label.clone(),
                    catalog.clone(),
                    table_meta_timestampss.clone(),
                ),
            )))
        })
    }
}
