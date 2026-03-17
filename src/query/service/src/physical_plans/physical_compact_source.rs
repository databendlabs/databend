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
use std::collections::HashSet;

use databend_common_base::runtime::Runtime;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline::sources::EmptySource;
use databend_common_pipeline::sources::PrefetchAsyncSourcer;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::StreamContext;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::BlockCompactMutator;
use databend_common_storages_fuse::operations::CompactLazyPartInfo;
use databend_common_storages_fuse::operations::CompactTransform;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::physical_plans::CommitSink;
use crate::physical_plans::CommitType;
use crate::physical_plans::Exchange;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CompactSource {
    pub meta: PhysicalPlanMeta,
    pub parts: Partitions,
    pub table_info: TableInfo,
    pub column_ids: HashSet<ColumnId>,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[typetag::serde]
impl IPhysicalPlan for CompactSource {
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
        PhysicalPlan::new(CompactSource {
            meta: self.meta.clone(),
            parts: self.parts.clone(),
            table_info: self.table_info.clone(),
            column_ids: self.column_ids.clone(),
            table_meta_timestamps: self.table_meta_timestamps,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let table = builder
            .ctx
            .build_table_by_table_info(&self.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        if self.parts.is_empty() {
            return builder.main_pipeline.add_source(EmptySource::create, 1);
        }

        let is_lazy = self.parts.partitions_type() == PartInfoType::LazyLevel;
        let thresholds = table.get_block_thresholds();
        let cluster_key_id = table.cluster_key_id();
        let mut max_threads = builder.settings.get_max_threads()? as usize;

        if is_lazy {
            let query_ctx = builder.ctx.clone();
            let dal = table.get_operator();

            let lazy_parts = self
                .parts
                .partitions
                .iter()
                .map(|v| {
                    v.as_any()
                        .downcast_ref::<CompactLazyPartInfo>()
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>();

            let column_ids = self.column_ids.clone();
            builder.main_pipeline.set_on_init(move || {
                let ctx = query_ctx.clone();
                let partitions =
                    Runtime::with_worker_threads(2, Some("build_compact_tasks".to_string()))?
                        .block_on(async move {
                            let partitions = BlockCompactMutator::build_compact_tasks(
                                ctx.clone(),
                                dal.clone(),
                                column_ids.clone(),
                                cluster_key_id,
                                thresholds,
                                lazy_parts,
                            )
                            .await?;

                            Result::<_>::Ok(partitions)
                        })?;

                let partitions = Partitions::create(PartitionsShuffleKind::Mod, partitions);
                query_ctx.set_partitions(partitions)?;
                Ok(())
            });
        } else {
            max_threads = max_threads.min(self.parts.len()).max(1);
            builder.ctx.set_partitions(self.parts.clone())?;
        }

        let block_reader = table.create_block_reader(
            builder.ctx.clone(),
            Projection::Columns(table.all_column_indices()),
            false,
            table.change_tracking_enabled(),
            false,
        )?;
        let stream_ctx = if table.change_tracking_enabled() {
            Some(StreamContext::try_create(
                builder.ctx.get_function_context()?,
                table.schema_with_stream(),
                table.get_table_info().ident.seq,
                false,
                false,
            )?)
        } else {
            None
        };
        // Add source pipe.
        builder.main_pipeline.add_source(
            |output| {
                let source = databend_common_storages_fuse::operations::CompactSource::create(
                    builder.ctx.clone(),
                    block_reader.clone(),
                    1,
                );
                PrefetchAsyncSourcer::create(builder.ctx.get_scan_progress(), output, source)
            },
            max_threads,
        )?;
        let storage_format = table.get_storage_format();
        builder.main_pipeline.add_block_meta_transformer(|| {
            CompactTransform::create(
                builder.ctx.clone(),
                block_reader.clone(),
                storage_format,
                stream_ctx.clone(),
            )
        });

        // sort
        let cluster_stats_gen = table.cluster_gen_for_append(
            builder.ctx.clone(),
            &mut builder.main_pipeline,
            thresholds,
            None,
        )?;
        builder.main_pipeline.add_transform(|input, output| {
            let proc = TransformSerializeBlock::try_create(
                builder.ctx.clone(),
                input,
                output,
                table,
                cluster_stats_gen.clone(),
                MutationKind::Compact,
                self.table_meta_timestamps,
            )?;
            proc.into_processor()
        })?;

        if is_lazy {
            builder.main_pipeline.try_resize(1)?;
            builder
                .main_pipeline
                .add_async_accumulating_transformer(|| {
                    TableMutationAggregator::create(
                        table,
                        builder.ctx.clone(),
                        vec![],
                        vec![],
                        vec![],
                        Default::default(),
                        MutationKind::Compact,
                        self.table_meta_timestamps,
                    )
                });
        }
        Ok(())
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_compact_block(
        &mut self,
        compact_block: &databend_common_sql::plans::OptimizeCompactBlock,
    ) -> Result<PhysicalPlan> {
        let databend_common_sql::plans::OptimizeCompactBlock {
            catalog,
            database,
            table,
            limit,
        } = compact_block;

        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(catalog).await?;
        let tbl = catalog.get_table(&tenant, database, table).await?;
        // check mutability
        tbl.check_mutable()?;

        let table_info = tbl.get_table_info().clone();

        let Some((parts, snapshot)) = tbl.compact_blocks(self.ctx.clone(), limit.clone()).await?
        else {
            return Err(ErrorCode::NoNeedToCompact(format!(
                "No need to do compact for '{database}'.'{table}'"
            )));
        };

        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(tbl.as_ref(), Some(snapshot.clone()))?;

        let merge_meta = parts.partitions_type() == PartInfoType::LazyLevel;
        let mut root: PhysicalPlan = PhysicalPlan::new(CompactSource {
            parts,
            table_info: table_info.clone(),
            column_ids: snapshot.schema.to_leaf_column_id_set(),
            table_meta_timestamps,
            meta: PhysicalPlanMeta::new("ConstantTableScan"),
        });

        let is_distributed = (!self.ctx.get_cluster().is_empty())
            && self.ctx.get_settings().get_enable_distributed_compact()?;
        if is_distributed {
            root = PhysicalPlan::new(Exchange {
                input: root,
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
                meta: PhysicalPlanMeta::new("ConstantTableScan"),
            });
        }

        root = PhysicalPlan::new(CommitSink {
            input: root,
            table_info,
            snapshot: Some(snapshot),
            commit_type: CommitType::Mutation {
                kind: MutationKind::Compact,
                merge_meta,
            },
            update_stream_meta: vec![],
            deduplicated_label: None,
            recluster_info: None,
            table_meta_timestamps,
            meta: PhysicalPlanMeta::new("CommitSink"),
        });

        root.adjust_plan_id(&mut 0);
        Ok(root)
    }
}
