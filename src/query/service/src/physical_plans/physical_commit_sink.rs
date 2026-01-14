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
use std::sync::Arc;

use databend_common_catalog::plan::ReclusterInfoSideCar;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::plans::TruncateMode;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::MutationGenerator;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::operations::TransformMergeCommitMeta;
use databend_common_storages_fuse::operations::TruncateGenerator;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;

use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

// serde is required by `PhysicalPlan`
/// The commit sink is used to commit the data to the table.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CommitSink {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub snapshot: Option<Arc<TableSnapshot>>,
    pub table_info: TableInfo,
    pub commit_type: CommitType,
    pub update_stream_meta: Vec<UpdateStreamMetaReq>,
    pub deduplicated_label: Option<String>,
    pub table_meta_timestamps: TableMetaTimestamps,

    // Used for recluster.
    pub recluster_info: Option<ReclusterInfoSideCar>,
}

#[typetag::serde]
impl IPhysicalPlan for CommitSink {
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

        PhysicalPlan::new(CommitSink {
            input: children.remove(0),
            meta: self.meta.clone(),
            snapshot: self.snapshot.clone(),
            table_info: self.table_info.clone(),
            commit_type: self.commit_type.clone(),
            update_stream_meta: self.update_stream_meta.clone(),
            deduplicated_label: self.deduplicated_label.clone(),
            table_meta_timestamps: self.table_meta_timestamps,
            recluster_info: self.recluster_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let table = builder
            .ctx
            .build_table_by_table_info(&self.table_info, &None, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        builder.main_pipeline.try_resize(1)?;
        match &self.commit_type {
            CommitType::Truncate { mode } => {
                let prev_snapshot_id = match mode {
                    TruncateMode::Delete => None,
                    _ => self.snapshot.as_ref().map(|snapshot| snapshot.snapshot_id),
                };
                let snapshot_gen = TruncateGenerator::new(mode.clone());
                if matches!(mode, TruncateMode::Delete) {
                    let mutation_status = builder.ctx.get_mutation_status();
                    let deleted_rows = self
                        .snapshot
                        .as_ref()
                        .map_or(0, |snapshot| snapshot.summary.row_count);
                    builder
                        .main_pipeline
                        .set_on_finished(move |info: &ExecutionInfo| match &info.res {
                            Ok(_) => {
                                mutation_status.write().deleted_rows = deleted_rows;
                                Ok(())
                            }
                            Err(error_code) => Err(error_code.clone()),
                        });
                }
                builder.main_pipeline.add_sink(|input| {
                    databend_common_storages_fuse::operations::CommitSink::try_create(
                        table,
                        builder.ctx.clone(),
                        None,
                        self.update_stream_meta.clone(),
                        snapshot_gen.clone(),
                        input,
                        None,
                        prev_snapshot_id,
                        self.deduplicated_label.clone(),
                        self.table_meta_timestamps,
                    )
                })
            }
            CommitType::Mutation { kind, merge_meta } => {
                if *merge_meta {
                    let cluster_key_id = table.cluster_key_id();
                    builder.main_pipeline.add_accumulating_transformer(|| {
                        TransformMergeCommitMeta::create(cluster_key_id)
                    });
                } else {
                    builder
                        .main_pipeline
                        .add_async_accumulating_transformer(|| {
                            let base_segments = if matches!(
                                kind,
                                MutationKind::Compact
                                    | MutationKind::Insert
                                    | MutationKind::Recluster
                            ) {
                                vec![]
                            } else {
                                self.snapshot.segments().to_vec()
                            };

                            // extract re-cluster related mutations from physical plan
                            let recluster_info = self.recluster_info.clone().unwrap_or_default();

                            let extended_merged_blocks = recluster_info
                                .merged_blocks
                                .into_iter()
                                .map(|(block_meta, column_hlls)| {
                                    Arc::new(ExtendedBlockMeta {
                                        block_meta: Arc::unwrap_or_clone(block_meta),
                                        draft_virtual_block_meta: None,
                                        column_hlls: column_hlls.map(BlockHLLState::Serialized),
                                    })
                                })
                                .collect::<Vec<Arc<ExtendedBlockMeta>>>();

                            TableMutationAggregator::create(
                                table,
                                builder.ctx.clone(),
                                base_segments,
                                extended_merged_blocks,
                                recluster_info.removed_segment_indexes,
                                recluster_info.removed_statistics,
                                *kind,
                                self.table_meta_timestamps,
                            )
                        });
                }

                let snapshot_gen = MutationGenerator::new(self.snapshot.clone(), *kind);
                builder.main_pipeline.add_sink(|input| {
                    databend_common_storages_fuse::operations::CommitSink::try_create(
                        table,
                        builder.ctx.clone(),
                        None,
                        self.update_stream_meta.clone(),
                        snapshot_gen.clone(),
                        input,
                        None,
                        None,
                        self.deduplicated_label.clone(),
                        self.table_meta_timestamps,
                    )
                })
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum CommitType {
    Truncate {
        mode: TruncateMode,
    },
    Mutation {
        kind: MutationKind,
        merge_meta: bool,
    },
}
