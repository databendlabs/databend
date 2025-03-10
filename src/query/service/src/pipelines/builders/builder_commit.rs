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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::CommitSink as PhysicalCommitSink;
use databend_common_sql::executor::physical_plans::CommitType;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::plans::TruncateMode;
use databend_common_storages_fuse::operations::CommitSink;
use databend_common_storages_fuse::operations::MutationGenerator;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::operations::TransformMergeCommitMeta;
use databend_common_storages_fuse::operations::TruncateGenerator;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_commit_sink(&mut self, plan: &PhysicalCommitSink) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        let table = self.ctx.build_table_by_table_info(&plan.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        self.main_pipeline.try_resize(1)?;
        match &plan.commit_type {
            CommitType::Truncate { mode } => {
                let prev_snapshot_id = match mode {
                    TruncateMode::Delete => None,
                    _ => plan.snapshot.as_ref().map(|snapshot| snapshot.snapshot_id),
                };
                let snapshot_gen = TruncateGenerator::new(mode.clone());
                if matches!(mode, TruncateMode::Delete) {
                    let mutation_status = self.ctx.get_mutation_status();
                    let deleted_rows = plan
                        .snapshot
                        .as_ref()
                        .map_or(0, |snapshot| snapshot.summary.row_count);
                    self.main_pipeline
                        .set_on_finished(move |info: &ExecutionInfo| match &info.res {
                            Ok(_) => {
                                mutation_status.write().deleted_rows = deleted_rows;
                                Ok(())
                            }
                            Err(error_code) => Err(error_code.clone()),
                        });
                }
                self.main_pipeline.add_sink(|input| {
                    CommitSink::try_create(
                        table,
                        self.ctx.clone(),
                        None,
                        plan.update_stream_meta.clone(),
                        snapshot_gen.clone(),
                        input,
                        None,
                        prev_snapshot_id,
                        plan.deduplicated_label.clone(),
                        plan.table_meta_timestamps,
                    )
                })
            }
            CommitType::Mutation { kind, merge_meta } => {
                if *merge_meta {
                    let cluster_key_id = table.cluster_key_id();
                    self.main_pipeline.add_accumulating_transformer(|| {
                        TransformMergeCommitMeta::create(cluster_key_id)
                    });
                } else {
                    self.main_pipeline.add_async_accumulating_transformer(|| {
                        let base_segments = if matches!(
                            kind,
                            MutationKind::Compact | MutationKind::Insert | MutationKind::Recluster
                        ) {
                            vec![]
                        } else {
                            plan.snapshot.segments().to_vec()
                        };

                        // extract re-cluster related mutations from physical plan
                        let recluster_info = plan.recluster_info.clone().unwrap_or_default();

                        TableMutationAggregator::create(
                            table,
                            self.ctx.clone(),
                            base_segments,
                            recluster_info.merged_blocks,
                            recluster_info.removed_segment_indexes,
                            recluster_info.removed_statistics,
                            *kind,
                            plan.table_meta_timestamps,
                        )
                    });
                }

                let snapshot_gen = MutationGenerator::new(plan.snapshot.clone(), *kind);
                self.main_pipeline.add_sink(|input| {
                    CommitSink::try_create(
                        table,
                        self.ctx.clone(),
                        None,
                        plan.update_stream_meta.clone(),
                        snapshot_gen.clone(),
                        input,
                        None,
                        None,
                        plan.deduplicated_label.clone(),
                        plan.table_meta_timestamps,
                    )
                })
            }
        }
    }
}
