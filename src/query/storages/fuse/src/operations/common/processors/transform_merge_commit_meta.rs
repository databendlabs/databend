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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::merge_column_hll;
use databend_storages_common_table_meta::meta::merge_column_top_n_mut;

use crate::operations::CommitMeta;
use crate::operations::ConflictResolveContext;
use crate::operations::SnapshotChanges;
use crate::operations::SnapshotMerged;
use crate::statistics::merge_statistics;

pub struct TransformMergeCommitMeta {
    to_merged: Vec<CommitMeta>,
    cluster_key_info: Option<ClusterKeyInfo>,
}

impl TransformMergeCommitMeta {
    pub fn create(cluster_key_info: Option<ClusterKeyInfo>) -> Self {
        TransformMergeCommitMeta {
            to_merged: vec![],
            cluster_key_info,
        }
    }

    fn merge_conflict_resolve_context(
        l: ConflictResolveContext,
        r: ConflictResolveContext,
        cluster_key_info: Option<&ClusterKeyInfo>,
    ) -> Result<ConflictResolveContext> {
        match (l, r) {
            (
                ConflictResolveContext::ModifiedSegmentExistsInLatest(l),
                ConflictResolveContext::ModifiedSegmentExistsInLatest(r),
            ) => {
                assert!(!l.check_intersect(&r));

                Ok(ConflictResolveContext::ModifiedSegmentExistsInLatest(
                    SnapshotChanges {
                        removed_segment_indexes: l
                            .removed_segment_indexes
                            .into_iter()
                            .chain(r.removed_segment_indexes)
                            .collect(),
                        removed_statistics: merge_statistics(
                            l.removed_statistics.clone(),
                            &r.removed_statistics,
                            cluster_key_info,
                        ),
                        appended_segments: l
                            .appended_segments
                            .into_iter()
                            .chain(r.appended_segments)
                            .collect(),
                        replaced_segments: l
                            .replaced_segments
                            .into_iter()
                            .chain(r.replaced_segments)
                            .collect(),
                        merged_statistics: merge_statistics(
                            l.merged_statistics.clone(),
                            &r.merged_statistics,
                            cluster_key_info,
                        ),
                    },
                ))
            }
            (
                ConflictResolveContext::AppendOnly((l, l_schema)),
                ConflictResolveContext::AppendOnly((r, r_schema)),
            ) => {
                if l_schema != r_schema {
                    return Err(ErrorCode::Internal(
                        "append-only commit meta schemas do not match".to_string(),
                    ));
                }
                Ok(ConflictResolveContext::AppendOnly((
                    SnapshotMerged {
                        merged_segments: l
                            .merged_segments
                            .into_iter()
                            .chain(r.merged_segments)
                            .collect(),
                        merged_statistics: merge_statistics(
                            l.merged_statistics.clone(),
                            &r.merged_statistics,
                            cluster_key_info,
                        ),
                    },
                    l_schema,
                )))
            }
            (ConflictResolveContext::None, ctx) | (ctx, ConflictResolveContext::None) => Ok(ctx),
            _ => Err(ErrorCode::Internal(
                "conflict resolve context types do not match".to_string(),
            )),
        }
    }

    pub fn merge_commit_meta(
        l: CommitMeta,
        r: CommitMeta,
        cluster_key_info: Option<&ClusterKeyInfo>,
    ) -> Result<CommitMeta> {
        assert_eq!(l.table_id, r.table_id, "table id mismatch");

        let mut top_n = l.top_n;
        merge_column_top_n_mut(&mut top_n, r.top_n)?;

        Ok(CommitMeta {
            conflict_resolve_context: Self::merge_conflict_resolve_context(
                l.conflict_resolve_context,
                r.conflict_resolve_context,
                cluster_key_info,
            )?,
            new_segment_locs: l
                .new_segment_locs
                .into_iter()
                .chain(r.new_segment_locs)
                .collect(),
            table_id: l.table_id,
            logical_updated_rows: l.logical_updated_rows + r.logical_updated_rows,
            logical_deleted_rows: l.logical_deleted_rows + r.logical_deleted_rows,
            hll: merge_column_hll(l.hll, r.hll),
            top_n,
        })
    }
}

impl AccumulatingTransform for TransformMergeCommitMeta {
    const NAME: &'static str = "TransformMergeCommitMeta";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let commit_meta = CommitMeta::try_from(data)?;
        self.to_merged.push(commit_meta);
        Ok(vec![])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        let to_merged = std::mem::take(&mut self.to_merged);
        if to_merged.is_empty() {
            return Ok(vec![]);
        }
        let mut to_merged = to_merged.into_iter();
        let first = to_merged.next().unwrap();
        let merged = to_merged.try_fold(first, |acc, x| {
            Self::merge_commit_meta(acc, x, self.cluster_key_info.as_ref())
        })?;
        Ok(vec![merged.into()])
    }
}

#[cfg(test)]
mod tests {
    use databend_storages_common_table_meta::meta::Statistics;

    use super::*;

    fn commit_meta_with_rows(added_rows: u64, removed_rows: u64, deleted_rows: u64) -> CommitMeta {
        let mut meta = CommitMeta::empty(1);
        meta.conflict_resolve_context =
            ConflictResolveContext::ModifiedSegmentExistsInLatest(SnapshotChanges {
                merged_statistics: Statistics {
                    row_count: added_rows,
                    ..Default::default()
                },
                removed_statistics: Statistics {
                    row_count: removed_rows,
                    ..Default::default()
                },
                ..Default::default()
            });
        meta.logical_deleted_rows = deleted_rows;
        meta
    }

    #[test]
    fn test_merge_commit_meta_derives_logical_insert_rows() {
        let deleted = commit_meta_with_rows(90, 100, 10);
        let inserted = commit_meta_with_rows(105, 100, 0);

        let merged = TransformMergeCommitMeta::merge_commit_meta(deleted, inserted, None).unwrap();
        assert_eq!(merged.logical_deleted_rows, 10);
        assert_eq!(
            merged
                .conflict_resolve_context
                .logical_insert_rows(merged.logical_deleted_rows),
            5
        );
    }

    #[test]
    fn test_logical_insert_rows_saturates_for_physical_row_reduction() {
        let compacted = commit_meta_with_rows(1, 5, 0);

        assert_eq!(
            compacted
                .conflict_resolve_context
                .logical_insert_rows(compacted.logical_deleted_rows),
            0
        );
    }
}
