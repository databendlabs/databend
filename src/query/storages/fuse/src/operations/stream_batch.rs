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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::FormatVersion;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::VACUUM2_OBJECT_KEY_PREFIX;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::is_uuid_v7;
use futures::TryStreamExt;
use opendal::EntryMode;

use crate::FUSE_TBL_SNAPSHOT_PREFIX;
use crate::FuseTable;
use crate::io::SnapshotsIO;
use crate::io::TableMetaLocationGenerator;

const V4_SNAPSHOT_SUFFIX: &str = "_v4.mpk";

#[derive(Clone, Debug, PartialEq, Eq)]
struct StreamBatchCandidate {
    snapshot_id: SnapshotId,
    prev_snapshot_id: Option<(SnapshotId, FormatVersion)>,
    row_count: u64,
    location: String,
    format_version: FormatVersion,
}

impl StreamBatchCandidate {
    fn new(snapshot: &TableSnapshot, location: String, format_version: FormatVersion) -> Self {
        Self {
            snapshot_id: snapshot.snapshot_id,
            prev_snapshot_id: snapshot.prev_snapshot_id,
            row_count: snapshot.summary.row_count,
            location,
            format_version,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ObserveResult {
    Continue,
    Selected(StreamBatchCandidate),
    Fallback,
}

struct StreamBatchSelector {
    base_snapshot_id: Option<SnapshotId>,
    base_row_count: u64,
    batch_limit: u64,
    latest_snapshot_id: SnapshotId,
    scanned: HashMap<SnapshotId, StreamBatchCandidate>,
    proven_committed: HashSet<SnapshotId>,
    selected: Option<StreamBatchCandidate>,
}

impl StreamBatchSelector {
    fn new(
        base_snapshot_id: Option<SnapshotId>,
        base_row_count: u64,
        batch_limit: u64,
        latest_snapshot_id: SnapshotId,
    ) -> Self {
        let mut proven_committed = HashSet::new();
        if let Some(base_snapshot_id) = base_snapshot_id {
            proven_committed.insert(base_snapshot_id);
        }

        Self {
            base_snapshot_id,
            base_row_count,
            batch_limit,
            latest_snapshot_id,
            scanned: HashMap::new(),
            proven_committed,
            selected: None,
        }
    }

    fn observe(&mut self, candidate: StreamBatchCandidate) -> ObserveResult {
        let snapshot_id = candidate.snapshot_id;
        let prev_snapshot_id = candidate.prev_snapshot_id;
        self.scanned.insert(snapshot_id, candidate.clone());

        // Every snapshot written by Databend is based on a committed previous snapshot.
        // Thus, seeing a child proves that its previous snapshot is a legal batch boundary,
        // even when the child itself is an orphan left by a failed optimistic commit.
        // This assumes the table has not been FLASHBACKed: flashback can leave committed
        // snapshots on an abandoned branch. Detecting that history is intentionally out of
        // scope for this optimization; keep the setting disabled for tables using flashback.
        if let Some((prev_snapshot_id, _)) = prev_snapshot_id {
            if let Some(previous) = self.scanned.get(&prev_snapshot_id).cloned() {
                let result = self.consider_committed(previous);
                if result != ObserveResult::Continue {
                    return result;
                }
            }
        }

        // The catalog pointer independently proves that the latest snapshot is committed.
        if snapshot_id == self.latest_snapshot_id {
            let result = self.consider_committed(candidate);
            if result != ObserveResult::Continue {
                return result;
            }
            return self
                .selected
                .clone()
                .map_or(ObserveResult::Fallback, ObserveResult::Selected);
        }

        ObserveResult::Continue
    }

    fn consider_committed(&mut self, candidate: StreamBatchCandidate) -> ObserveResult {
        if Some(candidate.snapshot_id) == self.base_snapshot_id
            || !self.proven_committed.insert(candidate.snapshot_id)
        {
            return ObserveResult::Continue;
        }

        // A committed V4 snapshot pointing to an older-format snapshot means the requested
        // interval crosses the UUID-v7 history boundary. Let the linked-list fallback handle it.
        if candidate
            .prev_snapshot_id
            .is_some_and(|(_, version)| version < TableSnapshot::VERSION)
        {
            return ObserveResult::Fallback;
        }

        let change_row_count = candidate.row_count.abs_diff(self.base_row_count);
        if change_row_count <= self.batch_limit {
            self.selected = Some(candidate);
            ObserveResult::Continue
        } else {
            // The limit is a hint. If one commit is already larger than it, select that commit
            // so the stream can still make progress.
            ObserveResult::Selected(self.selected.clone().unwrap_or(candidate))
        }
    }
}

impl FuseTable {
    /// Find a committed V4 snapshot after `base_snapshot` by listing UUID-v7 snapshot keys in
    /// chronological order. Returns `None` when the fast path is unavailable or cannot establish
    /// a boundary; callers should fall back to traversing `prev_snapshot_id` from the latest.
    pub async fn try_find_stream_batch_snapshot_v4(
        &self,
        base_snapshot: Option<&TableSnapshot>,
        batch_limit: u64,
    ) -> Result<Option<(Arc<TableSnapshot>, FormatVersion)>> {
        let Some(latest_location) = self.snapshot_loc() else {
            return Ok(None);
        };
        let latest_version = TableMetaLocationGenerator::snapshot_version(&latest_location);
        if latest_version != TableSnapshot::VERSION {
            return Ok(None);
        }

        let Some(latest_snapshot) = self.read_table_snapshot().await? else {
            return Ok(None);
        };
        if !is_uuid_v7(&latest_snapshot.snapshot_id) {
            return Ok(None);
        }

        let (base_snapshot_id, base_row_count, start_after) = match base_snapshot {
            Some(base) => {
                if base.format_version != TableSnapshot::VERSION || !is_uuid_v7(&base.snapshot_id) {
                    return Ok(None);
                }
                if base.snapshot_id == latest_snapshot.snapshot_id {
                    return Ok(Some((latest_snapshot, latest_version)));
                }
                let location = self
                    .meta_location_generator()
                    .gen_snapshot_location(&base.snapshot_id, TableSnapshot::VERSION)?;
                (Some(base.snapshot_id), base.summary.row_count, location)
            }
            None => {
                let snapshot_prefix = format!(
                    "{}/{}/",
                    self.meta_location_generator().prefix(),
                    FUSE_TBL_SNAPSHOT_PREFIX
                );
                (
                    None,
                    0,
                    format!("{}{}", snapshot_prefix, VACUUM2_OBJECT_KEY_PREFIX),
                )
            }
        };

        let operator = self.get_operator();
        if !operator.info().full_capability().list_with_start_after {
            return Ok(None);
        }

        let snapshot_prefix = format!(
            "{}/{}/",
            self.meta_location_generator().prefix(),
            FUSE_TBL_SNAPSHOT_PREFIX
        );
        let mut lister = operator
            .lister_with(&snapshot_prefix)
            .start_after(&start_after)
            .await?;
        let mut selector = StreamBatchSelector::new(
            base_snapshot_id,
            base_row_count,
            batch_limit,
            latest_snapshot.snapshot_id,
        );

        while let Some(entry) = lister.try_next().await? {
            if entry.metadata().mode() != EntryMode::FILE {
                continue;
            }

            let location = entry.path().to_string();
            if location.as_str() > latest_location.as_str() {
                break;
            }
            if !location.ends_with(V4_SNAPSHOT_SUFFIX) {
                continue;
            }

            let (snapshot, format_version) = if location == latest_location {
                (latest_snapshot.clone(), latest_version)
            } else {
                SnapshotsIO::read_snapshot(location.clone(), operator.clone(), true).await?
            };
            if format_version != TableSnapshot::VERSION || !is_uuid_v7(&snapshot.snapshot_id) {
                return Ok(None);
            }

            let candidate = StreamBatchCandidate::new(&snapshot, location.clone(), format_version);
            match selector.observe(candidate) {
                ObserveResult::Continue => {}
                ObserveResult::Fallback => return Ok(None),
                ObserveResult::Selected(candidate) => {
                    if candidate.snapshot_id == latest_snapshot.snapshot_id {
                        return Ok(Some((latest_snapshot, latest_version)));
                    }
                    let (snapshot, _) =
                        SnapshotsIO::read_snapshot(candidate.location, operator.clone(), true)
                            .await?;
                    return Ok(Some((snapshot, candidate.format_version)));
                }
            }

            if location == latest_location {
                break;
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use uuid::Builder;

    use super::*;

    fn snapshot_id(millis: u64) -> SnapshotId {
        Builder::from_unix_timestamp_millis(millis, &[0; 10]).into_uuid()
    }

    fn candidate(millis: u64, prev_millis: Option<u64>, row_count: u64) -> StreamBatchCandidate {
        let id = snapshot_id(millis);
        StreamBatchCandidate {
            snapshot_id: id,
            prev_snapshot_id: prev_millis.map(|prev| (snapshot_id(prev), TableSnapshot::VERSION)),
            row_count,
            location: id.to_string(),
            format_version: TableSnapshot::VERSION,
        }
    }

    #[test]
    fn test_selects_proven_committed_snapshot_and_ignores_orphan() {
        let base = snapshot_id(1);
        let latest = snapshot_id(5);
        let mut selector = StreamBatchSelector::new(Some(base), 0, 2, latest);

        assert_eq!(
            selector.observe(candidate(2, Some(1), 100)),
            ObserveResult::Continue
        );
        assert_eq!(
            selector.observe(candidate(3, Some(1), 1)),
            ObserveResult::Continue
        );
        assert_eq!(
            selector.observe(candidate(4, Some(3), 2)),
            ObserveResult::Continue
        );
        assert_eq!(
            selector.observe(candidate(5, Some(4), 3)),
            ObserveResult::Selected(candidate(4, Some(3), 2))
        );
    }

    #[test]
    fn test_oversized_first_commit_still_makes_progress() {
        let base = snapshot_id(1);
        let latest = snapshot_id(4);
        let mut selector = StreamBatchSelector::new(Some(base), 0, 1, latest);

        assert_eq!(
            selector.observe(candidate(2, Some(1), 3)),
            ObserveResult::Continue
        );
        assert_eq!(
            selector.observe(candidate(3, Some(2), 4)),
            ObserveResult::Selected(candidate(2, Some(1), 3))
        );
    }

    #[test]
    fn test_mixed_snapshot_history_falls_back() {
        let base = snapshot_id(1);
        let latest = snapshot_id(2);
        let mut selector = StreamBatchSelector::new(Some(base), 0, 1, latest);
        let mut latest_candidate = candidate(2, Some(1), 1);
        latest_candidate.prev_snapshot_id = Some((base, TableSnapshot::VERSION - 1));

        assert_eq!(selector.observe(latest_candidate), ObserveResult::Fallback);
    }

    #[test]
    fn test_catalog_proves_latest_snapshot() {
        let base = snapshot_id(1);
        let latest = snapshot_id(2);
        let mut selector = StreamBatchSelector::new(Some(base), 0, 1, latest);

        assert_eq!(
            selector.observe(candidate(2, Some(1), 1)),
            ObserveResult::Selected(candidate(2, Some(1), 1))
        );
    }

    #[test]
    fn test_stream_without_base_snapshot() {
        let latest = snapshot_id(2);
        let mut selector = StreamBatchSelector::new(None, 0, 1, latest);

        assert_eq!(
            selector.observe(candidate(1, None, 1)),
            ObserveResult::Continue
        );
        assert_eq!(
            selector.observe(candidate(2, Some(1), 2)),
            ObserveResult::Selected(candidate(1, None, 1))
        );
    }
}
