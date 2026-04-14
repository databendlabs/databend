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
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::warn;
use opendal::Operator;

use super::MetaReaders;
use crate::statistics::reducers::deduct_statistics_mut;

#[derive(Debug, PartialEq, Eq)]
struct SnapshotSegmentDedupInfo {
    deduped_segments: Vec<Location>,
    duplicated_segments: Vec<(Location, usize)>,
}

pub(crate) async fn normalize_snapshot(
    snapshot: Arc<TableSnapshot>,
    operator: Operator,
) -> Result<Arc<TableSnapshot>> {
    let dedup_info = dedup_snapshot_segments(&snapshot.segments);
    if dedup_info.duplicated_segments.is_empty() {
        return Ok(snapshot);
    }

    let reader = MetaReaders::segment_info_reader(operator, snapshot.schema.clone().into());
    let mut normalized_snapshot = snapshot.as_ref().clone();
    let mut duplicate_refs = 0;

    for (location, duplicate_count) in &dedup_info.duplicated_segments {
        let params = LoadParams {
            location: location.0.clone(),
            len_hint: None,
            ver: location.1,
            put_cache: true,
        };
        let segment = reader.read(&params).await?;
        deduct_duplicated_segment_summary(
            &mut normalized_snapshot.summary,
            &segment.summary,
            *duplicate_count,
        );
        duplicate_refs += *duplicate_count;
    }

    normalized_snapshot.segments = dedup_info.deduped_segments;
    warn!(
        "[FUSE-TABLE] deduplicated {} duplicate segment references while reading snapshot {}, segments {} -> {}",
        duplicate_refs,
        normalized_snapshot.snapshot_id,
        snapshot.segments.len(),
        normalized_snapshot.segments.len()
    );
    Ok(Arc::new(normalized_snapshot))
}

fn dedup_snapshot_segments(segments: &[Location]) -> SnapshotSegmentDedupInfo {
    let mut seen = HashSet::with_capacity(segments.len());
    let mut first_locations = HashMap::with_capacity(segments.len());
    let mut deduped_segments = Vec::with_capacity(segments.len());
    let mut duplicated_segments: HashMap<String, (Location, usize)> = HashMap::new();

    for segment in segments {
        let key = segment.0.clone();
        if seen.insert(key.clone()) {
            first_locations.insert(key, segment.clone());
            deduped_segments.push(segment.clone());
            continue;
        }

        let first_location = first_locations
            .get(&key)
            .cloned()
            .unwrap_or_else(|| segment.clone());
        duplicated_segments
            .entry(key)
            .and_modify(|(_, count)| *count += 1)
            .or_insert((first_location, 1));
    }

    let mut duplicated_segments = duplicated_segments.into_values().collect::<Vec<_>>();
    duplicated_segments.sort_by(|(left, _), (right, _)| left.0.cmp(&right.0));

    SnapshotSegmentDedupInfo {
        deduped_segments,
        duplicated_segments,
    }
}

fn deduct_duplicated_segment_summary(
    summary: &mut Statistics,
    duplicated_segment_summary: &Statistics,
    duplicate_count: usize,
) {
    for _ in 0..duplicate_count {
        deduct_statistics_mut(summary, duplicated_segment_summary);
    }

    deduct_duplicated_additional_stats_meta(
        &mut summary.additional_stats_meta,
        duplicated_segment_summary.additional_stats_meta.as_ref(),
        duplicate_count,
    );
}

fn deduct_duplicated_additional_stats_meta(
    summary_meta: &mut Option<AdditionalStatsMeta>,
    duplicated_meta: Option<&AdditionalStatsMeta>,
    duplicate_count: usize,
) {
    let (Some(summary_meta), Some(duplicated_meta)) = (summary_meta.as_mut(), duplicated_meta)
    else {
        return;
    };

    let duplicate_count = duplicate_count as u64;
    summary_meta.row_count = summary_meta
        .row_count
        .saturating_sub(duplicated_meta.row_count.saturating_mul(duplicate_count));
    summary_meta.unstats_rows = summary_meta
        .unstats_rows
        .saturating_sub(duplicated_meta.unstats_rows.saturating_mul(duplicate_count));
}

#[cfg(test)]
mod tests {
    use databend_storages_common_table_meta::meta::AdditionalStatsMeta;

    use super::*;

    #[test]
    fn test_dedup_snapshot_segments_preserves_first_occurrence_order() {
        let dedup_info = dedup_snapshot_segments(&[
            ("a".to_string(), 1),
            ("b".to_string(), 1),
            ("a".to_string(), 1),
            ("c".to_string(), 1),
            ("b".to_string(), 1),
        ]);

        assert_eq!(dedup_info.deduped_segments, vec![
            ("a".to_string(), 1),
            ("b".to_string(), 1),
            ("c".to_string(), 1),
        ]);
        assert_eq!(dedup_info.duplicated_segments, vec![
            (("a".to_string(), 1), 1),
            (("b".to_string(), 1), 1),
        ]);
    }

    #[test]
    fn test_deduct_duplicated_segment_summary_updates_stats() {
        let mut summary = Statistics {
            row_count: 30,
            block_count: 3,
            perfect_block_count: 3,
            uncompressed_byte_size: 300,
            compressed_byte_size: 150,
            index_size: 60,
            additional_stats_meta: Some(AdditionalStatsMeta {
                row_count: 30,
                unstats_rows: 6,
                ..Default::default()
            }),
            ..Default::default()
        };
        let duplicated_segment_summary = Statistics {
            row_count: 10,
            block_count: 1,
            perfect_block_count: 1,
            uncompressed_byte_size: 100,
            compressed_byte_size: 50,
            index_size: 20,
            additional_stats_meta: Some(AdditionalStatsMeta {
                row_count: 10,
                unstats_rows: 2,
                ..Default::default()
            }),
            ..Default::default()
        };

        deduct_duplicated_segment_summary(&mut summary, &duplicated_segment_summary, 2);

        assert_eq!(summary.row_count, 10);
        assert_eq!(summary.block_count, 1);
        assert_eq!(summary.perfect_block_count, 1);
        assert_eq!(summary.uncompressed_byte_size, 100);
        assert_eq!(summary.compressed_byte_size, 50);
        assert_eq!(summary.index_size, 20);
        let additional_stats_meta = summary.additional_stats_meta.unwrap();
        assert_eq!(additional_stats_meta.row_count, 10);
        assert_eq!(additional_stats_meta.unstats_rows, 2);
    }
}
