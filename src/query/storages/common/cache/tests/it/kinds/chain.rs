// Copyright 2026 Datafuse Labs.
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

use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::Unit;
use databend_storages_common_cache::kinds::inverted;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegment;
use tempfile::TempDir;

use super::case::cases;
use super::samples::Accuracy;
use super::samples::Sample;
use super::setup::Backend;
use super::setup::manager;
use super::setup::with_disk;

const MEMORY: u64 = 1 << 30;

/// A new kind needs a sample value here to be covered by these tests.
#[test]
fn test_cases_cover_every_kind() {
    let covered: Vec<_> = cases().iter().map(|c| c.kind()).collect();
    assert_eq!(covered, CacheKind::all().collect::<Vec<_>>());
}

#[test]
fn test_memory_round_trip() {
    for backend in Backend::ALL {
        let manager = manager(&backend.config(MEMORY), MEMORY, false);
        for case in cases() {
            let legacy = backend == Backend::Legacy;
            case.memory_round_trip(&manager, legacy);
        }
    }
}

#[test]
fn test_handles_share_loads() {
    for backend in Backend::ALL {
        let manager = manager(&backend.config(MEMORY), MEMORY, false);
        for case in cases() {
            case.shared_loads(&manager);
        }
    }
}

#[test]
fn test_disk_round_trip() {
    for backend in Backend::ALL {
        let dir = TempDir::new().unwrap();
        let manager = manager(&with_disk(backend.config(MEMORY), &dir), MEMORY, true);
        for case in cases() {
            case.disk_round_trip(&manager);
        }
    }
}

/// The backends size their caches by `mem_bytes`; it follows the bytes a value holds.
#[test]
fn test_mem_bytes_follow_allocations() {
    let mut off = Vec::new();
    for case in cases() {
        let (ratio, accuracy) = case.mem_bytes_ratio();
        let expected = match accuracy {
            Accuracy::Measured => 0.75..=1.1,
            Accuracy::Estimated => 0.1..=10.0,
            Accuracy::Unchecked => continue,
        };
        if !expected.contains(&ratio) {
            off.push(format!("{:?}: {ratio:.2}", case.kind()));
        }
    }
    assert!(off.is_empty(), "mem_bytes / allocated: {off:#?}");
}

/// The legacy backend charges what the pre-refactor caches did: nothing for kinds bounded by an
/// item count (but inverted index metas), and the old byte estimates otherwise.
#[test]
fn test_legacy_charges_the_pre_refactor_estimate() {
    let manager = manager(&Backend::Legacy.config(MEMORY), MEMORY, false);
    let units: HashMap<_, _> = manager
        .rows()
        .into_iter()
        .filter_map(|row| Some((row.kind?, row.unit)))
        .collect();
    // The one kind bounded by count whose values were sized.
    let inverted_meta = CacheKind::of::<inverted::Meta>();
    for case in cases() {
        if units[&case.kind()] == Unit::Count && case.kind() != inverted_meta {
            assert_eq!(case.legacy_bytes(), 0, "{:?}", case.kind());
        }
    }
    let meta = inverted::Meta::sample(1);
    assert_eq!(meta.legacy_bytes(), meta.memory_size());

    let compact = segment::Info::sample(1);
    assert_eq!(
        compact.legacy_bytes(),
        size_of::<CompactSegmentInfo>() + compact.raw_block_metas.bytes.len()
    );
    let column_oriented = segment::ColumnOriented::sample(1);
    assert_eq!(
        column_oriented.legacy_bytes(),
        size_of::<ColumnOrientedSegment>() + column_oriented.block_metas.memory_size()
    );
}
