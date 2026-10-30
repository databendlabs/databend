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

use databend_storages_common_cache::CacheKind;
use tempfile::TempDir;

use super::case::cases;
use super::samples::Accuracy;
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
            case.memory_round_trip(&manager, backend.admit_after(case.kind()));
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
    for case in cases() {
        let (ratio, accuracy) = case.mem_bytes_ratio();
        let expected = match accuracy {
            Accuracy::Measured => 0.75..=1.1,
            Accuracy::Estimated => 0.1..=10.0,
        };
        assert!(
            expected.contains(&ratio),
            "{:?}: mem_bytes / allocated = {ratio:.2}",
            case.kind()
        );
    }
}
