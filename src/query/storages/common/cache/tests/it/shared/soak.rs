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

use super::soak_engine::Phases;
use super::soak_engine::SoakConfig;
use super::soak_engine::soak;

/// Twenty phases of churn on every pool, checked while it runs and between phases. The long
/// version is `tests/soak.rs`.
#[test]
fn test_churn_keeps_memory_bounded_and_accounting_exact() {
    for seed in 0..2 {
        let reports = soak(
            SoakConfig {
                threads: 8,
                phases: Phases::Count(20),
                ops_per_phase: 4000,
                cap: 4 << 20,
                keys: 400,
                seed,
            },
            |_| {},
        );
        assert_eq!(reports.len(), 20);
        assert!(reports.iter().all(|r| r.monitor_samples > 0));
    }
}
