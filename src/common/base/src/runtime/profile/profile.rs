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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;

use crate::runtime::ThreadTracker;
use crate::runtime::error_info::NodeErrorType;
use crate::runtime::metrics::ScopedRegistry;
use crate::runtime::perf::PerfEvent;
use crate::runtime::perf::PerfValue;
use crate::runtime::profile::ProfileStatisticsName;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProfileLabel {
    pub name: String,
    pub value: Vec<String>,
}

impl ProfileLabel {
    pub fn create(name: String, value: Vec<String>) -> ProfileLabel {
        ProfileLabel { name, value }
    }
}

pub struct Profile {
    /// The id of processor
    pub pid: usize,
    /// The name of processor
    pub p_name: String,

    pub plan_id: Option<u32>,
    pub plan_name: Option<String>,
    pub plan_parent_id: Option<u32>,
    pub labels: Arc<Vec<ProfileLabel>>,
    pub title: Arc<String>,

    pub statistics: [AtomicUsize; std::mem::variant_count::<ProfileStatisticsName>()],
    /// Sum of every value that `drain_statistics` has moved out of `statistics`.
    /// `statistics` is reset to zero on each drain so that incremental reports
    /// (e.g. `system.query_profile`, cross-node statistics exchange) never double count;
    /// `statistics + drained_total` is therefore the processor's lifetime total at any
    /// instant, which is what post-execution per-processor summaries need.
    pub drained_total: [AtomicUsize; std::mem::variant_count::<ProfileStatisticsName>()],
    pub metrics_registry: Option<Arc<ScopedRegistry>>,
    pub errors: Arc<Mutex<Vec<NodeErrorType>>>,
    /// Hardware/software perf counter results, only populated during EXPLAIN PERF.
    pub perf_counters: Mutex<HashMap<PerfEvent, PerfValue>>,
}

impl Clone for Profile {
    fn clone(&self) -> Self {
        Profile {
            pid: self.pid,
            p_name: self.p_name.clone(),
            plan_id: self.plan_id,
            plan_name: self.plan_name.clone(),
            plan_parent_id: self.plan_parent_id,
            labels: self.labels.clone(),
            title: self.title.clone(),
            metrics_registry: self.metrics_registry.clone(),
            statistics: std::array::from_fn(|idx| {
                AtomicUsize::new(self.statistics[idx].load(Ordering::SeqCst))
            }),
            drained_total: std::array::from_fn(|idx| {
                AtomicUsize::new(self.drained_total[idx].load(Ordering::SeqCst))
            }),
            errors: self.errors.clone(),
            perf_counters: Mutex::new(self.perf_counters.lock().clone()),
        }
    }
}

impl Profile {
    fn create_items() -> [AtomicUsize; std::mem::variant_count::<ProfileStatisticsName>()] {
        std::array::from_fn(|_| AtomicUsize::new(0))
    }
    pub fn create(
        pid: usize,
        p_name: String,
        plan_id: Option<u32>,
        plan_name: Option<String>,
        plan_parent_id: Option<u32>,
        title: Arc<String>,
        labels: Arc<Vec<ProfileLabel>>,
        metrics_registry: Option<Arc<ScopedRegistry>>,
    ) -> Profile {
        Profile {
            pid,
            p_name,
            plan_id,
            plan_name,
            plan_parent_id,
            title,
            labels,
            statistics: Self::create_items(),
            drained_total: Self::create_items(),
            metrics_registry,
            errors: Arc::new(Mutex::new(vec![])),
            perf_counters: Mutex::new(HashMap::new()),
        }
    }

    pub fn record_usize_profile(name: ProfileStatisticsName, value: usize) {
        ThreadTracker::with(|x| match x.borrow().payload.profile.as_ref() {
            None => {}
            Some(profile) => {
                profile.statistics[name as usize].fetch_add(value, Ordering::SeqCst);
            }
        });
    }

    /// Record perf counter values into the current thread's profile.
    /// Each entry carries its own per-event multiplexed flag.
    pub fn record_perf_counters(values: Vec<(PerfEvent, u64, bool)>) {
        ThreadTracker::with(|x| match x.borrow().payload.profile.as_ref() {
            None => {}
            Some(profile) => {
                let mut counters = profile.perf_counters.lock();
                for (event, value, mux) in values {
                    let e = counters.entry(event).or_default();
                    e.count += value;
                    e.multiplexed = e.multiplexed || mux;
                }
            }
        });
    }

    pub fn load_profile(&self, name: ProfileStatisticsName) -> usize {
        self.statistics[name as usize].load(Ordering::SeqCst)
    }

    /// Atomically move the current value of `statistics[index]` into `drained_total`
    /// and return it. After this call `statistics[index]` is zero.
    ///
    /// Incremental reporters (plan-level profiles, cross-node exchange) must use this
    /// instead of `fetch_min(0, ..)` directly so the lifetime total is preserved.
    #[inline]
    pub fn drain_statistic(&self, index: usize) -> usize {
        let v = self.statistics[index].fetch_min(0, Ordering::SeqCst);
        if v != 0 {
            self.drained_total[index].fetch_add(v, Ordering::SeqCst);
        }
        v
    }

    /// Lifetime value of a statistic: what has been drained plus what has not.
    #[inline]
    pub fn load_total(&self, name: ProfileStatisticsName) -> usize {
        let i = name as usize;
        self.statistics[i].load(Ordering::SeqCst) + self.drained_total[i].load(Ordering::SeqCst)
    }

    /// A detached copy whose `statistics` hold lifetime totals and whose `drained_total`
    /// is zero. `load_profile` on the result returns the same as `load_total` on `self`.
    pub fn snapshot_total(&self) -> Profile {
        let p = self.clone();
        for i in 0..std::mem::variant_count::<ProfileStatisticsName>() {
            let total = p.statistics[i].load(Ordering::SeqCst)
                + p.drained_total[i].swap(0, Ordering::SeqCst);
            p.statistics[i].store(total, Ordering::SeqCst);
        }
        p
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use super::Profile;
    use crate::runtime::profile::ProfileStatisticsName;

    fn profile() -> Profile {
        Profile::create(
            0,
            "p".to_string(),
            Some(1),
            Some("plan".to_string()),
            None,
            Arc::new(String::new()),
            Arc::new(vec![]),
            None,
        )
    }

    const CPU: usize = ProfileStatisticsName::CpuTime as usize;

    #[test]
    fn drain_moves_value_and_preserves_total() {
        let p = profile();
        p.statistics[CPU].fetch_add(100, Ordering::SeqCst);

        // Drain returns the live value and zeroes it, but the lifetime total is kept.
        assert_eq!(p.drain_statistic(CPU), 100);
        assert_eq!(p.statistics[CPU].load(Ordering::SeqCst), 0);
        assert_eq!(p.load_profile(ProfileStatisticsName::CpuTime), 0);
        assert_eq!(p.load_total(ProfileStatisticsName::CpuTime), 100);

        // A second drain of nothing is a no-op on the total.
        assert_eq!(p.drain_statistic(CPU), 0);
        assert_eq!(p.load_total(ProfileStatisticsName::CpuTime), 100);

        // Accumulate more, drain again: total is the sum across drains.
        p.statistics[CPU].fetch_add(25, Ordering::SeqCst);
        assert_eq!(p.drain_statistic(CPU), 25);
        assert_eq!(p.load_total(ProfileStatisticsName::CpuTime), 125);
    }

    #[test]
    fn snapshot_total_folds_drained_into_statistics() {
        let p = profile();
        p.statistics[CPU].fetch_add(100, Ordering::SeqCst);
        p.drain_statistic(CPU);
        p.statistics[CPU].fetch_add(7, Ordering::SeqCst); // live, not yet drained

        let snap = p.snapshot_total();
        // The snapshot reads the full total through the ordinary accessor...
        assert_eq!(snap.load_profile(ProfileStatisticsName::CpuTime), 107);
        assert_eq!(snap.drained_total[CPU].load(Ordering::SeqCst), 0);
        // ...and taking it does not disturb the live profile.
        assert_eq!(p.load_profile(ProfileStatisticsName::CpuTime), 7);
        assert_eq!(p.load_total(ProfileStatisticsName::CpuTime), 107);
    }

    #[test]
    fn clone_carries_drained_total() {
        let p = profile();
        p.statistics[CPU].fetch_add(42, Ordering::SeqCst);
        p.drain_statistic(CPU);
        let c = p.clone();
        assert_eq!(c.load_total(ProfileStatisticsName::CpuTime), 42);
    }
}
