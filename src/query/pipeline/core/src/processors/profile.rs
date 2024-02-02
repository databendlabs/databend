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

use std::cell::RefCell;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::processors::profiles::ProfileStatisticsName;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PlanProfile {
    pub id: Option<u32>,
    pub name: Option<String>,
    pub parent_id: Option<u32>,
    pub title: Arc<String>,
    pub labels: Arc<Vec<ProfileLabel>>,

    /// The time spent to process in nanoseconds
    pub cpu_time: usize,
    /// The time spent to wait in nanoseconds, usually used to
    /// measure the time spent on waiting for I/O
    pub wait_time: usize,

    pub exchange_rows: usize,
    pub exchange_bytes: usize,

    pub statistics: [usize; std::mem::variant_count::<ProfileStatisticsName>()],
}

impl PlanProfile {
    pub fn create(profile: &Profile) -> PlanProfile {
        PlanProfile {
            id: profile.plan_id,
            name: profile.plan_name.clone(),
            parent_id: profile.plan_parent_id,
            title: profile.title.clone(),
            labels: profile.labels.clone(),
            cpu_time: profile.load_profile(ProfileStatisticsName::CpuTime),
            wait_time: profile.load_profile(ProfileStatisticsName::WaitTime),
            exchange_rows: profile.load_profile(ProfileStatisticsName::ExchangeRows),
            exchange_bytes: profile.load_profile(ProfileStatisticsName::ExchangeBytes),
            statistics: std::array::from_fn(|index| {
                profile.statistics[index].load(Ordering::SeqCst)
            }),
        }
    }

    pub fn accumulate(&mut self, profile: &Profile) {
        for index in 0..std::mem::variant_count::<ProfileStatisticsName>() {
            self.statistics[index] += profile.statistics[index].load(Ordering::SeqCst);
        }

        self.cpu_time += profile.load_profile(ProfileStatisticsName::CpuTime);
        self.wait_time += profile.load_profile(ProfileStatisticsName::WaitTime);
        self.exchange_rows += profile.load_profile(ProfileStatisticsName::ExchangeRows);
        self.exchange_bytes += profile.load_profile(ProfileStatisticsName::ExchangeBytes);
    }

    pub fn merge(&mut self, profile: &PlanProfile) {
        if self.parent_id.is_none() {
            self.parent_id = profile.parent_id;
        }

        for index in 0..std::mem::variant_count::<ProfileStatisticsName>() {
            self.statistics[index] += profile.statistics[index];
        }

        self.cpu_time += profile.cpu_time;
        self.wait_time += profile.wait_time;
        self.exchange_rows += profile.exchange_rows;
        self.exchange_bytes += profile.exchange_bytes;
    }
}

pub struct PlanScopeGuard {
    idx: usize,
    scope_size: Arc<AtomicUsize>,
}

impl PlanScopeGuard {
    pub fn create(scope_size: Arc<AtomicUsize>, idx: usize) -> PlanScopeGuard {
        PlanScopeGuard { idx, scope_size }
    }
}

impl Drop for PlanScopeGuard {
    fn drop(&mut self) {
        if self.scope_size.fetch_sub(1, Ordering::SeqCst) != self.idx + 1
            && !std::thread::panicking()
        {
            panic!("Broken pipeline scope stack.");
        }
    }
}

#[derive(Clone, Debug)]
pub struct PlanScope {
    pub id: u32,
    pub name: String,
    pub parent_id: Option<u32>,
    pub title: Arc<String>,
    pub labels: Arc<Vec<ProfileLabel>>,
}

impl PlanScope {
    pub fn create(
        id: u32,
        name: String,
        title: Arc<String>,
        labels: Arc<Vec<ProfileLabel>>,
    ) -> PlanScope {
        PlanScope {
            id,
            labels,
            title,
            parent_id: None,
            name,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
}

thread_local! {
    static CURRENT_PROFILE: RefCell<Option<Arc<Profile>>> = const { RefCell::new(None) };
}

impl Profile {
    fn create_items() -> [AtomicUsize; std::mem::variant_count::<ProfileStatisticsName>()] {
        std::array::from_fn(|_| AtomicUsize::new(0))
    }
    pub fn create(pid: usize, p_name: String, scope: Option<PlanScope>) -> Profile {
        Profile {
            pid,
            p_name,
            plan_id: scope.as_ref().map(|x| x.id),
            plan_name: scope.as_ref().map(|x| x.name.clone()),
            plan_parent_id: scope.as_ref().and_then(|x| x.parent_id),
            statistics: Self::create_items(),
            labels: scope
                .as_ref()
                .map(|x| x.labels.clone())
                .unwrap_or(Arc::new(vec![])),
            title: scope
                .as_ref()
                .map(|x| x.title.clone())
                .unwrap_or(Arc::new(String::new())),
        }
    }

    pub fn track_profile(profile: &Arc<Profile>) {
        CURRENT_PROFILE.set(Some(profile.clone()))
    }

    pub fn record_usize_profile(name: ProfileStatisticsName, value: usize) {
        CURRENT_PROFILE.with(|x| match x.borrow().as_ref() {
            None => {}
            Some(profile) => {
                profile.statistics[name as usize].fetch_add(value, Ordering::SeqCst);
            }
        });
    }

    pub fn load_profile(&self, name: ProfileStatisticsName) -> usize {
        self.statistics[name as usize].load(Ordering::SeqCst)
    }
}
