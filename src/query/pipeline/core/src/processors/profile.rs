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

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_base::runtime::drop_guard;
use databend_common_base::runtime::error_info::NodeErrorType;
use databend_common_base::runtime::metrics::MetricSample;
use databend_common_base::runtime::metrics::ScopedRegistry;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileLabel;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;

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
        drop_guard(move || {
            if self.scope_size.fetch_sub(1, Ordering::SeqCst) != self.idx + 1
                && !std::thread::panicking()
            {
                panic!("Broken pipeline scope stack.");
            }
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ErrorInfoDesc {
    message: String,
    detail: String,
    backtrace: String,
}

impl ErrorInfoDesc {
    pub fn create(error: &ErrorCode) -> ErrorInfoDesc {
        ErrorInfoDesc {
            message: error.message(),
            detail: error.detail(),
            backtrace: error.backtrace_str(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum ErrorInfo {
    Other(ErrorInfoDesc),
    IoError(ErrorInfoDesc),
    ScheduleError(ErrorInfoDesc),
    CalculationError(ErrorInfoDesc),
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PlanProfile {
    pub id: Option<u32>,
    pub name: Option<String>,
    pub parent_id: Option<u32>,
    pub title: Arc<String>,
    pub labels: Arc<Vec<ProfileLabel>>,

    pub statistics: [usize; std::mem::variant_count::<ProfileStatisticsName>()],
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    #[serde(default)]
    pub metrics: BTreeMap<String, Vec<MetricSample>>,

    pub errors: Vec<Arc<ErrorInfo>>,
}

impl PlanProfile {
    fn get_profile_error(profile: &Profile) -> Vec<Arc<ErrorInfo>> {
        let errors = profile.errors.lock();

        let mut errors_info = Vec::with_capacity(errors.len());

        for error in errors.iter() {
            errors_info.push(Arc::new(match error {
                NodeErrorType::ScheduleEventError(e) => {
                    ErrorInfo::ScheduleError(ErrorInfoDesc::create(e))
                }
                NodeErrorType::SyncProcessError(e) => {
                    ErrorInfo::CalculationError(ErrorInfoDesc::create(e))
                }
                NodeErrorType::AsyncProcessError(e) => ErrorInfo::IoError(ErrorInfoDesc::create(e)),
                NodeErrorType::LocalError(e) => ErrorInfo::Other(ErrorInfoDesc::create(e)),
            }));
        }

        errors_info
    }

    pub fn create(profile: &Profile) -> PlanProfile {
        PlanProfile {
            id: profile.plan_id,
            name: profile.plan_name.clone(),
            parent_id: profile.plan_parent_id,
            title: profile.title.clone(),
            labels: profile.labels.clone(),
            metrics: BTreeMap::new(),
            statistics: std::array::from_fn(|_| 0),
            errors: Self::get_profile_error(profile),
        }
    }

    pub fn accumulate(&mut self, profile: &Profile) {
        for index in 0..std::mem::variant_count::<ProfileStatisticsName>() {
            self.statistics[index] += profile.statistics[index].load(Ordering::SeqCst);
        }

        self.errors.extend(Self::get_profile_error(profile));
    }

    pub fn merge(&mut self, profile: &PlanProfile) {
        if self.parent_id.is_none() {
            self.parent_id = profile.parent_id;
        }

        for index in 0..std::mem::variant_count::<ProfileStatisticsName>() {
            self.statistics[index] += profile.statistics[index];
        }

        for errors in &profile.errors {
            self.errors.push(errors.clone());
        }

        for (id, metrics) in &profile.metrics {
            match self.metrics.entry(id.clone()) {
                Entry::Occupied(mut v) => {
                    v.get_mut().extend(metrics.clone());
                }
                Entry::Vacant(v) => {
                    v.insert(metrics.clone());
                }
            }
        }
    }

    pub fn add_metrics(&mut self, node_id: String, metrics: Vec<MetricSample>) {
        if metrics.is_empty() {
            return;
        }

        match self.metrics.entry(node_id) {
            Entry::Vacant(v) => {
                v.insert(metrics);
            }
            Entry::Occupied(mut v) => {
                v.insert(metrics);
            }
        };
    }
}

#[derive(Clone)]
pub struct PlanScope {
    pub id: u32,
    pub name: String,
    pub parent_id: Option<u32>,
    pub title: Arc<String>,
    pub labels: Arc<Vec<ProfileLabel>>,
    pub metrics_registry: Arc<ScopedRegistry>,
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
            metrics_registry: ScopedRegistry::create(None),
        }
    }
}
