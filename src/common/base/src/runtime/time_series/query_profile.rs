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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use log::info;
use serde::Serialize;

use crate::runtime::ThreadTracker;
use crate::runtime::TimeSeriesProfileDesc;
use crate::runtime::get_time_series_profile_desc;
use crate::runtime::time_series::profile::TimeSeriesProfileName;
use crate::runtime::time_series::profile::TimeSeriesProfiles;

const DEFAULT_BATCH_SIZE: usize = 1024;

pub struct QueryTimeSeriesProfile {
    pub global_count: AtomicUsize,
    pub plans_profiles: Vec<(u32, Arc<TimeSeriesProfiles>)>,
    pub query_id: String,
}

impl QueryTimeSeriesProfile {
    pub fn record_time_series_profile(name: TimeSeriesProfileName, value: usize) {
        ThreadTracker::with(
            |x| match x.borrow().payload.local_time_series_profile.as_ref() {
                None => {}
                Some(profile) => {
                    if profile.record(name, value) {
                        if let Some(global_profile) =
                            x.borrow().payload.time_series_profile.as_ref()
                        {
                            let should_flush = Self::should_flush(&global_profile.global_count);
                            if should_flush {
                                global_profile.flush(false);
                            }
                        }
                    }
                }
            },
        )
    }

    pub fn should_flush(global_count: &AtomicUsize) -> bool {
        let mut prev = 0;
        loop {
            let next = if prev == DEFAULT_BATCH_SIZE - 1 {
                0
            } else {
                prev + 1
            };
            match global_count.compare_exchange_weak(prev, next, SeqCst, SeqCst) {
                Ok(_) => {
                    if next == 0 {
                        return true;
                    }
                    return false;
                }
                Err(next_prev) => {
                    prev = next_prev;
                }
            }
        }
    }

    pub fn flush(&self, finish: bool) {
        #[derive(Serialize)]
        struct QueryTimeSeries {
            query_id: String,
            plans: Vec<PlanTimeSeries>,
            desc: Arc<Vec<TimeSeriesProfileDesc>>,
        }
        #[derive(Serialize)]
        struct PlanTimeSeries {
            plan_id: u32,
            data: Vec<Vec<Vec<usize>>>,
        }
        let mut quota = DEFAULT_BATCH_SIZE as i32;
        let mut plans = Vec::with_capacity(self.plans_profiles.len());
        for (plan_id, plan_profile) in self.plans_profiles.iter() {
            if quota == 0 && !finish {
                break;
            }
            let profile_time_series_vec = plan_profile.flush(finish, &mut quota);
            plans.push(PlanTimeSeries {
                plan_id: *plan_id,
                data: profile_time_series_vec,
            });
        }
        if quota == DEFAULT_BATCH_SIZE as i32 {
            return;
        }
        let query_time_series = QueryTimeSeries {
            query_id: self.query_id.clone(),
            plans,
            desc: get_time_series_profile_desc(),
        };
        let json = serde_json::to_string(&query_time_series).unwrap();
        info!(target: "databend::log::time_series", "{}", json);
    }
}

impl Drop for QueryTimeSeriesProfile {
    fn drop(&mut self) {
        self.flush(true);
    }
}

pub struct QueryTimeSeriesProfileBuilder {
    plans_profile: BTreeMap<u32, Arc<TimeSeriesProfiles>>,
    query_id: String,
}

impl QueryTimeSeriesProfileBuilder {
    pub fn new(query_id: String) -> Self {
        QueryTimeSeriesProfileBuilder {
            plans_profile: BTreeMap::new(),
            query_id,
        }
    }

    pub fn register_time_series_profile(&mut self, plan_id: u32) -> Arc<TimeSeriesProfiles> {
        if !self.plans_profile.contains_key(&plan_id) {
            let profile = Arc::new(TimeSeriesProfiles::new());
            self.plans_profile.insert(plan_id, profile.clone());
            profile
        } else {
            self.plans_profile.get(&plan_id).unwrap().clone()
        }
    }

    pub fn build(&self) -> QueryTimeSeriesProfile {
        QueryTimeSeriesProfile {
            global_count: AtomicUsize::new(0),
            plans_profiles: self
                .plans_profile
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect(),
            query_id: self.query_id.clone(),
        }
    }
}
