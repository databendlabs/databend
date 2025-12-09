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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::pipelines::processors::transforms::hash_join::desc::RuntimeFilterDesc;

#[derive(Debug)]
pub struct RuntimeFilterBuildLimit {
    limit: Option<usize>,
    total_rows: AtomicUsize,
    disabled: AtomicBool,
}

impl RuntimeFilterBuildLimit {
    pub fn new(limit: Option<usize>) -> Self {
        let disabled = limit.is_some_and(|value| value == 0);
        Self {
            limit,
            total_rows: AtomicUsize::new(0),
            disabled: AtomicBool::new(disabled),
        }
    }

    pub fn from_descs(
        descs: &[RuntimeFilterDesc],
        selectivity_threshold: u64,
        probe_ratio_threshold: f64,
    ) -> Self {
        let limit = compute_max_build_rows(descs, selectivity_threshold, probe_ratio_threshold);
        Self::new(limit)
    }

    pub fn try_add_rows(&self, rows: usize) -> bool {
        if self.disabled.load(Ordering::Relaxed) {
            return false;
        }
        let Some(limit) = self.limit else {
            self.total_rows.fetch_add(rows, Ordering::Relaxed);
            return true;
        };

        loop {
            let current = self.total_rows.load(Ordering::Relaxed);
            if current >= limit {
                self.disabled.store(true, Ordering::Relaxed);
                return false;
            }
            if current.saturating_add(rows) > limit {
                self.disabled.store(true, Ordering::Relaxed);
                return false;
            }
            let new_total = current + rows;
            if self
                .total_rows
                .compare_exchange(current, new_total, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                if new_total >= limit {
                    self.disabled.store(true, Ordering::Relaxed);
                }
                return true;
            }
        }
    }

    pub fn is_disabled(&self) -> bool {
        self.disabled.load(Ordering::Relaxed)
    }
}

fn compute_max_build_rows(
    descs: &[RuntimeFilterDesc],
    selectivity_threshold: u64,
    probe_ratio_threshold: f64,
) -> Option<usize> {
    descs
        .iter()
        .filter_map(|desc| {
            let selectivity_limit = selectivity_limit(desc, selectivity_threshold);
            let ratio_limit = probe_ratio_limit(desc, probe_ratio_threshold);
            match (selectivity_limit, ratio_limit) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            }
        })
        .min()
}

fn selectivity_limit(desc: &RuntimeFilterDesc, selectivity_threshold: u64) -> Option<usize> {
    let build_table_rows = desc.build_table_rows?;
    if selectivity_threshold == 0 {
        return Some(0);
    }
    let numerator = (selectivity_threshold as u128)
        .saturating_mul(build_table_rows as u128)
        .saturating_sub(1);
    let limit = numerator / 100;
    Some(limit.min(usize::MAX as u128) as usize)
}

fn probe_ratio_limit(desc: &RuntimeFilterDesc, probe_ratio_threshold: f64) -> Option<usize> {
    if probe_ratio_threshold <= 0.0 {
        return None;
    }
    let ratio = probe_ratio_threshold.floor() as u128;
    if ratio == 0 {
        return None;
    }
    let mut min_limit: Option<usize> = None;
    for probe_rows in desc.probe_table_rows.iter().flatten() {
        if *probe_rows == 0 {
            return Some(0);
        }
        let numerator = (*probe_rows as u128).saturating_sub(1);
        let limit = (numerator / ratio).min(usize::MAX as u128) as usize;
        min_limit = Some(match min_limit {
            Some(current) => current.min(limit),
            None => limit,
        });
    }
    min_limit
}
