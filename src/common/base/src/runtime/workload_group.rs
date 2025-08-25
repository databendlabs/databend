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
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::Semaphore;

use crate::runtime::MemStat;

pub const CPU_QUOTA_KEY: &str = "cpu_quota";
pub const MEMORY_QUOTA_KEY: &str = "memory_quota";
pub const QUERY_TIMEOUT_QUOTA_KEY: &str = "query_timeout";
pub const MAX_CONCURRENCY_QUOTA_KEY: &str = "max_concurrency";
pub const QUERY_QUEUED_TIMEOUT_QUOTA_KEY: &str = "query_queued_timeout";
pub const MAX_MEMORY_USAGE_RATIO: &str = "max_memory_usage_ratio";

pub const DEFAULT_MAX_MEMORY_USAGE_RATIO: usize = 25;

#[derive(serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq, Debug)]
pub enum QuotaValue {
    Duration(Duration),
    Percentage(usize),
    Bytes(usize),
    Number(usize),
}

impl Display for QuotaValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaValue::Percentage(v) => write!(f, "{}%", v),
            QuotaValue::Duration(v) => write!(f, "{:?}", v),
            QuotaValue::Bytes(v) => write!(f, "{}", v),
            QuotaValue::Number(v) => write!(f, "{}", v),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Debug)]
pub struct WorkloadGroup {
    pub id: String,
    pub name: String,
    pub quotas: HashMap<String, QuotaValue>,
}

impl WorkloadGroup {
    pub fn get_quota(&self, key: &'static str) -> Option<QuotaValue> {
        self.quotas.get(key).cloned()
    }

    pub fn get_max_memory_usage_ratio(&self) -> usize {
        let Some(QuotaValue::Percentage(v)) = self.quotas.get(MAX_MEMORY_USAGE_RATIO) else {
            return DEFAULT_MAX_MEMORY_USAGE_RATIO;
        };

        if *v == 0 {
            return DEFAULT_MAX_MEMORY_USAGE_RATIO;
        }

        std::cmp::min(*v, 100)
    }
}

pub struct WorkloadGroupResource {
    pub meta: WorkloadGroup,
    pub queue_key: String,
    pub permits: usize,
    pub mutex: Arc<Mutex<()>>,
    pub semaphore: Arc<Semaphore>,
    pub mem_stat: Arc<MemStat>,
    pub max_memory_usage: Arc<AtomicUsize>,
    #[allow(clippy::type_complexity)]
    pub destroy_fn: Option<Box<dyn FnOnce(&str, Option<usize>) + Send + Sync + 'static>>,
}

impl Drop for WorkloadGroupResource {
    fn drop(&mut self) {
        if let Some(destroy_fn) = self.destroy_fn.take() {
            let mut mem_percentage = None;
            if let Some(QuotaValue::Percentage(v)) = self.meta.quotas.get(MEMORY_QUOTA_KEY) {
                mem_percentage = Some(*v);
            }

            destroy_fn(&self.meta.id, mem_percentage);
        }
    }
}
