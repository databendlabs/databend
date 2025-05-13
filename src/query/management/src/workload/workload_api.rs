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
use std::time::Duration;

use databend_common_exception::Result;

pub const CPU_QUOTA_KEY: &str = "cpu_quota";
pub const MEMORY_QUOTA_KEY: &str = "memory_quota";
pub const QUERY_TIMEOUT_QUOTA_KEY: &str = "query_timeout";
pub const MAX_CONCURRENCY_QUOTA_KEY: &str = "max_concurrency";
pub const QUERY_QUEUED_TIMEOUT_QUOTA_KEY: &str = "query_queued_timeout";

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
}

#[async_trait::async_trait]
pub trait WorkloadApi: Sync + Send {
    async fn create(&self, group: WorkloadGroup) -> Result<WorkloadGroup>;

    async fn drop(&self, name: String) -> Result<()>;

    async fn rename(&self, old_name: String, new_name: String) -> Result<()>;

    async fn alter_quotas(&self, name: String, quotas: HashMap<String, QuotaValue>) -> Result<()>;

    async fn get_all(&self) -> Result<Vec<WorkloadGroup>>;

    async fn get_by_id(&self, id: &str) -> Result<WorkloadGroup>;

    async fn get_by_name(&self, name: &str) -> Result<WorkloadGroup>;
}
