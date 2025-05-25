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

use databend_common_base::runtime::workload_group::QuotaValue;
use databend_common_base::runtime::workload_group::WorkloadGroup;
use databend_common_exception::Result;

#[async_trait::async_trait]
pub trait WorkloadApi: Sync + Send {
    async fn create(&self, group: WorkloadGroup) -> Result<WorkloadGroup>;

    async fn drop(&self, name: String) -> Result<()>;

    async fn rename(&self, old_name: String, new_name: String) -> Result<()>;

    async fn set_quotas(&self, name: String, quotas: HashMap<String, QuotaValue>) -> Result<()>;

    async fn unset_quotas(&self, name: String, quotas: Vec<String>) -> Result<()>;

    async fn get_all(&self) -> Result<Vec<WorkloadGroup>>;

    async fn get_by_id(&self, id: &str) -> Result<WorkloadGroup>;

    async fn get_by_name(&self, name: &str) -> Result<WorkloadGroup>;
}
