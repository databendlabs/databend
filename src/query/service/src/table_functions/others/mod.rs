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

mod execute_background_job;
mod license_info;
mod suggested_background_compaction_tasks;
mod suggested_background_tasks;
mod task_dependents;
mod task_dependents_enable;
mod tenant_quota;

pub use execute_background_job::ExecuteBackgroundJobTable;
pub use license_info::LicenseInfoTable;
pub use suggested_background_tasks::SuggestedBackgroundTasksSource;
pub use suggested_background_tasks::SuggestedBackgroundTasksTable;
pub use task_dependents::TaskDependentsTable;
pub use task_dependents_enable::TaskDependentsEnableTable;
pub use tenant_quota::TenantQuotaTable;
