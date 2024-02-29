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

use std::sync::Arc;
use std::time::Duration;

use databend_common_exception::Result;
use databend_common_settings::Settings;

#[derive(Clone)]
pub struct ExecutorSettings {
    pub enable_new_executor: bool,
    pub query_id: Arc<String>,
    pub max_execute_time_in_seconds: Duration,
}

impl ExecutorSettings {
    pub fn try_create(settings: &Settings, query_id: String) -> Result<ExecutorSettings> {
        let max_execute_time_in_seconds = settings.get_max_execute_time_in_seconds()?;
        Ok(ExecutorSettings {
            enable_new_executor: settings.get_enable_experimental_new_executor()?,
            query_id: Arc::new(query_id),
            max_execute_time_in_seconds: Duration::from_secs(max_execute_time_in_seconds),
        })
    }
}
