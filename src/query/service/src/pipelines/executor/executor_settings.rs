// Copyright 2022 Datafuse Labs.
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

use std::time::Duration;

use common_exception::Result;
use common_settings::Settings;

pub struct ExecutorSettings {
    pub max_execute_time: Duration,
}

impl ExecutorSettings {
    pub fn try_create(settings: &Settings) -> Result<ExecutorSettings> {
        let max_execute_time = settings.get_max_execute_time()?;
        Ok(ExecutorSettings {
            max_execute_time: Duration::from_millis(max_execute_time),
        })
    }
}
