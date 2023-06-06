// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

#[derive(Clone, Default)]
pub enum JobMode {
    // only run filtered tasks once
    #[default]
    OneShot = 0,
    // run filtered tasks on given interval
    Interval = 1,
}

#[derive(Default, Clone)]
pub struct JobConfig {
    pub job_name: String,
    pub interval: Option<Duration>,
    pub mode: JobMode,
}

#[derive(Clone)]
pub struct JobSchedulerConfig{
    // Interval for check and run job
    pub poll_interval: Duration,
}