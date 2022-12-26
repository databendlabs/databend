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

//! Defines meta-service metric.
//!
//! The metric key is built in form of `<namespace>_<sub_system>_<field>`.
//!
//! The `namespace` is `fuse`.

use metrics::{counter, decrement_gauge};
use metrics::increment_gauge;

macro_rules! key {
    ($key: literal) => {
        concat!("pipeline_", $key)
    };
}

pub fn metrics_inc_global_schedule_sync_tasks(size: f64) {
    increment_gauge!(key!("global_schedule_sync_tasks"), size);
}

pub fn metrics_inc_wait_schedule_sync_tasks(size: f64) {
    increment_gauge!(key!("wait_schedule_sync_tasks"), size);
}

pub fn metrics_dec_wait_schedule_sync_tasks() {
    decrement_gauge!(key!("wait_schedule_sync_tasks"), 1 as f64);
}

pub fn metrics_inc_global_schedule_async_tasks() {
    counter!(key!("global_schedule_async_tasks"), 1);
}

pub fn metrics_inc_wait_schedule_async_tasks() {
    increment_gauge!(key!("wait_schedule_async_tasks"), 1 as f64);
}

pub fn metrics_dec_wait_schedule_async_tasks() {
    decrement_gauge!(key!("wait_schedule_async_tasks"), 1 as f64);
}

pub fn metrics_inc_schedule_sync_task_time(size: f64) {
    increment_gauge!(key!("schedule_sync_tasks_time"), size);
}

pub fn metrics_inc_schedule_async_task_time(size: f64) {
    increment_gauge!(key!("schedule_async_tasks_time"), size);
}
