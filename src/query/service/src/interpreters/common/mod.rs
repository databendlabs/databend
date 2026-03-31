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

mod column;
mod finish_hook;
mod grant;
mod metrics;
#[cfg(feature = "cloud-control")]
mod notification;
mod query_log;
mod stream;
mod table;
#[cfg(all(feature = "cloud-control", feature = "task-support"))]
mod task;
mod util;
#[cfg(feature = "cloud-control")]
mod worker;

mod log;
pub mod table_option_validation;

pub use column::*;
pub use finish_hook::QueryFinishHooks;
pub use grant::validate_grant_object_exists;
pub use log::*;
#[cfg(feature = "cloud-control")]
pub use notification::get_notification_client_config;
pub use query_log::InterpreterQueryLog;
pub use stream::dml_build_update_stream_req;
pub use stream::query_build_update_stream_req;
pub use table::check_referenced_computed_columns;
#[cfg(all(feature = "cloud-control", feature = "task-support"))]
pub use task::get_task_client_config;
#[cfg(all(feature = "cloud-control", feature = "task-support"))]
pub use task::make_schedule_options;
#[cfg(all(feature = "cloud-control", feature = "task-support"))]
pub use task::make_warehouse_options;
pub use util::check_deduplicate_label;
#[cfg(feature = "cloud-control")]
pub use worker::get_worker_client_config;

pub use self::metrics::*;
