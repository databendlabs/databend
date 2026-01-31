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

mod backtrace;
mod catch_unwind;
mod defer;
pub mod error_info;
mod executor_stats;
mod global_runtime;
mod memory;
pub mod metrics;
mod perf;
pub mod profile;
#[allow(clippy::module_inception)]
mod runtime;
mod runtime_tracker;
mod thread;
mod time_series;
mod trace;
pub mod workload_group;

pub use backtrace::AsyncTaskItem;
pub use backtrace::dump_backtrace;
pub use backtrace::get_all_tasks;
pub use catch_unwind::CatchUnwindFuture;
pub use catch_unwind::catch_unwind;
pub use catch_unwind::drop_guard;
pub use defer::defer;
pub use executor_stats::ExecutorStats;
pub use executor_stats::ExecutorStatsSlot;
pub use executor_stats::ExecutorStatsSnapshot;
pub use global_runtime::GlobalIORuntime;
pub use global_runtime::GlobalQueryRuntime;
pub use memory::GLOBAL_MEM_STAT;
pub use memory::GLOBAL_QUERIES_MANAGER;
pub use memory::GlobalStatBuffer;
pub use memory::MemStat;
pub use memory::MemStatBuffer;
pub use memory::OutOfLimit;
pub use memory::ParentMemStat;
pub use memory::set_alloc_error_hook;
pub use perf::QueryPerf;
pub use perf::QueryPerfGuard;
pub use runtime::Dropper;
pub use runtime::GLOBAL_TASK;
pub use runtime::Runtime;
pub use runtime::block_on;
pub use runtime::execute_futures_in_parallel;
pub use runtime::spawn;
pub use runtime::spawn_blocking;
pub use runtime::spawn_local;
pub use runtime::spawn_named;
pub use runtime::try_block_on;
pub use runtime::try_spawn_blocking;
pub use runtime_tracker::CaptureLogSettings;
pub use runtime_tracker::LimitMemGuard;
pub use runtime_tracker::ThreadTracker;
pub use runtime_tracker::TrackingGuard;
pub use runtime_tracker::TrackingPayload;
pub use runtime_tracker::TrackingPayloadExt;
pub use runtime_tracker::UnlimitedFuture;
pub use thread::Thread;
pub use thread::ThreadJoinHandle;
pub use time_series::ProfilePoints;
pub use time_series::QueryTimeSeriesProfile;
pub use time_series::QueryTimeSeriesProfileBuilder;
pub use time_series::TimeSeriesProfileDesc;
pub use time_series::TimeSeriesProfileName;
pub use time_series::TimeSeriesProfiles;
pub use time_series::compress_time_point;
pub use time_series::get_time_series_profile_desc;
pub use tokio::task::JoinHandle;
pub use trace::QueryTrace;
pub use trace::TraceCollector;
