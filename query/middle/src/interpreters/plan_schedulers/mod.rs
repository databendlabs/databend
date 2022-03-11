// Copyright 2021 Datafuse Labs.
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

#[allow(clippy::needless_range_loop)]
mod plan_scheduler;
mod plan_scheduler_error;
mod plan_scheduler_insert;
mod plan_scheduler_query;
mod plan_scheduler_rewriter;
mod plan_scheduler_stream;

pub use plan_scheduler::PlanScheduler;
pub use plan_scheduler_error::handle_error;
pub use plan_scheduler_insert::InsertWithPlan;
pub use plan_scheduler_query::schedule_query;
pub use plan_scheduler_rewriter::apply_plan_rewrite;
pub use plan_scheduler_stream::Scheduled;
pub use plan_scheduler_stream::ScheduledStream;
