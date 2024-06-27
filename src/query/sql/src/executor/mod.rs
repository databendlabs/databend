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

mod explain;
mod format;
mod physical_plan;
mod physical_plan_builder;
mod physical_plan_display;
mod physical_plan_visitor;
pub mod physical_plans;
mod util;

pub mod table_read_plan;

pub use physical_plan::PhysicalPlan;
pub use physical_plan_builder::MergeIntoBuildInfo;
pub use physical_plan_builder::PhysicalPlanBuilder;
pub use physical_plan_visitor::PhysicalPlanReplacer;
pub use util::*;
