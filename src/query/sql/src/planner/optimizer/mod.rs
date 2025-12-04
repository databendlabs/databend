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

databend_common_tracing::register_module_tag!("[OPTIMIZER]");

mod cost;
pub mod ir;
#[allow(clippy::module_inception)]
mod optimizer;
mod optimizer_api;
mod optimizer_context;
pub mod optimizers;
pub mod pipeline;
mod statistics;

pub use optimizer::optimize;
pub use optimizer::optimize_query;
pub use optimizer_api::Optimizer;
pub use optimizer_context::OptimizerContext;
