// Copyright 2020 Datafuse Labs.
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

#[cfg(test)]
mod optimizer_constant_folding_test;
#[cfg(test)]
mod optimizer_projection_push_down_test;
#[cfg(test)]
mod optimizer_scatters_test;
#[cfg(test)]
mod optimizer_statistics_exact_test;
#[cfg(test)]
mod optimizer_test;

mod optimizer;
mod metrics;
mod optimizer_constant_folding;
mod optimizer_projection_push_down;
mod optimizer_scatters;
mod optimizer_statistics_exact;

pub use optimizer::Optimizer;
pub use optimizer::Optimizers;
pub use optimizer_constant_folding::ConstantFoldingOptimizer;
pub use optimizer_projection_push_down::ProjectionPushDownOptimizer;
pub use optimizer_scatters::ScattersOptimizer;
pub use optimizer_statistics_exact::StatisticsExactOptimizer;
