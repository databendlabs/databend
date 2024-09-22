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

mod builder;
mod column_stat;

mod enforcer;
mod histogram;
#[allow(clippy::module_inception)]
mod property;
mod selectivity;

pub use builder::RelExpr;
pub use column_stat::ColumnStat;
pub use column_stat::ColumnStatSet;
pub use column_stat::NewStatistic;
pub use enforcer::require_property;
pub use enforcer::DistributionEnforcer;
pub use enforcer::Enforcer;
pub use histogram::histogram_from_ndv;
pub use histogram::UniformSampleSet;
pub use property::ColumnSet;
pub use property::Distribution;
pub use property::PhysicalProperty;
pub use property::RelationalProperty;
pub use property::RequiredProperty;
pub use property::StatInfo;
pub use property::Statistics;
pub use property::TableSet;
pub use selectivity::SelectivityEstimator;
pub use selectivity::DEFAULT_SELECTIVITY;
pub use selectivity::MAX_SELECTIVITY;
