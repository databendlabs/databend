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

mod entry;
mod kind_store;
mod legacy_capacities;
mod legacy_store;
mod pool_store;
mod shared_store;

pub use kind_store::InsertMode;
pub use kind_store::InsertOutcome;
pub use kind_store::KindStats;
pub use kind_store::KindStore;
pub use kind_store::PoolStats;
pub use kind_store::RejectReason;
pub use kind_store::ShrinkReport;
pub use legacy_capacities::legacy_capacities;
pub use legacy_capacities::legacy_total_bytes;
pub use legacy_store::LegacyCapacity;
pub use legacy_store::LegacyStore;
pub use shared_store::SharedStore;
