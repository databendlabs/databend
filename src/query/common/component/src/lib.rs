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

//! Reusable query-side components for simple cross-boundary communication.
//!
//! This crate is intentionally narrow:
//! - keep only self-contained state holders and lightweight coordination logic
//! - target capabilities that need to be shared across query/table context boundaries
//! - do not move session management, catalog/table access, settings, stage handling,
//!   authorization, or other service-like orchestration here
//!
//! In short, this crate hosts simple communication components, not higher-level services.

mod broadcast;
mod copy;
mod fragment;
mod mutation;
mod read_block_thresholds;
mod result_cache;
mod runtime_filter;
mod segment_locations;

pub use broadcast::BroadcastChannel;
pub use broadcast::BroadcastRegistry;
pub use copy::CopyState;
pub use fragment::FragmentId;
pub use mutation::MutationState;
pub use read_block_thresholds::ReadBlockThresholdsState;
pub use result_cache::ResultCacheState;
pub use runtime_filter::RuntimeFilterState;
pub use segment_locations::SegmentLocationsState;
