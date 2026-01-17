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

#![allow(clippy::collapsible_if, clippy::uninlined_format_args)]

mod encoding_rules;
pub mod memory;
mod parquet_rs;

pub use encoding_rules::DeltaOrderingStats;
pub use encoding_rules::delta_ordering::collect_delta_ordering_stats;
pub use encoding_rules::page_limit::MAX_BATCH_MEMORY_SIZE;
pub use encoding_rules::page_limit::write_batch_with_page_limit;
pub use parquet_rs::*;
