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

// exports components as pipeline processors

mod processor_broadcast;
mod processor_replace_into;
mod processor_unbranched_replace_into;
mod transform_merge_into_mutation_aggregator;

pub use processor_broadcast::BroadcastProcessor;
pub use processor_replace_into::ReplaceIntoProcessor;
pub use processor_unbranched_replace_into::UnbranchedReplaceIntoProcessor;
