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

mod no_prefetch;
mod predicate_and_topk;
mod topk_only;
mod utils;

pub mod policy;
pub use no_prefetch::NoPretchPolicyBuilder;
pub use predicate_and_topk::PredicateAndTopkPolicyBuilder;
pub use topk_only::TopkOnlyPolicyBuilder;
