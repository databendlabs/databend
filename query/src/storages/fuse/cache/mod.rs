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

mod local_cache;
mod metrics;

pub use local_cache::empty_with_capacity;
pub use local_cache::CachedLoader;
pub use local_cache::HasMetricLabel;
pub use local_cache::Loader;
pub use local_cache::MemoryCache;
