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

mod builtin;
mod cache_def;
mod cache_registry;
mod cached_item;
mod pool;

pub use cache_def::CacheDef;
pub use cache_registry::CacheKind;
pub use cache_registry::CacheRegistry;
pub use cache_registry::KindInfo;
pub use cached_item::CachedItem;
pub use pool::Pool;
