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

//! The pre-refactor caches: one LRU per kind, sized by the legacy options. Everything the legacy
//! backend needs about a kind is registered here, in `LegacyKinds::builtin`.

mod builtin;
mod registry;
mod store;

pub use registry::LegacyKind;
pub use registry::LegacyKinds;
pub use store::LegacyCapacity;
pub use store::LegacyStore;
