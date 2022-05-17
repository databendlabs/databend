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

/// Config mods provide config support.
///
/// We are providing two config types:
///
/// - [`inner::Config`] which will be exposed as [`crate::Config`] will be used in all business logic.
/// - [`outer_v0::Config`] is the outer config for [`inner::Config`] which will be exposed to end-users.
///
/// It's safe to refactor [`inner::Config`] in anyway, as long as it satisfied the following traits
///
/// - `TryInto<inner::Config> for outer_v0::Config`
/// - `From<inner::Config> for outer_v0::Config`
mod inner;
mod outer_v0;

pub use inner::Config;
