// Copyright 2020-2022 Jorge C. Leitão
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

use super::Index;

/// Sealed trait describing the subset (`i32` and `i64`) of [`Index`] that can be used
/// as offsets of variable-length Arrow arrays.
pub trait Offset: super::private::Sealed + Index {
    /// Whether it is `i32` (false) or `i64` (true).
    const IS_LARGE: bool;
}

impl Offset for i32 {
    const IS_LARGE: bool = false;
}

impl Offset for i64 {
    const IS_LARGE: bool = true;
}
