// Copyright 2025 RisingWave Labs
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

//! Arrow UDF Runtime

/// Whether the function will be called when some of its arguments are null.
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum CallMode {
    /// The function will be called normally when some of its arguments are null.
    /// It is then the function author's responsibility to check for null values if necessary and respond appropriately.
    #[default]
    CalledOnNullInput,

    /// The function always returns null whenever any of its arguments are null.
    /// If this parameter is specified, the function is not executed when there are null arguments;
    /// instead a null result is assumed automatically.
    ReturnNullOnNullInput,
}

mod into_field;

#[cfg(feature = "javascript-runtime")]
pub mod javascript;
#[cfg(feature = "python-runtime")]
pub mod python;
#[cfg(feature = "remote-runtime")]
pub mod remote;
#[cfg(feature = "wasm-runtime")]
pub mod wasm;
