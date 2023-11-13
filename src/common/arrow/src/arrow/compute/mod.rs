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

//! contains a wide range of compute operations (e.g.
//! [`arithmetics`], [`aggregate`],
//! [`filter`], [`comparison`], and [`sort`])
//!
//! This module's general design is
//! that each operator has two interfaces, a statically-typed version and a dynamically-typed
//! version.
//! The statically-typed version expects concrete arrays (such as [`PrimitiveArray`](crate::arrow::array::PrimitiveArray));
//! the dynamically-typed version expects `&dyn Array` and errors if the the type is not
//! supported.
//! Some dynamically-typed operators have an auxiliary function, `can_*`, that returns
//! true if the operator can be applied to the particular `DataType`.

#[cfg(any(feature = "compute_aggregate", feature = "io_parquet"))]
#[cfg_attr(docsrs, doc(cfg(feature = "compute_aggregate")))]
pub mod aggregate;
pub mod arity;
#[cfg(feature = "compute_cast")]
#[cfg_attr(docsrs, doc(cfg(feature = "compute_cast")))]
pub mod cast;
#[cfg(feature = "compute_concatenate")]
#[cfg_attr(docsrs, doc(cfg(feature = "compute_concatenate")))]
pub mod concatenate;
#[cfg(feature = "compute_merge_sort")]
#[cfg_attr(docsrs, doc(cfg(feature = "compute_merge_sort")))]
pub mod merge_sort;
#[cfg(feature = "compute_sort")]
#[cfg_attr(docsrs, doc(cfg(feature = "compute_sort")))]
pub mod sort;
#[cfg(feature = "compute_take")]
#[cfg_attr(docsrs, doc(cfg(feature = "compute_take")))]
pub mod take;
mod utils;
