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

//! Provides backward compatibility.
//!
//! Changed meta data types is loaded into a type that is compatible with all old versions.
//! And then reduce the compatible version to the latest version.
//!
//! To guarantee compatibility:
//! - Field type must not change: Changing `x: String` to `x: i64` makes compatibility impassible to achieve.
//! - Only add or remove fields.
//!
//! E.g. `A` is defined as:
//! ```ignore
//! #[derive(Serialize, Deserialize)]
//! struct A {
//!     i: u64,
//! }
//! ```
//!
//! An upgrade may introduce another field `j`, and remove `i`.
//! The upgraded message `B` will be:
//!```ignore
//! #[derive(Serialize)]
//! struct Foo {
//!     j: u64,
//! }
//!```
//!
//! To be compatible with `A` and `B`, the max compatible `C` should be:
//!```ignore
//! #[derive(Serialize, Deserialize)]
//! struct Compatible {
//!     i: Option<u64>,
//!     j: Option<u64>,
//! }
//!```
//!
//! This way `Compatible` is able to load both `{"i": 1}` or `{"j": 2}`.
//! The complete example is:
//! ```ignore
//! #[derive(Debug, Serialize, Deserialize)]
//! struct A {
//!     pub i: u64,
//! }
//!
//! #[derive(Debug, Serialize)]
//! struct B {
//!     pub j: u64,
//! }
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Compatible {
//!     pub i: Option<u64>,
//!     pub j: Option<u64>,
//! }
//!
//! impl<'de> Deserialize<'de> for B {
//!     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//!     where
//!         D: Deserializer<'de>,
//!     {
//!         let c: Compatible = de::Deserialize::deserialize(deserializer)?;
//!
//!         if c.i.is_some() {
//!             println!("loaded from serialized A, convert to B");
//!             Ok(B { j: c.i.unwrap() })
//!         } else {
//!             println!("loaded from serialized B");
//!             Ok(B { j: c.j.unwrap() })
//!         }
//!     }
//! }
//! ```
pub(crate) mod cmd_00000000_20220427;
