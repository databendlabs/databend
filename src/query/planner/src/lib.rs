// Copyright 2022 Datafuse Labs.
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

//! Databend Planner is the core part of Databend Query, it will:
//!
//! - Use `Parser` (provided by `common-ast`) to parse query into AST.
//! - Use `Binder` to bind query into `LogicalPlan`
//! - Use `Optimizer` to optimize `LogicalPlan` into `PhysicalPlan`
//!
//! After all the planners work, `Interpreter` will use `PhysicalPlan` to
//! build pipelines, then our processes will produce result data blocks.

mod metadata;
pub use metadata::ColumnEntry;
pub use metadata::ColumnSet;
pub use metadata::IndexType;
pub use metadata::Metadata;
pub use metadata::MetadataRef;
pub use metadata::TableEntry;
pub use metadata::DUMMY_TABLE_INDEX;
