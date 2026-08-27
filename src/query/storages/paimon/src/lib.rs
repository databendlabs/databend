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

mod catalog;
mod database;
mod error;
mod partition;
mod predicate;
mod source;
mod system;
mod table;
mod write;

pub use catalog::PaimonCatalog;
pub use catalog::table_from_info;
pub use partition::PaimonPartInfo;
pub use partition::SerializableDataSplit;
pub use predicate::PaimonPredicateAnalysis;
pub use predicate::analyze_predicate;
pub use predicate::apply_pushdowns;
pub use predicate::can_push_limit;
pub use predicate::projection_column_names;
#[doc(hidden)]
pub use source::reset_table_load_count_for_test;
#[doc(hidden)]
pub use source::table_load_count_for_test;
pub use system::PaimonSystemTableKind;
pub use system::ParsedName;
pub use system::parse_system_name;
#[doc(hidden)]
pub use system::read_system_table;
pub use table::PAIMON_ENGINE;
pub use table::PaimonTable;
pub use table::PaimonTableDescriptor;
pub use write::*;

pub const PAIMON_CATALOG: &str = "paimon";
