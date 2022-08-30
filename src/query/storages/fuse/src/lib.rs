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

#![feature(generic_associated_types)]
#![feature(type_alias_impl_trait)]
#![feature(io_error_other)]
#![deny(unused_crate_dependencies)]

mod constants;
mod fuse_part;
mod fuse_table;
pub mod io;
pub mod operations;
pub mod pruning;
pub mod statistics;
pub mod table_functions;
use common_catalog::table::NavigationPoint;
use common_catalog::table::Table;
use common_catalog::table::TableStatistics;
pub use common_catalog::table_context::TableContext;
use common_catalog::table_mutator::TableMutator;
use common_storages_util::table_option_keys;
pub use constants::*;
pub use fuse_part::ColumnLeaf;
pub use fuse_part::ColumnLeaves;
pub use fuse_table::FuseTable;
pub use table_option_keys::*;

mod sessions {
    pub use common_catalog::table_context::TableContext;
}

mod pipelines {
    pub use common_pipeline_core::Pipe;
    pub use common_pipeline_core::Pipeline;
    pub mod processors {
        pub use common_pipeline_core::processors::*;
        pub use common_pipeline_sources::processors::sources::*;
        pub use common_pipeline_transforms::processors::transforms;
    }
}

pub use common_storages_index as index;
