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

#![allow(
    clippy::cloned_ref_to_slice_refs,
    clippy::collapsible_if,
    clippy::let_and_return,
    clippy::unnecessary_unwrap,
    clippy::uninlined_format_args
)]
#![allow(clippy::useless_asref)]
#![feature(type_alias_impl_trait)]
#![feature(iter_order_by)]
#![feature(impl_trait_in_assoc_type)]
#![feature(int_roundings)]
#![feature(iterator_try_reduce)]
#![feature(box_patterns)]
#![allow(clippy::large_enum_variant)]
#![recursion_limit = "256"]
#![feature(try_blocks)]

mod constants;
mod fuse_column;
mod fuse_part;
mod fuse_table;
mod fuse_type;
mod retry;

#[cfg(test)]
pub(crate) mod test_utils {
    use std::sync::OnceLock;

    use databend_common_base::base::GlobalInstance;
    use databend_common_base::runtime::GlobalIORuntime;
    use databend_common_config::CacheConfig;
    use databend_common_exception::Result;
    use databend_storages_common_cache::CacheManager;

    pub(crate) fn init_test_globals() -> Result<()> {
        static INIT: OnceLock<Result<()>> = OnceLock::new();
        INIT.get_or_init(|| {
            GlobalInstance::init_production();
            GlobalIORuntime::init(1)?;
            CacheManager::init(&CacheConfig::default(), &(1024 * 1024), "test", false)
        })
        .clone()
    }
}

pub mod io;
pub mod operations;
pub mod pruning;
mod pruning_pipeline;
pub mod statistics;
pub mod table_functions;

pub use constants::*;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
pub use databend_common_catalog::table_context::TableContext;
pub use fuse_column::FuseTableColumnStatisticsProvider;
pub use fuse_part::FuseBlockPartInfo;
pub use fuse_part::FuseLazyPartInfo;
pub use fuse_table::FuseTable;
pub use fuse_table::RetentionPolicy;
pub use fuse_type::FuseSegmentFormat;
pub use fuse_type::FuseStorageFormat;
pub use fuse_type::FuseTableType;
pub use fuse_type::segment_format_from_location;
pub use fuse_type::unsupported_storage_format_error;
pub use io::BlockReadResult;
pub use pruning::SegmentLocation;
pub use retry::commit_with_backoff;
mod sessions {
    pub use databend_common_catalog::table_context::TableContext;
}

pub use databend_storages_common_index as index;
