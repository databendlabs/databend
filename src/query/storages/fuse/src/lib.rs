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
    clippy::arc_with_non_send_sync,
    clippy::cloned_ref_to_slice_refs,
    clippy::collapsible_if,
    clippy::let_and_return,
    clippy::unnecessary_unwrap,
    clippy::uninlined_format_args
)]
#![allow(clippy::useless_asref)]
#![allow(incomplete_features)]
#![feature(deref_patterns)]
#![allow(clippy::large_enum_variant)]
#![recursion_limit = "256"]

mod constants;
mod fuse_column;
mod fuse_part;
mod fuse_table;
mod fuse_type;
mod retry;

#[cfg(test)]
pub(crate) mod test_utils {
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::OnceLock;

    use databend_common_base::base::GlobalInstance;
    use databend_common_base::runtime::GlobalIORuntime;
    use databend_common_config::CacheConfig;
    use databend_common_exception::Result;
    use databend_storages_common_cache::CacheManager;
    use opendal::raw::Access;
    use opendal::raw::Layer;
    use opendal::raw::LayeredAccess;
    use opendal::raw::OpList;
    use opendal::raw::OpRead;
    use opendal::raw::OpWrite;
    use opendal::raw::RpDelete;
    use opendal::raw::RpList;
    use opendal::raw::RpRead;
    use opendal::raw::RpWrite;

    pub(crate) fn init_test_globals() -> Result<()> {
        static INIT: OnceLock<Result<()>> = OnceLock::new();
        INIT.get_or_init(|| {
            GlobalInstance::init_production();
            GlobalIORuntime::init(1)?;
            CacheManager::init(&CacheConfig::default(), &(1024 * 1024), "test", false)?;
            crate::io::FuseCaches::init(&CacheManager::instance());
            Ok(())
        })
        .clone()
    }

    /// Records the byte range of every storage read.
    #[derive(Clone, Debug, Default)]
    pub(crate) struct RecordingLayer {
        pub(crate) ranges: Arc<Mutex<Vec<Range<u64>>>>,
    }

    impl<A: Access> Layer<A> for RecordingLayer {
        type LayeredAccess = RecordingAccessor<A>;

        fn layer(&self, inner: A) -> Self::LayeredAccess {
            RecordingAccessor {
                inner,
                ranges: self.ranges.clone(),
            }
        }
    }

    #[derive(Clone, Debug)]
    pub(crate) struct RecordingAccessor<A: Access> {
        inner: A,
        ranges: Arc<Mutex<Vec<Range<u64>>>>,
    }

    impl<A: Access> LayeredAccess for RecordingAccessor<A> {
        type Inner = A;
        type Reader = A::Reader;
        type Writer = A::Writer;
        type Lister = A::Lister;
        type Deleter = A::Deleter;

        fn inner(&self) -> &Self::Inner {
            &self.inner
        }

        async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
            let range = args.range();
            if let Some(size) = range.size() {
                self.ranges
                    .lock()
                    .unwrap()
                    .push(range.offset()..range.offset() + size);
            }
            self.inner.read(path, args).await
        }

        async fn write(
            &self,
            path: &str,
            args: OpWrite,
        ) -> opendal::Result<(RpWrite, Self::Writer)> {
            self.inner.write(path, args).await
        }

        async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
            self.inner.list(path, args).await
        }

        async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
            self.inner.delete().await
        }
    }
}

pub mod io;
pub mod operations;
pub mod pruning;
mod pruning_pipeline;
pub mod segment_zone_map;
pub mod statistics;
pub mod table_functions;

pub use constants::*;
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
