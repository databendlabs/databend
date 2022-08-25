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

pub mod information_schema;
pub mod memory;
pub mod null;
pub mod random;
pub mod system;
pub mod view;

mod storages {
    pub use common_catalog::catalog::StorageDescription;
    pub use common_catalog::table::Table;
    pub use common_storages_util::storage_context::StorageContext;

    pub use super::memory;
    pub use super::system;
    pub use super::view;
}

mod catalogs {
    pub use common_catalog::catalog::Catalog;
    pub use common_catalog::catalog::StorageDescription;
}

mod sessions {
    pub use common_catalog::table_context::TableContext;
}

mod pipelines {
    pub use common_pipeline_core::Pipe;
    pub use common_pipeline_core::Pipeline;
    pub use common_pipeline_core::SinkPipeBuilder;
    pub use common_pipeline_core::SourcePipeBuilder;
    pub mod processors {
        pub use common_pipeline_core::processors::*;
        pub use common_pipeline_sinks::processors::sinks::*;
        pub use common_pipeline_sources::processors::sources::AsyncSource;
        pub use common_pipeline_sources::processors::sources::AsyncSourcer;
        pub use common_pipeline_sources::processors::sources::SyncSource;
        pub use common_pipeline_sources::processors::sources::SyncSourcer;
        pub use common_pipeline_transforms::processors::transforms;
    }
}
