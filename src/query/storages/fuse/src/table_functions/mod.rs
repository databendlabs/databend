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

mod cache_admin;
mod clustering_information;
mod clustering_statistics;
mod function_template;
mod fuse_amend;
mod fuse_blocks;
mod fuse_columns;
mod fuse_encodings;
mod fuse_segments;
mod fuse_snapshots;
mod fuse_statistics;
mod table_args;

pub use cache_admin::SetCacheCapacity;
pub use clustering_information::ClusteringInformation;
pub use clustering_information::ClusteringInformationTable;
pub use clustering_statistics::ClusteringStatistics;
pub use clustering_statistics::ClusteringStatisticsTable;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_function::TableFunction;
pub use function_template::SimpleTableFunc;
pub use function_template::TableFunctionTemplate;
pub use function_template::*;
pub use fuse_amend::FuseAmendTable;
pub use fuse_blocks::FuseBlockFunc;
pub use fuse_columns::FuseColumnFunc;
pub use fuse_encodings::FuseEncoding;
pub use fuse_encodings::FuseEncodingTable;
pub use fuse_segments::FuseSegmentFunc;
pub use fuse_snapshots::FuseSnapshotFunc;
pub use fuse_statistics::FuseStatisticTable;
pub use table_args::*;
