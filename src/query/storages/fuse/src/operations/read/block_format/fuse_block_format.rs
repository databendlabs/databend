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

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::Result;
use databend_storages_common_io::ReadSettings;

use crate::operations::read::read_data_source::ReadDataSource;

/// Format-specific reader for Fuse blocks.
///
/// The common read transform owns partition dispatch, runtime pruning, and
/// concurrency. Implementations only need to turn one pruned part into the
/// source payload that downstream format-specific deserializers consume.
#[async_trait::async_trait]
pub trait FuseBlockFormat: Send + Sync {
    /// Reads one block part and returns the wrapped source payload for the
    /// common read pipeline.
    async fn read_data(&self, part: PartInfoPtr, settings: ReadSettings) -> Result<ReadDataSource>;
}
