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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_storages_common_cache::TempPath;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Location {
    Remote(String),
    Local(TempPath),
}

impl Location {
    pub fn is_local(&self) -> bool {
        matches!(self, Location::Local(_))
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, Location::Remote(_))
    }
}

#[async_trait::async_trait]
pub trait DataBlockSpill: Clone + Send + Sync + 'static {
    async fn spill(&self, data_block: DataBlock) -> Result<Location> {
        self.merge_and_spill(vec![data_block]).await
    }
    async fn merge_and_spill(&self, data_block: Vec<DataBlock>) -> Result<Location>;
    async fn restore(&self, location: &Location) -> Result<DataBlock>;
}
