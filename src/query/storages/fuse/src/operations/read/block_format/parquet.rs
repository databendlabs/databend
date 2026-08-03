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

use databend_common_storage::read_metadata_async;
use opendal::Operator;

use super::ReadBlockMeta;
use crate::io::build_columns_meta;

pub struct FuseParquetBlockFormat;

impl FuseParquetBlockFormat {
    pub fn create() -> Self {
        Self
    }

    /// Reads the metadata needed to fetch an arbitrary block location.
    pub async fn read_block_meta(
        &self,
        operator: &Operator,
        location: &str,
    ) -> Option<ReadBlockMeta> {
        let metadata = read_metadata_async(location, operator, None).await.ok()?;
        debug_assert_eq!(metadata.num_row_groups(), 1);
        let row_group = &metadata.row_groups()[0];

        Some(ReadBlockMeta {
            columns_meta: build_columns_meta(row_group),
            num_rows: row_group.num_rows() as u64,
        })
    }
}
