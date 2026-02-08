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

use std::collections::HashMap;

use databend_common_expression::ColumnId;

use crate::meta::BlockMeta;
use crate::meta::ColumnMeta;
use crate::meta::Compression;
use crate::meta::VariantEncoding;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct BlockReadInfo {
    pub location: String,
    pub row_count: u64,
    pub col_metas: HashMap<ColumnId, ColumnMeta>,
    pub compression: Compression,
    pub block_size: u64,
    #[serde(default)]
    pub variant_encoding: VariantEncoding,
}

impl From<&BlockMeta> for BlockReadInfo {
    fn from(meta: &BlockMeta) -> Self {
        BlockReadInfo {
            location: meta.location.0.clone(),
            row_count: meta.row_count,
            col_metas: meta.col_metas.clone(),
            compression: meta.compression,
            block_size: meta.block_size,
            variant_encoding: meta.variant_encoding,
        }
    }
}
