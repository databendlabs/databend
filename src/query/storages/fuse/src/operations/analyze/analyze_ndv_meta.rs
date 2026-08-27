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
use std::fmt::Debug;
use std::fmt::Formatter;

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::ColumnId;
use databend_common_expression::local_block_meta_serde;
use databend_common_statistics::KllSketch;
use databend_common_storage::MetaHLL;
use databend_storages_common_table_meta::meta::BlockCountMinSketch;
use databend_storages_common_table_meta::meta::BlockTopN;

#[derive(Clone)]
pub struct AnalyzeNDVMeta {
    pub row_count: u64,
    pub unstats_rows: u64,
    pub column_hlls: HashMap<ColumnId, MetaHLL>,
    pub top_n: Option<BlockTopN>,
    pub count_min_sketch: Option<BlockCountMinSketch>,
    pub dropped_top_n_columns: Vec<ColumnId>,
    pub kll_histograms: HashMap<ColumnId, KllSketch>,
}

impl AnalyzeNDVMeta {
    pub fn create(
        row_count: u64,
        unstats_rows: u64,
        column_hlls: HashMap<ColumnId, MetaHLL>,
        top_n: Option<BlockTopN>,
        count_min_sketch: Option<BlockCountMinSketch>,
        dropped_top_n_columns: Vec<ColumnId>,
        kll_histograms: HashMap<ColumnId, KllSketch>,
    ) -> BlockMetaInfoPtr {
        Box::new(AnalyzeNDVMeta {
            row_count,
            unstats_rows,
            column_hlls,
            top_n,
            count_min_sketch,
            dropped_top_n_columns,
            kll_histograms,
        })
    }
}

impl Debug for AnalyzeNDVMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("AnalyzeNDVMeta").finish()
    }
}

local_block_meta_serde!(AnalyzeNDVMeta);

#[typetag::serde(name = "analyze_ndv")]
impl BlockMetaInfo for AnalyzeNDVMeta {}
