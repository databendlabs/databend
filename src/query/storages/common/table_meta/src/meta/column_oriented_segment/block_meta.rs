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
use std::collections::HashSet;

use databend_common_expression::ColumnId;

use super::ColumnOrientedSegment;
use crate::meta::BlockMeta;
use crate::meta::ColumnMeta;
use crate::meta::ColumnMetaV0;
pub trait AbstractBlockMeta: Send + Sync + 'static + Sized {
    fn block_size(&self) -> u64;
    fn file_size(&self) -> u64;
    fn row_count(&self) -> u64;
    fn location_path(&self) -> String;
    fn col_metas(&self, col_ids: &HashSet<ColumnId>) -> HashMap<ColumnId, ColumnMeta>;
}

impl AbstractBlockMeta for BlockMeta {
    fn block_size(&self) -> u64 {
        self.block_size
    }

    fn file_size(&self) -> u64 {
        self.file_size
    }

    fn row_count(&self) -> u64 {
        self.row_count
    }
    fn col_metas(&self, col_ids: &HashSet<ColumnId>) -> HashMap<ColumnId, ColumnMeta> {
        let mut col_metas = HashMap::new();
        for col_id in col_ids {
            if let Some(col_meta) = self.col_metas.get(col_id) {
                col_metas.insert(*col_id, col_meta.clone());
            }
        }
        col_metas
    }

    fn location_path(&self) -> String {
        self.location.0.to_string()
    }
}

impl AbstractBlockMeta for ColumnOrientedBlockMeta {
    fn block_size(&self) -> u64 {
        self.segment.block_size_col()[self.row_number]
    }

    fn file_size(&self) -> u64 {
        self.segment.file_size_col()[self.row_number]
    }

    fn row_count(&self) -> u64 {
        self.segment.row_count_col()[self.row_number]
    }

    fn col_metas(&self, col_ids: &HashSet<ColumnId>) -> HashMap<ColumnId, ColumnMeta> {
        let col_meta_cols = self.segment.col_meta_cols(col_ids);
        let mut col_metas = HashMap::new();
        for column_id in col_ids {
            let meta_col = col_meta_cols.get(column_id).unwrap();
            let meta = meta_col.index(self.row_number).unwrap();
            let meta = meta.as_tuple().unwrap();
            let offset = meta[0].as_number().unwrap().as_u_int64().unwrap();
            let length = meta[1].as_number().unwrap().as_u_int64().unwrap();
            let num_values = meta[2].as_number().unwrap().as_u_int64().unwrap();
            col_metas.insert(
                *column_id,
                ColumnMeta::Parquet(ColumnMetaV0 {
                    offset: *offset,
                    len: *length,
                    num_values: *num_values,
                }),
            );
        }
        col_metas
    }

    fn location_path(&self) -> String {
        self.segment
            .location_path_col()
            .index(self.row_number)
            .unwrap()
            .to_string()
    }
}

pub struct ColumnOrientedBlockMeta {
    pub row_number: usize,
    pub segment: ColumnOrientedSegment,
}
