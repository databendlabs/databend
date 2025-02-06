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

use std::collections::HashSet;

use databend_common_expression::ColumnId;
use databend_storages_common_pruner::VirtualBlockMetaIndex;

use super::VirtualColumnReader;
use crate::io::BlockReader;
use crate::io::NativeSourceData;

impl VirtualColumnReader {
    pub fn sync_read_native_data(
        &self,
        virtual_block_meta: &Option<&VirtualBlockMetaIndex>,
    ) -> Option<(NativeSourceData, Option<HashSet<ColumnId>>)> {
        let Some(virtual_block_meta) = virtual_block_meta else {
            return None;
        };

        let virtual_loc = &virtual_block_meta.virtual_block_location;
        let metas = virtual_block_meta
            .virtual_column_metas
            .values()
            .cloned()
            .collect();

        let readers =
            BlockReader::sync_read_native_column(self.dal.clone(), virtual_loc, metas, None)
                .ok()?;
        let virtual_indices = self.build_virtual_indices(virtual_block_meta);

        let mut results = NativeSourceData::new();
        for (virtual_index, reader) in virtual_indices.into_iter().zip(readers.into_iter()) {
            results.insert(virtual_index, vec![reader]);
        }
        let ignore_column_ids =
            self.generate_ignore_column_ids(&virtual_block_meta.ignored_source_column_ids);

        Some((results, ignore_column_ids))
    }

    pub async fn read_native_data(
        &self,
        virtual_block_meta: &Option<&VirtualBlockMetaIndex>,
    ) -> Option<(NativeSourceData, Option<HashSet<ColumnId>>)> {
        let Some(virtual_block_meta) = virtual_block_meta else {
            return None;
        };

        let virtual_loc = &virtual_block_meta.virtual_block_location;
        let metas = virtual_block_meta
            .virtual_column_metas
            .values()
            .cloned()
            .collect();

        let (_, readers) =
            BlockReader::read_native_columns_data(self.dal.clone(), virtual_loc, 0, metas, None)
                .await
                .ok()?;
        let virtual_indices = self.build_virtual_indices(virtual_block_meta);

        let mut results = NativeSourceData::new();
        for (virtual_index, reader) in virtual_indices.into_iter().zip(readers.into_iter()) {
            results.insert(virtual_index, vec![reader]);
        }
        let ignore_column_ids =
            self.generate_ignore_column_ids(&virtual_block_meta.ignored_source_column_ids);

        Some((results, ignore_column_ids))
    }

    fn build_virtual_indices(&self, virtual_block_meta: &VirtualBlockMetaIndex) -> Vec<usize> {
        let mut virtual_indices = Vec::with_capacity(virtual_block_meta.virtual_column_metas.len());
        for (index, virtual_column_info) in self
            .virtual_column_info
            .virtual_column_fields
            .iter()
            .enumerate()
        {
            if virtual_block_meta
                .virtual_column_metas
                .contains_key(&virtual_column_info.column_id)
            {
                let virtual_index = self.source_schema.num_fields() + index;
                virtual_indices.push(virtual_index);
            }
        }
        virtual_indices
    }
}
