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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::VirtualColumnInfo;
use databend_common_exception::Result;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_table_meta::meta::VirtualBlockMeta;

pub struct VirtualColumnPruner {
    virtual_column: VirtualColumnInfo,
}

impl VirtualColumnPruner {
    pub fn try_create(
        push_down: &Option<PushDownInfo>,
    ) -> Result<Option<Arc<VirtualColumnPruner>>> {
        let virtual_column = push_down.as_ref().and_then(|p| p.virtual_column.as_ref());
        if let Some(virtual_column) = virtual_column {
            return Ok(Some(Arc::new(VirtualColumnPruner {
                virtual_column: virtual_column.clone(),
            })));
        }
        Ok(None)
    }

    #[async_backtrace::framed]
    pub async fn prune_virtual_columns(
        &self,
        virtual_block_meta: &Option<VirtualBlockMeta>,
    ) -> Result<Option<VirtualBlockMetaIndex>> {
        if let Some(virtual_block_meta) = virtual_block_meta {
            let mut virtual_column_metas = BTreeMap::new();
            let mut need_source_column_ids = HashSet::new();
            for virtual_column_field in &self.virtual_column.virtual_column_fields {
                if let Some(virtual_column_meta) = virtual_block_meta
                    .virtual_col_metas
                    .get(&virtual_column_field.column_id)
                {
                    virtual_column_metas
                        .insert(virtual_column_field.column_id, virtual_column_meta.clone());
                    continue;
                }
                // The virtual column does not exist and must be generated from the source column.
                need_source_column_ids.insert(virtual_column_field.source_column_id);
            }
            // The remaining source column can be ignored.
            let mut ignored_source_column_ids = HashSet::new();
            for column_id in self
                .virtual_column
                .source_column_ids
                .difference(&need_source_column_ids)
            {
                ignored_source_column_ids.insert(*column_id);
            }

            if !virtual_column_metas.is_empty() {
                let virtual_block_meta = VirtualBlockMetaIndex {
                    virtual_block_location: virtual_block_meta.virtual_location.0.clone(),
                    virtual_column_metas,
                    ignored_source_column_ids,
                };
                return Ok(Some(virtual_block_meta));
            }
        }
        Ok(None)
    }
}
