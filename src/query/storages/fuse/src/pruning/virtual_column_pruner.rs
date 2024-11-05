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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::VirtualColumnInfo;
use databend_common_exception::Result;
use databend_common_storage::parquet_rs::infer_schema_with_extension;
use databend_common_storage::parquet_rs::read_metadata_async;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_table_meta::meta::ColumnMeta;
use opendal::Operator;

use crate::io::read::build_columns_meta;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::FuseStorageFormat;

pub struct VirtualColumnPruner {
    dal: Operator,
    virtual_column: VirtualColumnInfo,
    storage_format: FuseStorageFormat,
}

impl VirtualColumnPruner {
    pub fn try_create(
        dal: Operator,
        push_down: &Option<PushDownInfo>,
        storage_format: FuseStorageFormat,
    ) -> Result<Option<Arc<VirtualColumnPruner>>> {
        let virtual_column = push_down.as_ref().and_then(|p| p.virtual_column.as_ref());
        if let Some(virtual_column) = virtual_column {
            return Ok(Some(Arc::new(VirtualColumnPruner {
                dal,
                virtual_column: virtual_column.clone(),
                storage_format,
            })));
        }
        Ok(None)
    }

    #[async_backtrace::framed]
    pub async fn read_metas_schema(
        &self,
        virtual_loc: &str,
    ) -> Option<(HashMap<u32, ColumnMeta>, ArrowSchema)> {
        let (metas, schema) = match self.storage_format {
            FuseStorageFormat::Parquet => {
                let metadata = read_metadata_async(virtual_loc, &self.dal, None)
                    .await
                    .ok()?;
                debug_assert_eq!(metadata.num_row_groups(), 1);
                let row_group = &metadata.row_groups()[0];
                let schema = infer_schema_with_extension(metadata.file_metadata()).ok()?;
                let columns_meta = build_columns_meta(row_group);
                (columns_meta, schema)
            }
            FuseStorageFormat::Native => {
                let (metas, schema) =
                    BlockReader::async_read_native_schema(&self.dal, virtual_loc).await?;
                let mut columns_meta = HashMap::with_capacity(metas.len());
                for (index, meta) in metas.into_iter().enumerate() {
                    columns_meta.insert(index as u32, meta);
                }
                (columns_meta, schema)
            }
        };
        Some((metas, schema))
    }

    #[async_backtrace::framed]
    pub async fn prune_virtual_columns(
        &self,
        block_loc: &str,
    ) -> Result<Option<VirtualBlockMetaIndex>> {
        let virtual_loc = TableMetaLocationGenerator::gen_virtual_block_location(block_loc);

        if let Some((mut metas, schema)) = self.read_metas_schema(&virtual_loc).await {
            let mut virtual_column_metas = BTreeMap::new();
            let mut need_source_column_ids = HashSet::new();
            for virtual_column_field in &self.virtual_column.virtual_column_fields {
                if let Ok(idx) = schema.index_of(&virtual_column_field.name) {
                    if let Some(meta) = metas.remove(&(idx as u32)) {
                        virtual_column_metas.insert(virtual_column_field.column_id, meta);
                        continue;
                    }
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
                    virtual_block_location: virtual_loc,
                    virtual_column_metas,
                    ignored_source_column_ids,
                };
                return Ok(Some(virtual_block_meta));
            }
        }
        Ok(None)
    }
}
