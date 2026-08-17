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
use std::sync::Arc;

use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::StringType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::VirtualColumnPhysicalType;
use itertools::Itertools;
use jsonb::keypath::OwnedKeyPaths;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct VirtualColumnsTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for VirtualColumnsTable {
    const NAME: &'static str = "system.virtual_columns";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let session_state = ctx.session_state()?;

        let catalog_mgr = CatalogManager::instance();
        let catalog = catalog_mgr.get_default_catalog(session_state)?;

        let mut database_names = Vec::new();
        let mut table_names = Vec::new();
        let mut source_column_names = Vec::new();
        let mut virtual_column_names = Vec::new();
        let mut virtual_column_types = Vec::new();

        let dbs = catalog.list_databases(&tenant).await?;
        for db in dbs {
            let tables = catalog.list_tables(&tenant, db.name()).await?;
            for table in tables {
                if !table.storage_format_as_parquet() {
                    continue;
                }
                let Ok(fuse_table) = FuseTable::try_from_table(table.as_ref()) else {
                    continue;
                };
                if !fuse_table.enable_virtual_column() {
                    continue;
                }
                let Some(snapshot) = fuse_table.read_table_snapshot().await? else {
                    continue;
                };

                // Merge the segment-local virtual schemas into one summary schema.
                // Key is (source_column_id, canonical path), value is the union of
                // observed data types across segments.
                let segments_io =
                    SegmentsIO::create(ctx.clone(), fuse_table.get_operator(), fuse_table.schema());
                let segments = segments_io
                    .read_segments::<SegmentInfo>(&snapshot.segments, true)
                    .await?;
                let mut merged: BTreeMap<(u32, String), Vec<VirtualColumnPhysicalType>> =
                    BTreeMap::new();
                for segment in segments {
                    let Ok(segment) = segment else {
                        continue;
                    };
                    let Some(schema) = &segment.summary.virtual_segment_schema else {
                        continue;
                    };
                    for column in &schema.column_paths {
                        for path in &column.paths {
                            let column_id = path.column_id;
                            let entry = merged
                                .entry((column.source_column_id, path.path.clone()))
                                .or_default();
                            for block in &segment.blocks {
                                let Some(column_meta) = block
                                    .virtual_block_meta
                                    .as_ref()
                                    .and_then(|meta| meta.virtual_column_metas.get(&column_id))
                                else {
                                    continue;
                                };
                                let physical_type = column_meta.physical_type();
                                if !entry.contains(&physical_type) {
                                    entry.push(physical_type);
                                }
                            }
                        }
                    }
                }
                if merged.is_empty() {
                    continue;
                }

                let table_schema = table.schema();
                for ((source_column_id, path), mut data_types) in merged {
                    let Ok(source_field) = table_schema.field_of_column_id(source_column_id) else {
                        continue;
                    };
                    let name = OwnedKeyPaths::from_canonical_path(&path)
                        .map(|path| path.to_canonical_path())
                        .unwrap_or(path);
                    data_types.sort_by_key(|data_type| format!("{data_type:?}"));
                    data_types.dedup();

                    database_names.push(db.name().to_owned());
                    table_names.push(table.name().to_owned());
                    source_column_names.push(source_field.name().clone());
                    virtual_column_names.push(name);
                    virtual_column_types.push(
                        data_types
                            .iter()
                            .map(|data_type| data_type.table_data_type().to_string())
                            .join(", "),
                    );
                }
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(database_names),
            StringType::from_data(table_names),
            StringType::from_data(source_column_names),
            StringType::from_data(virtual_column_names),
            StringType::from_data(virtual_column_types),
        ]))
    }
}

impl VirtualColumnsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("database", TableDataType::String),
            TableField::new("table", TableDataType::String),
            TableField::new("source_column", TableDataType::String),
            TableField::new("virtual_column_name", TableDataType::String),
            TableField::new("virtual_column_type", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'virtual_columns'".to_string(),
            name: "virtual_columns".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemVirtualColumns".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}
