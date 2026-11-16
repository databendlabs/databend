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
use std::sync::Arc;

use chrono::Duration;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::parse_clone_group_id;
use log::info;

use super::string_literal;
use super::string_value;
use crate::FuseTable;
use crate::sessions::TableContext;
use crate::table_functions::SimpleTableFunc;
use crate::table_functions::TableArgs;
pub struct FuseVacuumDropInvertedIndex {
    args: FuseVacuumDropInvertedIndexArgs,
}

struct FuseVacuumDropInvertedIndexArgs {
    database_table: Option<(String, String)>,
}

impl From<&FuseVacuumDropInvertedIndexArgs> for TableArgs {
    fn from(args: &FuseVacuumDropInvertedIndexArgs) -> Self {
        let mut table_args = vec![];
        if let Some((database, table)) = &args.database_table {
            table_args.push(string_literal(database));
            table_args.push(string_literal(table));
        }
        TableArgs::new_positioned(table_args)
    }
}

#[async_trait::async_trait]
impl SimpleTableFunc for FuseVacuumDropInvertedIndex {
    fn get_engine_name(&self) -> String {
        "fuse_vacuum_drop_inverted_index".to_owned()
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("table_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("index_name", TableDataType::String),
            TableField::new("index_version", TableDataType::String),
            TableField::new(
                "num_removed_files",
                TableDataType::Number(NumberDataType::UInt64),
            ),
        ])
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        let mut table_ids = Vec::new();
        let mut index_names = Vec::new();
        let mut index_versions = Vec::new();
        let mut num_removed_files = Vec::new();
        let catalog = ctx.get_default_catalog()?;
        let duration = Duration::days(ctx.get_settings().get_data_retention_time_in_days()? as i64);
        let retention_time = chrono::Utc::now() - duration;
        let tenant = ctx.get_tenant();
        let table = match &self.args.database_table {
            Some((database_name, table_name)) => Some(
                catalog
                    .get_table(&tenant, database_name, table_name)
                    .await?,
            ),
            None => None,
        };
        let table_id = table.map(|t| t.get_id());

        let reply = catalog
            .list_marked_deleted_table_indexes(&tenant, table_id)
            .await?;

        info!(
            "duration: {:?}, retention_time: {:?}, table_id: {:?}, marked_deleted_indexes: {:?}",
            duration, retention_time, table_id, reply
        );

        for (table_id, indexes) in reply.table_indexes {
            let mut indexes_to_be_vacuumed = indexes
                .into_iter()
                .filter(|(_, _, index_meta)| index_meta.dropped_on < retention_time)
                .map(|(index_name, index_version, _)| (index_name, index_version))
                .collect::<Vec<_>>();
            if indexes_to_be_vacuumed.is_empty() {
                continue;
            }
            let Some(table_meta) = catalog.get_table_meta_by_id(table_id).await? else {
                // Skip vacuuming indexes of dropped tables - this will be handled by the vacuum drop table operation
                info!("skip vacuuming indexes of dropped table: {}", table_id);
                continue;
            };
            // Protect index definitions, not every surviving clone. Historical reads and
            // FLASHBACK use the table's current definitions, so dropping/replacing an index on
            // every descendant releases the old version even if segment metadata still lists it.
            // Like the existing cleanup, retention is measured from the owner's DROP, not from
            // the last descendant DROP; this does not provide a lease for in-flight queries.
            let mut used_indexes = table_meta
                .data
                .indexes
                .values()
                .filter(|index| index.index_type == TableIndexType::Inverted)
                .map(|index| (index.name.clone(), index.version.clone()))
                .collect::<HashSet<_>>();
            let group_id = parse_clone_group_id(&table_meta.data.options)?;
            let mut descendants = HashSet::new();
            if let Some(group_id) = group_id {
                let bindings = catalog.list_clone_group_bindings(group_id).await?;
                descendants = FuseTable::clone_descendant_ids(group_id, table_id, &bindings)?;
                if !descendants.is_empty() {
                    let ids = descendants.iter().copied().collect::<Vec<_>>();
                    let metas = catalog.mget_table_metas_by_ids(&ids).await?;
                    // Consume each requested member exactly once. Missing or unexpected metadata
                    // cannot prove that a version is unused, even if a member might have been GCed.
                    let mut unread_ids = descendants.clone();
                    let complete = metas.into_iter().all(|(id, meta)| {
                        let Some(meta) = meta else {
                            return false;
                        };
                        if !unread_ids.remove(&id) {
                            return false;
                        }
                        used_indexes.extend(
                            meta.data
                                .indexes
                                .into_values()
                                .filter(|index| index.index_type == TableIndexType::Inverted)
                                .map(|index| (index.name, index.version)),
                        );
                        true
                    });
                    if !complete || !unread_ids.is_empty() {
                        info!(
                            "defer dropped inverted indexes of table {table_id}: incomplete clone metadata"
                        );
                        continue;
                    }
                }
            }
            // V2 uses the full version; V1 directories use the index name and seven version
            // characters. Keep both layouts if either physical prefix has an active definition.
            let (used_versions, used_legacy_prefixes): (HashSet<_>, HashSet<_>) = used_indexes
                .into_iter()
                .map(|(name, version)| {
                    let short_version = version.chars().take(7).collect::<String>();
                    (version, (name, short_version))
                })
                .unzip();
            indexes_to_be_vacuumed.retain(|(name, version)| {
                !used_versions.contains(version)
                    && !used_legacy_prefixes
                        .contains(&(name.clone(), version.chars().take(7).collect::<String>()))
            });
            if indexes_to_be_vacuumed.is_empty() {
                continue;
            }
            if let Some(group_id) = group_id {
                // A descendant may clone V, then DROP V before its metadata is read above. The
                // first list misses the new child, so recheck before deleting any candidate.
                // After this point a new clone cannot inherit V from a scanned source that no
                // longer defines V: publishing pre-DROP metadata fails the source sequence CAS.
                let bindings = catalog.list_clone_group_bindings(group_id).await?;
                let current = FuseTable::clone_descendant_ids(group_id, table_id, &bindings)?;
                if !current.is_subset(&descendants) {
                    info!(
                        "defer dropped inverted indexes of table {table_id}: new clone descendants"
                    );
                    continue;
                }
            }
            let table_info = TableInfo::new(
                Default::default(),
                Default::default(),
                TableIdent::new(table_id, table_meta.seq),
                table_meta.data,
            );
            let table = catalog.get_table_by_info(&table_info)?;
            info!(
                "indexes_to_be_vacuumed for table: {:?}, indexes: {:?}",
                table_id, indexes_to_be_vacuumed
            );
            for (index_name, index_version) in &indexes_to_be_vacuumed {
                let n = table
                    .remove_inverted_index_files(
                        ctx.clone(),
                        index_name.clone(),
                        index_version.clone(),
                    )
                    .await?;
                table_ids.push(table_id);
                index_names.push(index_name.clone());
                index_versions.push(index_version.clone());
                num_removed_files.push(n);
            }
            catalog
                .remove_marked_deleted_table_indexes(&tenant, table_id, &indexes_to_be_vacuumed)
                .await?;
        }

        Ok(Some(DataBlock::new_from_columns(vec![
            UInt64Type::from_data(table_ids),
            StringType::from_data(index_names),
            StringType::from_data(index_versions),
            UInt64Type::from_data(num_removed_files),
        ])))
    }

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let args = table_args.expect_all_positioned(func_name, None)?;
        let database_table = match args.len() {
            2 => {
                let database_name = string_value(&args[0])?;
                let table_name = string_value(&args[1])?;
                Some((database_name, table_name))
            }
            0 => None,
            _ => {
                return Err(ErrorCode::BadArguments(format!(
                    "expecting (<database_name>, <table_name>) or no args, but got {:?}",
                    args
                )));
            }
        };
        Ok(Self {
            args: FuseVacuumDropInvertedIndexArgs { database_table },
        })
    }
}
