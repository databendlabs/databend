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

use std::sync::Arc;

use databend_common_catalog::database::Database;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::types::SeqV;
use educe::Educe;
use paimon::catalog::Identifier;

use crate::PaimonCatalog;
use crate::ParsedName;
use crate::error::map_paimon_result;
use crate::error::read_only;
use crate::parse_system_name;
use crate::system::PaimonSystemTable;
use crate::table::PaimonTable;

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct PaimonDatabase {
    catalog: PaimonCatalog,
    info: DatabaseInfo,
    database_name: String,
}

impl PaimonDatabase {
    pub fn create(catalog: PaimonCatalog, database_name: String) -> Self {
        let info = DatabaseInfo {
            database_id: DatabaseId::new(0),
            name_ident: DatabaseNameIdent::new(
                Tenant::new_literal("dummy"),
                database_name.as_str(),
            ),
            meta: SeqV::new(0, DatabaseMeta {
                engine: crate::PAIMON_CATALOG.to_string(),
                created_on: chrono::Utc::now(),
                updated_on: chrono::Utc::now(),
                ..Default::default()
            }),
        };
        Self {
            catalog,
            info,
            database_name,
        }
    }
}

#[async_trait::async_trait]
impl Database for PaimonDatabase {
    fn name(&self) -> &str {
        &self.database_name
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.info
    }

    #[async_backtrace::framed]
    async fn get_table(&self, table_name: &str) -> Result<Arc<dyn Table>> {
        let (base, branch, kind) = match parse_system_name(table_name) {
            ParsedName::Base(name) => (name, None, None),
            ParsedName::System { base, branch, kind } => (base, branch, Some(kind)),
            ParsedName::UnknownSystem { .. } => {
                return Err(databend_common_exception::ErrorCode::UnknownTable(format!(
                    "{}.{}",
                    self.database_name, table_name
                )));
            }
        };
        let identifier = Identifier::new(self.database_name.clone(), base.clone());
        let paimon_table =
            map_paimon_result(self.catalog.paimon_catalog().get_table(&identifier).await)?;
        if let Some(kind) = kind {
            let paimon_table = match branch {
                Some(branch) => resolve_branch_table(&paimon_table, &base, &branch).await?,
                None => paimon_table,
            };
            return PaimonSystemTable::create(
                self.catalog.info(),
                table_name.to_string(),
                kind,
                &paimon_table,
            );
        }
        PaimonTable::from_paimon_table(
            self.catalog.info(),
            self.catalog.catalog_options().clone(),
            paimon_table,
        )
    }

    #[async_backtrace::framed]
    async fn list_tables_names(&self) -> Result<Vec<String>> {
        map_paimon_result(
            self.catalog
                .paimon_catalog()
                .list_tables(&self.database_name)
                .await,
        )
    }

    #[async_backtrace::framed]
    async fn list_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        let names = self.list_tables_names().await?;
        let mut tables = Vec::with_capacity(names.len());
        for name in names {
            tables.push(self.get_table(&name).await?);
        }
        Ok(tables)
    }

    #[async_backtrace::framed]
    async fn mget_tables(&self, table_names: &[String]) -> Result<Vec<Arc<dyn Table>>> {
        let mut tables = Vec::with_capacity(table_names.len());
        for name in table_names {
            tables.push(self.get_table(name).await?);
        }
        Ok(tables)
    }

    #[async_backtrace::framed]
    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply> {
        Err(read_only("create_table"))
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        Err(read_only("drop_table"))
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        Err(read_only("rename_table"))
    }
}

/// Point a base table at a named branch so system-table readers use
/// `{table}/branch/branch-{name}` metadata (same layout as Paimon managers).
async fn resolve_branch_table(
    base: &paimon::Table,
    base_name: &str,
    branch: &str,
) -> Result<paimon::Table> {
    let branch_location = format!("{}/branch/branch-{}", base.location(), branch);
    let schema_manager = base.schema_manager().with_branch(branch);
    let schema = match map_paimon_result(schema_manager.latest().await)? {
        Some(schema) => (*schema).clone(),
        None => base.schema().clone(),
    };
    let identifier = Identifier::new(
        base.identifier().database(),
        format!("{base_name}$branch_{branch}"),
    );
    Ok(paimon::Table::new(
        base.file_io().clone(),
        identifier,
        branch_location,
        schema,
        base.rest_env().cloned(),
    ))
}
