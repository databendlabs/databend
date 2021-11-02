// Copyright 2021 Datafuse Labs.
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

use std::convert::TryInto;
use std::sync::Arc;

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api::MetaApi;
use common_meta_raft_store::state_machine::AppliedState;
use common_meta_raft_store::state_machine::TableLookupKey;
use common_meta_types::Change;
use common_meta_types::Cmd;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateTableReply;
use common_meta_types::DatabaseInfo;
use common_meta_types::MatchSeq;
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_tracing::tracing;
use maplit::hashmap;

use crate::MetaEmbedded;

#[async_trait]
impl MetaApi for MetaEmbedded {
    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<CreateDatabaseReply> {
        let cmd = Cmd::CreateDatabase {
            name: plan.db.clone(),
        };

        let mut sm = self.inner.lock().await;
        let res = sm.apply_cmd(&cmd).await?;

        let ch: Change<u64> = res.try_into().unwrap();
        let (prev, result) = ch.unpack_data();

        assert!(result.is_some());

        if prev.is_some() && !plan.if_not_exists {
            return Err(ErrorCode::DatabaseAlreadyExists(format!(
                "{} database exists",
                plan.db
            )));
        }

        Ok(CreateDatabaseReply {
            database_id: result.unwrap(),
        })
    }

    async fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        let cmd = Cmd::DropDatabase {
            name: plan.db.clone(),
        };

        let mut sm = self.inner.lock().await;
        let res = sm.apply_cmd(&cmd).await?;

        assert!(res.result().is_none());

        if res.prev().is_none() && !plan.if_exists {
            return Err(ErrorCode::UnknownDatabase(format!(
                "database not found: {:}",
                plan.db
            )));
        }

        Ok(())
    }

    async fn get_database(&self, db: &str) -> Result<Arc<DatabaseInfo>> {
        let sm = self.inner.lock().await;
        let res = sm
            .get_database(db)?
            .ok_or_else(|| ErrorCode::UnknownDatabase(db.to_string()))?;

        let dbi = DatabaseInfo {
            database_id: res.data,
            db: db.to_string(),
        };
        Ok(Arc::new(dbi))
    }

    async fn get_databases(&self) -> Result<Vec<Arc<DatabaseInfo>>> {
        let sm = self.inner.lock().await;
        let res = sm.get_databases()?;
        Ok(res
            .iter()
            .map(|(name, db)| {
                Arc::new(DatabaseInfo {
                    database_id: *db,
                    db: name.to_string(),
                })
            })
            .collect::<Vec<_>>())
    }

    async fn create_table(&self, plan: CreateTablePlan) -> Result<CreateTableReply> {
        let db_name = &plan.db;
        let table_name = &plan.table;
        let if_not_exists = plan.if_not_exists;

        tracing::info!("create table: {:}: {:?}", &db_name, &table_name);

        let table_meta = plan.table_meta;

        let cr = Cmd::CreateTable {
            db_name: db_name.clone(),
            table_name: table_name.clone(),
            table_meta,
        };

        let mut sm = self.inner.lock().await;
        let res = sm.apply_cmd(&cr).await?;
        let (prev, result) = match res {
            AppliedState::TableIdent { prev, result } => (prev, result),
            _ => {
                panic!("not TableIdent result");
            }
        };

        assert!(result.is_some());

        if prev.is_some() && !if_not_exists {
            Err(ErrorCode::TableAlreadyExists(format!(
                "table exists: {}",
                table_name
            )))
        } else {
            Ok(CreateTableReply {
                table_id: result.unwrap().table_id,
            })
        }
    }

    async fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        let db_name = &plan.db;
        let table_name = &plan.table;
        let if_exists = plan.if_exists;

        let cr = Cmd::DropTable {
            db_name: db_name.clone(),
            table_name: table_name.clone(),
        };

        let mut sm = self.inner.lock().await;
        let res = sm.apply_cmd(&cr).await?;

        assert!(res.result().is_none());

        if res.prev().is_none() && !if_exists {
            return Err(ErrorCode::UnknownTable(format!(
                "Unknown table: '{:}'",
                table_name
            )));
        }

        Ok(())
    }

    async fn get_table(&self, db: &str, table_name: &str) -> Result<Arc<TableInfo>> {
        let sm = self.inner.lock().await;

        let seq_db = sm.get_database(db)?.ok_or_else(|| {
            ErrorCode::UnknownDatabase(format!("get table: database not found {:}", db))
        })?;

        let db_id = seq_db.data;

        let table_id = sm
            .table_lookup()
            .get(&TableLookupKey {
                database_id: db_id,
                table_name: table_name.to_string(),
            })?
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table: '{:}'", table_name)))?;
        let table_id = table_id.data.0;

        let seq_table = sm
            .get_table_by_id(&table_id)?
            .ok_or_else(|| ErrorCode::UnknownTable(table_name.to_string()))?;

        let version = seq_table.seq;
        let table_meta = seq_table.data;

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, version),
            desc: format!("'{}'.'{}'", db, table_name),
            name: table_name.to_string(),
            meta: table_meta,
        };

        Ok(Arc::new(table_info))
    }

    async fn get_tables(&self, db: &str) -> Result<Vec<Arc<TableInfo>>> {
        let sm = self.inner.lock().await;
        let tables = sm.get_tables(db)?;
        Ok(tables
            .iter()
            .map(|t| Arc::new(t.clone()))
            .collect::<Vec<_>>())
    }

    async fn get_table_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)> {
        let sm = self.inner.lock().await;
        let table = sm.get_table_by_id(&table_id)?.ok_or_else(|| {
            ErrorCode::UnknownTable(format!("table of id {} not found", table_id))
        })?;

        let version = table.seq;
        let table_info = table.data;

        Ok((TableIdent::new(table_id, version), Arc::new(table_info)))
    }

    async fn upsert_table_option(
        &self,
        table_id: MetaId,
        table_version: MetaVersion,
        option_key: String,
        option_value: String,
    ) -> Result<UpsertTableOptionReply> {
        let mut sm = self.inner.lock().await;

        let cmd = Cmd::UpsertTableOptions {
            table_id,
            seq: MatchSeq::Exact(table_version),
            table_options: hashmap! {
                option_key => Some(option_value),
            },
        };

        let res = sm.apply_cmd(&cmd).await?;
        if !res.changed() {
            let ch: Change<TableMeta> = res.try_into().unwrap();
            let (prev, _result) = ch.unwrap();

            return Err(ErrorCode::TableVersionMissMatch(format!(
                "targeting version {}, current version {}",
                table_version, prev.seq,
            )));
        }

        Ok(())
    }

    fn name(&self) -> String {
        "meta-embedded".to_string()
    }
}
