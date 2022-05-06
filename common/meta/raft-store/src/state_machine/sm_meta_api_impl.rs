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

use common_meta_api::MetaApi;
use common_meta_types::anyerror::AnyError;
use common_meta_types::AppError;
use common_meta_types::Change;
use common_meta_types::Cmd;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateShareReply;
use common_meta_types::CreateShareReq;
use common_meta_types::CreateTableReply;
use common_meta_types::CreateTableReq;
use common_meta_types::DatabaseAlreadyExists;
use common_meta_types::DatabaseIdent;
use common_meta_types::DatabaseInfo;
use common_meta_types::DatabaseMeta;
use common_meta_types::DatabaseNameIdent;
use common_meta_types::DropDatabaseReply;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropShareReply;
use common_meta_types::DropShareReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::GetDatabaseReq;
use common_meta_types::GetShareReq;
use common_meta_types::GetTableReq;
use common_meta_types::ListDatabaseReq;
use common_meta_types::ListTableReq;
use common_meta_types::MetaError;
use common_meta_types::MetaId;
use common_meta_types::MetaStorageError;
use common_meta_types::RenameDatabaseReply;
use common_meta_types::RenameDatabaseReq;
use common_meta_types::RenameTableReply;
use common_meta_types::RenameTableReq;
use common_meta_types::ShareAlreadyExists;
use common_meta_types::ShareInfo;
use common_meta_types::TableAlreadyExists;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::TableVersionMismatched;
use common_meta_types::UnknownDatabase;
use common_meta_types::UnknownShare;
use common_meta_types::UnknownTable;
use common_meta_types::UnknownTableId;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;
use common_tracing::tracing;

use crate::state_machine::DatabaseLookupKey;
use crate::state_machine::StateMachine;
use crate::state_machine::TableLookupKey;

#[async_trait::async_trait]
impl MetaApi for StateMachine {
    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> Result<CreateDatabaseReply, MetaError> {
        let res = self.sm_tree.txn(true, |t| {
            let r = self.apply_cmd(&Cmd::CreateDatabase(req.clone()), &t)?;
            Ok(r)
        })?;

        let mut ch: Change<DatabaseMeta> = res.try_into().unwrap();
        let db_id = ch.ident.take().expect("Some(db_id)");
        let (prev, result) = ch.unpack_data();

        assert!(result.is_some());

        if prev.is_some() && !req.if_not_exists {
            let ae = AppError::from(DatabaseAlreadyExists::new(
                req.name_ident.db_name,
                "create database",
            ));
            return Err(MetaError::from(ae));
        }

        Ok(CreateDatabaseReply { database_id: db_id })
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply, MetaError> {
        let res = self.sm_tree.txn(true, |t| {
            let r = self.apply_cmd(&Cmd::DropDatabase(req.clone()), &t)?;
            Ok(r)
        })?;

        assert!(res.result().is_none());

        if res.prev().is_none() && !req.if_exists {
            let ae = AppError::from(UnknownDatabase::new(
                req.name_ident.db_name,
                "drop database",
            ));
            return Err(MetaError::from(ae));
        }

        Ok(DropDatabaseReply {})
    }

    async fn rename_database(
        &self,
        req: RenameDatabaseReq,
    ) -> Result<RenameDatabaseReply, MetaError> {
        let res = self.sm_tree.txn(true, |t| {
            let r = self.apply_cmd(&Cmd::RenameDatabase(req.clone()), &t)?;
            Ok(r)
        })?;

        assert!(res.result().is_none());

        if res.prev().is_none() {
            let ae = AppError::from(UnknownDatabase::new(
                req.name_ident.db_name,
                "rename database",
            ));
            return Err(MetaError::from(ae));
        }

        Ok(RenameDatabaseReply {})
    }

    async fn get_database(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>, MetaError> {
        let db_id = self.get_database_id(&req.tenant, &req.db_name)?;
        let seq_meta = self.get_database_meta_by_id(&db_id)?;

        let dbi = DatabaseInfo {
            ident: DatabaseIdent {
                db_id,
                seq: seq_meta.seq,
            },
            name_ident: DatabaseNameIdent {
                tenant: req.tenant.clone(),
                db_name: req.db_name.clone(),
            },
            meta: seq_meta.data,
        };
        Ok(Arc::new(dbi))
    }

    async fn list_databases(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, MetaError> {
        let mut res = vec![];

        let it = self
            .database_lookup()
            .scan_prefix(&DatabaseLookupKey::new(req.tenant.clone(), "".to_string()))?;

        for r in it {
            let (db_lookup_key, seq_id) = r;
            let seq_meta = self.get_database_meta_by_id(&seq_id.data)?;

            let db_info = DatabaseInfo {
                ident: DatabaseIdent {
                    db_id: seq_id.data,
                    seq: seq_meta.seq,
                },
                name_ident: DatabaseNameIdent {
                    tenant: req.tenant.clone(),
                    db_name: db_lookup_key.get_database_name(),
                },
                meta: seq_meta.data,
            };
            res.push(Arc::new(db_info));
        }

        Ok(res)
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply, MetaError> {
        let db_name = req.db_name();
        let table_name = req.table_name();
        let if_not_exists = req.if_not_exists;

        tracing::info!("create table: {:}-{}", &db_name, &table_name);

        let res = self.sm_tree.txn(true, |t| {
            let r = self.apply_cmd(&Cmd::CreateTable(req.clone()), &t)?;
            Ok(r)
        })?;

        let mut ch: Change<TableMeta, u64> = res.try_into().unwrap();
        let table_id = ch.ident.take().unwrap();
        let (prev, result) = ch.unpack_data();

        assert!(result.is_some());

        if prev.is_some() && !if_not_exists {
            let ae = AppError::from(TableAlreadyExists::new(table_name, "create_table"));
            Err(MetaError::from(ae))
        } else {
            Ok(CreateTableReply { table_id })
        }
    }

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply, MetaError> {
        let table_name = req.table_name().to_string();
        let if_exists = req.if_exists;

        let res = self.sm_tree.txn(true, |t| {
            let r = self.apply_cmd(&Cmd::DropTable(req.clone()), &t)?;
            Ok(r)
        })?;

        assert!(res.result().is_none());

        if res.prev().is_none() && !if_exists {
            let ae = AppError::from(UnknownTable::new(table_name, "drop_table"));
            return Err(MetaError::from(ae));
        }

        Ok(DropTableReply {})
    }

    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply, MetaError> {
        let res = self
            .sm_tree
            .txn(true, |t| self.apply_cmd(&Cmd::RenameTable(req.clone()), &t));
        if let Err(MetaStorageError::AppError(AppError::UnknownTable(e))) = res {
            if req.if_exists {
                Ok(RenameTableReply { table_id: 0 })
            } else {
                Err(MetaError::AppError(AppError::UnknownTable(e)))
            }
        } else {
            let mut ch: Change<TableMeta, u64> = res?.try_into().unwrap();
            let table_id = ch.ident.take().unwrap();
            Ok(RenameTableReply { table_id })
        }
    }

    async fn get_table(&self, req: GetTableReq) -> Result<Arc<TableInfo>, MetaError> {
        let tenant = &req.tenant;
        let db = &req.db_name;
        let table_name = &req.table_name;

        let db_id = self.get_database_id(tenant, db)?;

        let table_id = self
            .table_lookup()
            .get(&TableLookupKey {
                database_id: db_id,
                table_name: table_name.to_string(),
            })?
            .ok_or_else(|| AppError::from(UnknownTable::new(table_name, "get_table")))?;

        let table_id = table_id.data.0;

        let seq_table = self
            .get_table_meta_by_id(&table_id)?
            .ok_or_else(|| AppError::from(UnknownTableId::new(table_id, "get_table")))?;

        let version = seq_table.seq;
        let table_meta = seq_table.data;

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, version),
            desc: format!("'{}'.'{}'.'{}'", tenant, db, table_name),
            name: table_name.to_string(),
            meta: table_meta,
        };

        Ok(Arc::new(table_info))
    }

    async fn list_tables(&self, req: ListTableReq) -> Result<Vec<Arc<TableInfo>>, MetaError> {
        let tenant = &req.tenant;
        let db_name = &req.db_name;
        let db_id = self.get_database_id(tenant, db_name)?;

        let mut tbls = vec![];
        let tables = self.tables();
        let tables_iter = self.table_lookup().range(..)?;
        for r in tables_iter {
            let (k, seq_table_id) = r?;

            let got_db_id = k.database_id;
            let table_name = k.table_name;

            if got_db_id == db_id {
                let table_id = seq_table_id.data.0;

                let seq_table_meta = tables.get(&table_id)?.ok_or_else(|| {
                    let ut = UnknownTableId::new(table_id, "list_tables");
                    MetaStorageError::Damaged(AnyError::new(&ut))
                })?;

                let version = seq_table_meta.seq;
                let table_meta = seq_table_meta.data;

                let table_info = TableInfo::new(
                    db_name,
                    &table_name,
                    TableIdent::new(table_id, version),
                    table_meta,
                );

                tbls.push(Arc::new(table_info));
            }
        }

        Ok(tbls)
    }

    async fn get_table_by_id(
        &self,
        table_id: MetaId,
    ) -> Result<(TableIdent, Arc<TableMeta>), MetaError> {
        let x = self.tables().get(&table_id)?;

        let table =
            x.ok_or_else(|| AppError::from(UnknownTableId::new(table_id, "get_table_by_id")))?;

        let version = table.seq;
        let table_meta = table.data;

        Ok((TableIdent::new(table_id, version), Arc::new(table_meta)))
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply, MetaError> {
        let cmd = Cmd::UpsertTableOptions(req.clone());

        let res = self.sm_tree.txn(true, |t| {
            let r = self.apply_cmd(&cmd, &t)?;
            Ok(r)
        })?;
        if !res.changed() {
            let ch: Change<TableMeta> = res.try_into().unwrap();
            let (prev, _result) = ch.unwrap();

            let ae = AppError::from(TableVersionMismatched::new(
                req.table_id,
                req.seq,
                prev.seq,
                "upsert_table_option",
            ));
            return Err(MetaError::from(ae));
        }

        Ok(UpsertTableOptionReply {})
    }

    async fn create_share(&self, req: CreateShareReq) -> Result<CreateShareReply, MetaError> {
        let share_name = &req.share_name;
        let if_not_exists = req.if_not_exists;

        tracing::info!("create share: {:}: {:?}", &req.tenant, &share_name);

        let res = self.sm_tree.txn(true, |t| {
            let r = self.apply_cmd(&Cmd::CreateShare(req.clone()), &t)?;
            Ok(r)
        })?;

        let mut ch: Change<ShareInfo, u64> = res.try_into().unwrap();
        let share_id = ch.ident.take().unwrap();
        let (prev, result) = ch.unpack_data();

        assert!(result.is_some());

        if prev.is_some() && !if_not_exists {
            let ae = AppError::from(ShareAlreadyExists::new(share_name, "create share"));
            Err(MetaError::from(ae))
        } else {
            Ok(CreateShareReply { share_id })
        }
    }

    async fn drop_share(&self, req: DropShareReq) -> Result<DropShareReply, MetaError> {
        let share_name = &req.share_name;
        let if_exists = req.if_exists;

        let res = self.sm_tree.txn(true, |t| {
            let r = self.apply_cmd(&Cmd::DropShare(req.clone()), &t)?;
            Ok(r)
        })?;

        assert!(res.result().is_none());

        if res.prev().is_none() && !if_exists {
            let ae = AppError::from(UnknownShare::new(share_name, "drop share"));
            return Err(MetaError::from(ae));
        }

        Ok(DropShareReply {})
    }

    async fn get_share(&self, req: GetShareReq) -> Result<Arc<ShareInfo>, MetaError> {
        let share_id = self.get_share_id(&req.tenant, &req.share_name)?;
        let seq_share_info = self.get_share_info_by_id(&share_id)?;
        Ok(Arc::new(seq_share_info.data))
    }

    fn name(&self) -> String {
        "StateMachine".to_string()
    }
}
