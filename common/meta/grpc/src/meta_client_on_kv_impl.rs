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

use std::sync::Arc;

use anyerror::AnyError;
use common_meta_api::KVApi;
use common_meta_api::MetaApi;
use common_meta_types::txn_condition;
use common_meta_types::txn_op::Request;
use common_meta_types::AppError;
use common_meta_types::ConditionResult;
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
use common_meta_types::DatabaseTenantIdIdent;
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
use common_meta_types::MatchSeq;
use common_meta_types::MetaError;
use common_meta_types::MetaId;
use common_meta_types::Operation;
use common_meta_types::RenameTableReply;
use common_meta_types::RenameTableReq;
use common_meta_types::ShareInfo;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::TxnCondition;
use common_meta_types::TxnDeleteRequest;
use common_meta_types::TxnOp;
use common_meta_types::TxnOpResponse;
use common_meta_types::TxnPutRequest;
use common_meta_types::TxnRequest;
use common_meta_types::UnknownDatabase;
use common_meta_types::UpsertKVAction;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;
use common_proto_conv::FromToProto;
use common_tracing::tracing;
use txn_condition::Target;
use ConditionResult::Eq;

use crate::meta_client_on_kv::DatabaseIdGen;
use crate::meta_client_on_kv::KVApiKey;
use crate::MetaClientOnKV;

#[tonic::async_trait]
impl MetaApi for MetaClientOnKV {
    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> Result<CreateDatabaseReply, MetaError> {
        let name_key = &req.name_ident;

        let mut id_key = DatabaseTenantIdIdent {
            tenant: name_key.tenant.clone(),
            db_id: 0,
        };

        loop {
            // Get db by name to ensure absence
            let (db_id_seq, db_id) = self.get_db_id_by_name(name_key).await?;
            tracing::debug!(db_id_seq, db_id, ?name_key, "--- xp:  get");

            if db_id_seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateDatabaseReply { database_id: db_id })
                } else {
                    Err(MetaError::AppError(AppError::DatabaseAlreadyExists(
                        DatabaseAlreadyExists::new(
                            &name_key.db_name,
                            format!("create db: tenant: {}", name_key.tenant),
                        ),
                    )))
                };
            }

            let db_id = self.fetch_id(DatabaseIdGen {}).await?;
            id_key.db_id = db_id;

            tracing::debug!(db_id, name_key = debug(&name_key), "new database id");

            {
                let txn_req = TxnRequest {
                    condition: vec![self.txn_cond_seq(name_key, Eq, 0)?],
                    if_then: vec![
                        self.txn_op_put(name_key, self.serialize_id(db_id)?)?, // (tenant, db_name) -> db_id
                        self.txn_op_put(&id_key, self.serialize_struct(&req.meta)?)?, // (tenant, db_id) -> db_meta
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = self.send_txn(txn_req).await?;

                tracing::debug!(
                    name = debug(&name_key),
                    id = debug(&id_key),
                    succ = display(succ),
                    "create_database"
                );

                if succ {
                    return Ok(CreateDatabaseReply { database_id: db_id });
                }
            }
        }
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply, MetaError> {
        let name_key = &req.name_ident;

        let mut id_key = DatabaseTenantIdIdent {
            tenant: name_key.tenant.clone(),
            db_id: 0,
        };

        loop {
            let res = self.get_db_or_err(name_key).await;
            let (db_id_seq, db_id, db_meta_seq, _db_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let MetaError::AppError(AppError::UnknownDatabase(_)) = e {
                        if req.if_exists {
                            return Ok(DropDatabaseReply {});
                        }
                    }

                    return Err(e);
                }
            };

            id_key.db_id = db_id;

            tracing::debug!(db_id, name_key = debug(&name_key), "drop_database");

            {
                let txn_req = TxnRequest {
                    condition: vec![
                        self.txn_cond_seq(name_key, Eq, db_id_seq)?,
                        self.txn_cond_seq(&id_key, Eq, db_meta_seq)?,
                    ],
                    if_then: vec![
                        self.txn_op_del(name_key)?, // (tenant, db_name) -> db_id
                        self.txn_op_del(&id_key)?,  // (tenant, db_id) -> db_meta
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = self.send_txn(txn_req).await?;

                tracing::debug!(
                    name = debug(&name_key),
                    id = debug(&id_key),
                    succ = display(succ),
                    "drop_database"
                );

                if succ {
                    return Ok(DropDatabaseReply {});
                }
            }
        }
    }

    async fn get_database(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>, MetaError> {
        let name_key = &req.inner;

        let mut id_key = DatabaseTenantIdIdent {
            tenant: name_key.tenant.clone(),
            db_id: 0,
        };

        // Get db id by name
        let (db_id_seq, db_id) = self.get_db_id_by_name(name_key).await?;
        self.db_has_to_exist(db_id_seq, name_key)?;

        id_key.db_id = db_id;

        // Get db_meta by id
        let (db_meta_seq, db_meta) = self.get_db_by_id(&id_key).await?;
        self.db_has_to_exist(db_meta_seq, name_key)?;

        let db = DatabaseInfo {
            ident: DatabaseIdent {
                db_id,
                seq: db_meta_seq,
            },
            name_ident: name_key.clone(),
            meta: db_meta.unwrap(),
        };

        Ok(Arc::new(db))
    }

    async fn list_databases(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, MetaError> {
        let name_key = DatabaseNameIdent {
            tenant: req.tenant,
            // Using a empty db to to list all
            db_name: "".to_string(),
        };

        let prefix = name_key.to_key();
        // Pairs of db-name and db_id with seq
        let key_seq_ids = self.inner.prefix_list_kv(&prefix).await?;

        let n = key_seq_ids.len();

        // Keys for fetching serialized DatabaseMeta from KVApi
        let mut kv_keys = Vec::with_capacity(n);
        let mut db_names = Vec::with_capacity(n);
        let mut db_ids = Vec::with_capacity(n);

        for (db_name_key, seq_id) in key_seq_ids.iter() {
            let db_id = self
                .deserialize_id(&seq_id.data)
                .map_err(Self::meta_encode_err)?;
            db_ids.push(db_id);

            // Parse key and get db_name:

            let n = DatabaseNameIdent::from_key(db_name_key).map_err(Self::meta_encode_err)?;
            db_names.push(n.db_name);

            // Build KVApi key for `mget()` db_meta
            let tenant_id_key = DatabaseTenantIdIdent {
                tenant: name_key.tenant.clone(),
                db_id,
            };

            let k = tenant_id_key.to_key();
            kv_keys.push(k);
        }

        // Batch get all db-metas.
        // - A db-meta may be already deleted. It is Ok. Just ignore it.

        let seq_metas = self.inner.mget_kv(&kv_keys).await?;
        let mut db_infos = Vec::with_capacity(kv_keys.len());

        for (i, seq_meta_opt) in seq_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                let db_meta: DatabaseMeta = self
                    .deserialize_struct(&seq_meta.data)
                    .map_err(Self::meta_encode_err)?;

                let db_info = DatabaseInfo {
                    ident: DatabaseIdent {
                        db_id: db_ids[i],
                        seq: seq_meta.seq,
                    },
                    name_ident: DatabaseNameIdent {
                        tenant: name_key.tenant.clone(),
                        db_name: db_names[i].clone(),
                    },
                    meta: db_meta,
                };
                db_infos.push(Arc::new(db_info));
            } else {
                tracing::debug!(
                    k = display(&kv_keys[i]),
                    "db_meta not found, maybe just deleted after listing names and before listing meta"
                );
            }
        }

        Ok(db_infos)
    }

    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply, MetaError> {
        todo!()
    }

    async fn drop_table(&self, _req: DropTableReq) -> Result<DropTableReply, MetaError> {
        todo!()
    }

    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply, MetaError> {
        todo!()
    }

    async fn get_table(&self, _req: GetTableReq) -> Result<Arc<TableInfo>, MetaError> {
        todo!()
    }

    async fn list_tables(&self, _req: ListTableReq) -> Result<Vec<Arc<TableInfo>>, MetaError> {
        todo!()
    }

    async fn get_table_by_id(
        &self,
        _table_id: MetaId,
    ) -> Result<(TableIdent, Arc<TableMeta>), MetaError> {
        todo!()
    }

    async fn upsert_table_option(
        &self,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply, MetaError> {
        todo!()
    }

    async fn create_share(&self, _req: CreateShareReq) -> Result<CreateShareReply, MetaError> {
        todo!()
    }

    async fn drop_share(&self, _req: DropShareReq) -> Result<DropShareReply, MetaError> {
        todo!()
    }
    async fn get_share(&self, _req: GetShareReq) -> Result<Arc<ShareInfo>, MetaError> {
        todo!()
    }

    fn name(&self) -> String {
        "MetaClientOnKV".to_string()
    }
}

// Supporting utils
impl MetaClientOnKV {
    /// Returns (db_id_seq, db_id, db_meta_seq, db_meta)
    async fn get_db_or_err(
        &self,
        name_key: &DatabaseNameIdent,
    ) -> Result<(u64, u64, u64, DatabaseMeta), MetaError> {
        let (db_id_seq, db_id) = self.get_db_id_by_name(name_key).await?;
        self.db_has_to_exist(db_id_seq, name_key)?;

        let id_key = DatabaseTenantIdIdent {
            tenant: name_key.tenant.clone(),
            db_id,
        };

        let (db_meta_seq, db_meta) = self.get_db_by_id(&id_key).await?;
        self.db_has_to_exist(db_meta_seq, name_key)?;

        Ok((db_id_seq, db_id, db_meta_seq, db_meta.unwrap()))
    }

    /// Get db id with seq, by name
    ///
    /// It returns (seq, db_id).
    /// If db is not found, (0,0) is returned.
    async fn get_db_id_by_name(
        &self,
        name_ident: &DatabaseNameIdent,
    ) -> Result<(u64, u64), MetaError> {
        let res = self.inner.get_kv(&name_ident.to_key()).await?;

        if let Some(seq_v) = res {
            Ok((seq_v.seq, self.deserialize_id(&seq_v.data)?))
        } else {
            Ok((0, 0))
        }
    }

    /// Get DatabaseMeta by database id.
    ///
    /// It returns seq number and the data.
    async fn get_db_by_id(
        &self,
        tid: &DatabaseTenantIdIdent,
    ) -> Result<(u64, Option<DatabaseMeta>), MetaError> {
        let res = self.inner.get_kv(&tid.to_key()).await?;

        if let Some(seq_v) = res {
            Ok((seq_v.seq, Some(self.deserialize_struct(&seq_v.data)?)))
        } else {
            Ok((0, None))
        }
    }

    /// Return OK if a db_id or db_meta exists by checking the seq.
    ///
    /// Otherwise returns UnknownDatabase error
    fn db_has_to_exist(&self, seq: u64, name_key: &DatabaseNameIdent) -> Result<(), MetaError> {
        if seq == 0 {
            tracing::debug!(seq, ?name_key, "db does not exist");

            Err(MetaError::AppError(AppError::UnknownDatabase(
                UnknownDatabase::new(
                    &name_key.db_name,
                    format!("drop db: tenant: {}", name_key.tenant),
                ),
            )))
        } else {
            Ok(())
        }
    }

    /// Generate an id on metasrv.
    ///
    /// Ids are categorized by generators.
    /// Ids may not be consecutive.
    async fn fetch_id<T: KVApiKey>(&self, generator: T) -> Result<u64, MetaError> {
        let res = self
            .inner
            .upsert_kv(UpsertKVAction {
                key: generator.to_key(),
                seq: MatchSeq::Any,
                value: Operation::Update(b"".to_vec()),
                value_meta: None,
            })
            .await?;

        // seq: MatchSeq::Any always succeeds
        let seq_v = res.result.unwrap();
        Ok(seq_v.seq)
    }

    /// Build a TxnCondition that compares the seq of a record.
    fn txn_cond_seq(
        &self,
        key: &impl KVApiKey,
        op: ConditionResult,
        seq: u64,
    ) -> Result<TxnCondition, MetaError> {
        let cond = TxnCondition {
            key: key.to_key(),
            expected: op as i32,
            target: Some(Target::Seq(seq)),
        };
        Ok(cond)
    }

    /// Build a txn operation that puts a record.
    fn txn_op_put(&self, key: &impl KVApiKey, value: Vec<u8>) -> Result<TxnOp, MetaError> {
        let put = TxnOp {
            request: Some(Request::Put(TxnPutRequest {
                key: key.to_key(),
                value,
                prev_value: true,
            })),
        };
        Ok(put)
    }

    /// Build a txn operation that deletes a record.
    fn txn_op_del(&self, key: &impl KVApiKey) -> Result<TxnOp, MetaError> {
        let put = TxnOp {
            request: Some(Request::Delete(TxnDeleteRequest {
                key: key.to_key(),
                prev_value: true,
            })),
        };
        Ok(put)
    }

    async fn send_txn(&self, txn_req: TxnRequest) -> Result<(bool, Vec<TxnOpResponse>), MetaError> {
        let tx_reply = self.inner.transaction(txn_req).await?;
        let res: Result<_, MetaError> = tx_reply.into();
        let (succ, responses) = res?;
        Ok((succ, responses))
    }

    fn serialize_id(&self, id: u64) -> Result<Vec<u8>, MetaError> {
        let v = serde_json::to_vec(&id).map_err(Self::meta_encode_err)?;
        Ok(v)
    }

    fn deserialize_id(&self, v: &[u8]) -> Result<u64, MetaError> {
        let id = serde_json::from_slice(v).map_err(Self::meta_encode_err)?;
        Ok(id)
    }

    fn serialize_struct<PB: common_protos::prost::Message>(
        &self,
        value: &impl FromToProto<PB>,
    ) -> Result<Vec<u8>, MetaError> {
        let p = value.to_pb().map_err(Self::meta_encode_err)?;
        let mut buf = vec![];
        common_protos::prost::Message::encode(&p, &mut buf).map_err(Self::meta_encode_err)?;
        Ok(buf)
    }

    fn deserialize_struct<PB, T>(&self, buf: &[u8]) -> Result<T, MetaError>
    where
        PB: common_protos::prost::Message + Default,
        T: FromToProto<PB>,
    {
        let p: PB = common_protos::prost::Message::decode(buf).map_err(Self::meta_encode_err)?;
        let v: T = FromToProto::from_pb(p).map_err(Self::meta_encode_err)?;

        Ok(v)
    }

    fn meta_encode_err<E: std::error::Error + 'static>(e: E) -> MetaError {
        MetaError::EncodeError(AnyError::new(&e))
    }
}
