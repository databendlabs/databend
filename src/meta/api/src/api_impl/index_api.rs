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

use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::DuplicatedIndexColumnId;
use databend_common_meta_app::app_error::IndexColumnIdNotFound;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::schema::CreateIndexReply;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetMarkedDeletedIndexesReply;
use databend_common_meta_app::schema::GetMarkedDeletedTableIndexesReply;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::IndexNameIdentRaw;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::index_id_ident::IndexId;
use databend_common_meta_app::schema::index_id_ident::IndexIdIdent;
use databend_common_meta_app::schema::index_id_to_name_ident::IndexIdToNameIdent;
use databend_common_meta_app::schema::index_name_ident::IndexName;
use databend_common_meta_app::schema::index_name_ident::IndexNameIdent;
use databend_common_meta_app::schema::marked_deleted_index_id::MarkedDeletedIndexId;
use databend_common_meta_app::schema::marked_deleted_index_ident::MarkedDeletedIndexIdIdent;
use databend_common_meta_app::schema::marked_deleted_table_index_id::MarkedDeletedTableIndexId;
use databend_common_meta_app::schema::marked_deleted_table_index_ident::MarkedDeletedTableIndexIdIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::UnknownError;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::Change;
use databend_meta_types::ConditionResult::Eq;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnOp;
use databend_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;
use uuid::Uuid;

use super::name_id_value_api::NameIdValueApi;
use super::schema_api::mark_index_as_deleted;
use super::schema_api::mark_table_index_as_deleted;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::serialize_struct;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_core_util::txn_delete_exact;
use crate::txn_del;
use crate::txn_op_builder_util::txn_put_pb_with_ttl;
use crate::txn_put_pb;

/// IndexApi defines APIs for index management and metadata.
///
/// This trait handles:
/// - Index lifecycle management (create, drop, update)
/// - Index metadata queries
/// - Table-specific index operations
/// - Index garbage collection
#[async_trait::async_trait]
pub trait IndexApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_ident = &req.name_ident;
        let meta = &req.meta;
        let overriding = req.create_option.is_overriding();
        let name_ident_raw = serialize_struct(&IndexNameIdentRaw::from(name_ident))?;

        let create_res = self
            .create_id_value(
                name_ident,
                meta,
                overriding,
                |id| {
                    vec![(
                        IndexIdToNameIdent::new_generic(name_ident.tenant(), id).to_string_key(),
                        name_ident_raw.clone(),
                    )]
                },
                |index_id, value| {
                    mark_index_as_deleted(name_ident.tenant(), value.table_id, *index_id)
                        .map(|(k, v)| vec![(k, v)])
                },
                |_, _| {},
            )
            .await?;

        match create_res {
            Ok(id) => Ok(CreateIndexReply { index_id: *id }),
            Err(existent) => match req.create_option {
                CreateOption::Create => {
                    Err(AppError::from(name_ident.exist_error(func_name!())).into())
                }
                CreateOption::CreateIfNotExists => Ok(CreateIndexReply {
                    index_id: *existent.data,
                }),
                CreateOption::CreateOrReplace => {
                    unreachable!(
                        "create_index: CreateOrReplace should never conflict with existent"
                    );
                }
            },
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_index(
        &self,
        name_ident: &IndexNameIdent,
    ) -> Result<Option<(SeqV<IndexId>, SeqV<IndexMeta>)>, MetaTxnError> {
        let mut trials = txn_backoff(None, func_name!());

        loop {
            trials.next().unwrap()?.await;
            let mut txn = TxnRequest::default();

            // remove name->id, id->meta, id->name
            let get_res = self.get_id_value(name_ident).await?;
            let Some((seq_id, seq_meta)) = get_res else {
                return Ok(None);
            };
            let id_ident = seq_id.data.into_t_ident(name_ident.tenant());
            txn_delete_exact(&mut txn, name_ident, seq_id.seq);
            txn_delete_exact(&mut txn, &id_ident, seq_meta.seq);
            txn.if_then.push(TxnOp::delete(
                IndexIdToNameIdent::new_generic(name_ident.tenant(), seq_id.data).to_string_key(),
            ));

            let (key, value) =
                mark_index_as_deleted(name_ident.tenant(), seq_meta.data.table_id, *seq_id.data)?;
            txn.if_then.push(TxnOp::put(key, value));

            let (succ, _responses) = send_txn(self, txn).await?;
            debug!(key :? =name_ident, id :? =&id_ident,succ = succ; "{}", func_name!());

            if succ {
                return Ok(Some((seq_id, seq_meta)));
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn update_index(
        &self,
        id_ident: IndexIdIdent,
        index_meta: IndexMeta,
    ) -> Result<Change<IndexMeta>, MetaError> {
        debug!(id_ident :? =(&id_ident); "SchemaApi: {}", func_name!());
        NameIdValueApi::<IndexNameIdent, _>::update_by_id(self, id_ident, index_meta).await
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_index(
        &self,
        name_ident: &IndexNameIdent,
    ) -> Result<Option<GetIndexReply>, MetaError> {
        debug!(req :? =name_ident; "SchemaApi: {}", func_name!());

        let res = self.get_id_value(name_ident).await?;

        let Some((seq_id, seq_meta)) = res else {
            return Ok(None);
        };

        Ok(Some(GetIndexReply {
            index_id: *seq_id.data,
            index_meta: seq_meta.data,
        }))
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_indexes(
        &self,
        req: ListIndexesReq,
    ) -> Result<Vec<(String, IndexId, IndexMeta)>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        // Get index id list by `prefix_list` "<prefix>/<tenant>"
        let ident = IndexNameIdent::new(&req.tenant, "dummy");
        let dir = DirName::new(ident);

        let name_id_metas = self.list_id_value(&dir).await?;

        let index_metas = name_id_metas
            // table_id is not specified
            // or table_id is specified and equals to the given table_id.
            .filter(|(_k, _seq_id, seq_meta)| {
                req.table_id.is_none() || req.table_id == Some(seq_meta.table_id)
            })
            .map(|(k, id_seqv, seq_meta)| (k.index_name().to_string(), id_seqv.data, seq_meta.data))
            .collect::<Vec<_>>();

        Ok(index_metas)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_marked_deleted_indexes(
        &self,
        tenant: &Tenant,
        table_id: Option<u64>,
    ) -> Result<GetMarkedDeletedIndexesReply, MetaError> {
        let dir = match table_id {
            Some(table_id) => {
                let ident = MarkedDeletedIndexIdIdent::new_generic(
                    tenant,
                    MarkedDeletedIndexId::new(table_id, 0),
                );
                DirName::new(ident)
            }
            None => {
                let ident =
                    MarkedDeletedIndexIdIdent::new_generic(tenant, MarkedDeletedIndexId::new(0, 0));
                DirName::new_with_level(ident, 2)
            }
        };
        let list_res = self.list_pb_vec(ListOptions::unlimited(&dir)).await?;
        let mut table_indexes = HashMap::new();
        for (k, v) in list_res {
            let table_id = k.name().table_id;
            let index_id = k.name().index_id;
            let index_meta = v.data;
            table_indexes
                .entry(table_id)
                .or_insert_with(Vec::new)
                .push((index_id, index_meta));
        }
        Ok(GetMarkedDeletedIndexesReply { table_indexes })
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_table_index(&self, req: CreateTableIndexReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tbid = TableId {
            table_id: req.table_id,
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_meta = self.get_pb(&tbid).await?.ok_or_else(|| {
                AppError::UnknownTableId(UnknownTableId::new(req.table_id, "create_table_index"))
            })?;

            debug!(ident :% =(&tbid); "create_table_index");

            let tb_meta_seq = seq_meta.seq;
            let mut table_meta = seq_meta.data;

            // update table indexes
            let indexes = &mut table_meta.indexes;
            if indexes.contains_key(&req.name) {
                match req.create_option {
                    CreateOption::Create => {
                        return Err(AppError::IndexAlreadyExists(
                            IndexNameIdent::new(&req.tenant, &req.name).exist_error(func_name!()),
                        )
                        .into());
                    }
                    CreateOption::CreateIfNotExists => {
                        return Ok(());
                    }
                    CreateOption::CreateOrReplace => {}
                }
            }
            // check the index column id exists
            for column_id in &req.column_ids {
                if table_meta.schema.is_column_deleted(*column_id) {
                    return Err(KVAppError::AppError(AppError::IndexColumnIdNotFound(
                        IndexColumnIdNotFound::new(*column_id, &req.name),
                    )));
                }
            }

            // column_id can not be duplicated
            for (name, index) in indexes.iter() {
                if *name == req.name || index.index_type != req.index_type {
                    continue;
                }
                for column_id in &req.column_ids {
                    if index.column_ids.contains(column_id) {
                        return Err(KVAppError::AppError(AppError::DuplicatedIndexColumnId(
                            DuplicatedIndexColumnId::new(*column_id, &req.name),
                        )));
                    }
                }
            }

            // If the column ids and options do not change,
            // use the old index version, otherwise create a new index version.
            let mut old_version = None;
            let mut mark_delete_op = None;
            if let Some(old_index) = indexes.get(&req.name) {
                if old_index.column_ids == req.column_ids && old_index.options == req.options {
                    old_version = Some(old_index.version.clone());
                } else {
                    let (m_key, m_value) = mark_table_index_as_deleted(
                        &req.tenant,
                        req.table_id,
                        &req.name,
                        &req.index_type,
                        &old_index.version,
                    )?;
                    mark_delete_op = Some(TxnOp::put(m_key, m_value));
                }
            }
            let version = old_version.unwrap_or(Uuid::new_v4().simple().to_string());

            let index = TableIndex {
                index_type: req.index_type.clone(),
                name: req.name.clone(),
                column_ids: req.column_ids.clone(),
                sync_creation: req.sync_creation,
                version,
                options: req.options.clone(),
            };
            indexes.insert(req.name.clone(), index);

            let mut txn_req = TxnRequest::new(
                //
                vec![txn_cond_eq_seq(&tbid, tb_meta_seq)],
                vec![
                    txn_put_pb_with_ttl(&tbid, &table_meta, None)?, // tb_id -> tb_meta
                ],
            );

            if let Some(mark_delete_op) = mark_delete_op {
                txn_req.if_then.push(mark_delete_op);
            }

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                id :? =(&tbid),
                succ = succ;
                "create_table_index"
            );

            if succ {
                return Ok(());
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_table_index(&self, req: DropTableIndexReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tbid = TableId {
            table_id: req.table_id,
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_meta = self.get_pb(&tbid).await?;

            debug!(ident :% =(&tbid); "drop_table_index");

            let Some(seq_meta) = seq_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "drop_table_index"),
                )));
            };

            let mut table_meta = seq_meta.data;
            // update table indexes
            let indexes = &mut table_meta.indexes;
            if !indexes.contains_key(&req.name) && !req.if_exists {
                return Err(KVAppError::AppError(AppError::UnknownIndex(
                    UnknownError::<IndexName>::new(
                        req.name.clone(),
                        format!("drop {} index", req.index_type),
                    ),
                )));
            }
            let Some(index) = indexes.remove(&req.name) else {
                return Ok(());
            };
            if index.index_type != req.index_type {
                return Err(KVAppError::AppError(AppError::UnknownIndex(
                    UnknownError::<IndexName>::new(
                        req.name.clone(),
                        format!(
                            "drop {} index, but the index is {}",
                            req.index_type, index.index_type
                        ),
                    ),
                )));
            }

            let (m_key, m_value) = mark_table_index_as_deleted(
                &req.tenant,
                req.table_id,
                &req.name,
                &req.index_type,
                &index.version,
            )?;

            let txn_req = TxnRequest::new(
                vec![
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, seq_meta.seq),
                ],
                vec![
                    txn_put_pb(&tbid, &table_meta)?, // tb_id -> tb_meta
                    TxnOp::put(m_key, m_value),
                ],
            );

            let (succ, _responses) = send_txn(self, txn_req).await?;
            debug!(id :? =(&tbid),succ = succ;"drop_table_index");

            if succ {
                return Ok(());
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_marked_deleted_table_indexes(
        &self,
        tenant: &Tenant,
        table_id: Option<u64>,
    ) -> Result<GetMarkedDeletedTableIndexesReply, MetaError> {
        let dir = match table_id {
            Some(table_id) => {
                let ident = MarkedDeletedTableIndexIdIdent::new_generic(
                    tenant,
                    MarkedDeletedTableIndexId::new(
                        table_id,
                        "dummy".to_string(),
                        "dummy".to_string(),
                    ),
                );
                DirName::new_with_level(ident, 2)
            }
            None => {
                let ident = MarkedDeletedTableIndexIdIdent::new_generic(
                    tenant,
                    MarkedDeletedTableIndexId::new(0, "dummy".to_string(), "dummy".to_string()),
                );
                DirName::new_with_level(ident, 3)
            }
        };
        let list_res = self.list_pb_vec(ListOptions::unlimited(&dir)).await?;
        let mut table_indexes = HashMap::new();
        for (k, v) in list_res {
            let table_id = k.name().table_id;
            let index_name = k.name().index_name.clone();
            let index_version = k.name().index_version.clone();
            let index_meta = v.data;
            table_indexes
                .entry(table_id)
                .or_insert_with(Vec::new)
                .push((index_name, index_version, index_meta));
        }
        Ok(GetMarkedDeletedTableIndexesReply { table_indexes })
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn remove_marked_deleted_index_ids(
        &self,
        tenant: &Tenant,
        table_id: u64,
        index_ids: &[u64],
    ) -> Result<(), MetaTxnError> {
        let mut trials = txn_backoff(None, func_name!());

        loop {
            trials.next().unwrap()?.await;
            let mut txn = TxnRequest::default();

            for index_id in index_ids {
                txn.if_then
                    .push(txn_del(&MarkedDeletedIndexIdIdent::new_generic(
                        tenant,
                        MarkedDeletedIndexId::new(table_id, *index_id),
                    )));
            }

            let (succ, _responses) = send_txn(self, txn).await?;

            if succ {
                return Ok(());
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn remove_marked_deleted_table_indexes(
        &self,
        tenant: &Tenant,
        table_id: u64,
        indexes: &[(String, String)],
    ) -> Result<(), MetaTxnError> {
        let mut trials = txn_backoff(None, func_name!());

        loop {
            trials.next().unwrap()?.await;
            let mut txn = TxnRequest::default();

            for (index_name, index_version) in indexes {
                txn.if_then
                    .push(txn_del(&MarkedDeletedTableIndexIdIdent::new_generic(
                        tenant,
                        MarkedDeletedTableIndexId::new(
                            table_id,
                            index_name.to_string(),
                            index_version.to_string(),
                        ),
                    )));
            }

            let (succ, _responses) = send_txn(self, txn).await?;

            if succ {
                return Ok(());
            }
        }
    }
}

#[async_trait::async_trait]
impl<KV> IndexApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
