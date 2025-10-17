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

use std::fmt::Display;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::DropTableWithDropTime;
use databend_common_meta_app::app_error::UndropTableAlreadyExists;
use databend_common_meta_app::app_error::UndropTableHasNoHistory;
use databend_common_meta_app::app_error::UndropTableRetentionGuard;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::schema::marked_deleted_index_id::MarkedDeletedIndexId;
use databend_common_meta_app::schema::marked_deleted_index_ident::MarkedDeletedIndexIdIdent;
use databend_common_meta_app::schema::marked_deleted_table_index_id::MarkedDeletedTableIndexId;
use databend_common_meta_app::schema::marked_deleted_table_index_ident::MarkedDeletedTableIndexIdIdent;
use databend_common_meta_app::schema::vacuum_watermark_ident::VacuumWatermarkIdent;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::MarkedDeletedIndexMeta;
use databend_common_meta_app::schema::MarkedDeletedIndexType;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdHistoryIdent;
use databend_common_meta_app::schema::TableIdList;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::UndropTableByIdReq;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;
use log::error;
use log::warn;
use ConditionResult::Eq;

use crate::catalog_api::CatalogApi;
use crate::data_retention_util::is_drop_time_retainable;
use crate::database_api::DatabaseApi;
use crate::database_util::get_db_or_err;
use crate::dictionary_api::DictionaryApi;
use crate::error_util::db_id_has_to_exist;
use crate::garbage_collection_api::GarbageCollectionApi;
use crate::get_u64_value;
use crate::index_api::IndexApi;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::lock_api::LockApi;
use crate::security_api::SecurityApi;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::table_api::TableApi;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_op_builder_util::txn_op_put_pb;
use crate::txn_op_del;
use crate::txn_op_put;

impl<KV> SchemaApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
    Self: CatalogApi,
    Self: DatabaseApi,
    Self: DictionaryApi,
    Self: GarbageCollectionApi,
    Self: IndexApi,
    Self: LockApi,
    Self: SecurityApi,
    Self: TableApi,
{
}

/// SchemaApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls SchemaApi.
// #[tonic::async_trait]
// impl<KV: kvapi::KVApi<Error = MetaError> + ?Sized> SchemaApi for KV {

#[async_trait::async_trait]
pub trait SchemaApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
    Self: CatalogApi,
    Self: DatabaseApi,
    Self: DictionaryApi,
    Self: GarbageCollectionApi,
    Self: IndexApi,
    Self: LockApi,
    Self: SecurityApi,
    Self: TableApi,
{
    // Pure trait composition - all methods moved to respective domain traits
}

pub async fn get_history_table_metas(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    include_non_retainable: bool,
    now: &DateTime<Utc>,
    tb_id_list: TableIdList,
) -> Result<Vec<(TableId, SeqV<TableMeta>)>, MetaError> {
    let mut tb_metas = vec![];

    let table_ids = tb_id_list.id_list.into_iter().map(TableId::new);

    let kvs = kv_api.get_pb_vec(table_ids).await?;

    for (k, table_meta) in kvs {
        let Some(table_meta) = table_meta else {
            error!("get_table_history cannot find {:?} table_meta", k);
            continue;
        };

        if include_non_retainable || is_drop_time_retainable(table_meta.drop_on, *now) {
            tb_metas.push((k, table_meta));
        }
    }

    Ok(tb_metas)
}

pub async fn construct_drop_table_txn_operations(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    table_name: String,
    tenant: &Tenant,
    catalog_name: Option<String>,
    table_id: u64,
    db_id: u64,
    if_exists: bool,
    if_delete: bool,
    txn: &mut TxnRequest,
) -> Result<(u64, u64), KVAppError> {
    let tbid = TableId { table_id };

    // Check if table exists.
    let (tb_meta_seq, tb_meta) = kv_api.get_pb_seq_and_value(&tbid).await?;
    if tb_meta_seq == 0 {
        return Err(KVAppError::AppError(AppError::UnknownTableId(
            UnknownTableId::new(table_id, "drop_table_by_id failed to find valid tb_meta"),
        )));
    }

    // Get db name, tenant name and related info for tx.
    let table_id_to_name = TableIdToName { table_id };
    let (_, table_name_opt) = kv_api.get_pb_seq_and_value(&table_id_to_name).await?;

    let dbid_tbname = if let Some(db_id_table_name) = table_name_opt {
        db_id_table_name
    } else {
        let dbid_tbname = DBIdTableName {
            db_id,
            table_name: table_name.clone(),
        };
        warn!(
            "drop_table_by_id cannot find {:?}, use {:?} instead",
            table_id_to_name, dbid_tbname
        );

        dbid_tbname
    };

    let db_id = dbid_tbname.db_id;
    let tbname = dbid_tbname.table_name.clone();
    let (tb_id_seq, _) = get_u64_value(kv_api, &dbid_tbname).await?;
    if tb_id_seq == 0 {
        return if if_exists {
            Ok((0, 0))
        } else {
            Err(KVAppError::AppError(AppError::UnknownTable(
                UnknownTable::new(tbname, "drop_table_by_id"),
            )))
        };
    }

    let (db_meta_seq, db_meta) = get_db_by_id_or_err(kv_api, db_id, "drop_table_by_id").await?;

    debug!(
        ident :% =(&tbid),
        tenant :% =(tenant.display());
        "drop table by id"
    );

    let mut tb_meta = tb_meta.unwrap();
    // drop a table with drop_on time
    if tb_meta.drop_on.is_some() {
        return if if_exists {
            Ok((0, 0))
        } else {
            Err(KVAppError::AppError(AppError::DropTableWithDropTime(
                DropTableWithDropTime::new(&dbid_tbname.table_name),
            )))
        };
    }

    tb_meta.drop_on = Some(Utc::now());

    // There must NOT be concurrent txn(b) that list-then-delete tables:
    // Otherwise, (b) may not delete all of the tables, if this txn(a) is operating on some table.
    // We guarantee there is no `(b)` so we do not have to assert db seq.
    txn.condition.extend(vec![
        // assert db_meta seq so that no other txn can delete this db
        txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta_seq),
        // table is not changed
        txn_cond_seq(&tbid, Eq, tb_meta_seq),
    ]);

    txn.if_then.extend(vec![
        // update db_meta seq so that no other txn can delete this db
        txn_op_put(&DatabaseId { db_id }, serialize_struct(&db_meta)?), // (db_id) -> db_meta
        txn_op_put(&tbid, serialize_struct(&tb_meta)?), // (tenant, db_id, tb_id) -> tb_meta
    ]);
    if if_delete {
        // still this table id
        txn.condition
            .push(txn_cond_seq(&dbid_tbname, Eq, tb_id_seq));
        // (db_id, tb_name) -> tb_id
        txn.if_then.push(txn_op_del(&dbid_tbname));
    }

    // add TableIdListKey if not exist
    if if_delete {
        // get table id list from _fd_table_id_list/db_id/table_name
        let dbid_tbname_idlist = TableIdHistoryIdent {
            database_id: db_id,
            table_name: dbid_tbname.table_name.clone(),
        };
        let (tb_id_list_seq, _tb_id_list_opt) =
            kv_api.get_pb_seq_and_value(&dbid_tbname_idlist).await?;
        if tb_id_list_seq == 0 {
            let mut tb_id_list = TableIdList::new();
            tb_id_list.append(table_id);

            warn!(
                "drop table:{:?}, table_id:{:?} has no TableIdList",
                dbid_tbname, table_id
            );

            txn.condition
                .push(txn_cond_seq(&dbid_tbname_idlist, Eq, tb_id_list_seq));
            txn.if_then.push(txn_op_put(
                &dbid_tbname_idlist,
                serialize_struct(&tb_id_list)?,
            ));
        }
    }

    // Clean up ownership if catalog_name is provided (CREATE OR REPLACE case)
    if let Some(catalog_name) = catalog_name {
        let ownership_object = OwnershipObject::Table {
            catalog_name,
            db_id,
            table_id,
        };
        let ownership_key = TenantOwnershipObjectIdent::new(tenant.clone(), ownership_object);
        txn.if_then.push(txn_op_del(&ownership_key));
    }

    Ok((tb_id_seq, table_id))
}

/// Returns (db_meta_seq, db_meta)
pub async fn get_db_by_id_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    db_id: u64,
    msg: impl Display,
) -> Result<(u64, DatabaseMeta), KVAppError> {
    let id_key = DatabaseId { db_id };

    let (db_meta_seq, db_meta) = kv_api.get_pb_seq_and_value(&id_key).await?;
    db_id_has_to_exist(db_meta_seq, db_id, msg)?;

    Ok((
        db_meta_seq,
        // Safe unwrap(): db_meta_seq > 0 implies db_meta is not None.
        db_meta.unwrap(),
    ))
}

pub fn build_upsert_table_deduplicated_label(deduplicated_label: String) -> TxnOp {
    TxnOp::put_with_ttl(
        deduplicated_label,
        1_i8.to_le_bytes().to_vec(),
        Some(Duration::from_secs(86400)),
    )
}

#[tonic::async_trait]
pub(crate) trait UndropTableStrategy {
    fn table_name_ident(&self) -> &TableNameIdent;

    // Determines whether replacing an existing table with the same name is allowed.
    fn force_replace(&self) -> bool;

    async fn refresh_target_db_meta<'a>(
        &'a self,
        kv_api: &'a (impl kvapi::KVApi<Error = MetaError> + ?Sized),
    ) -> Result<(u64, SeqV<DatabaseMeta>), KVAppError>;

    fn extract_and_validate_table_id(
        &self,
        tb_id_list: &mut TableIdList,
    ) -> Result<u64, KVAppError>;
}

#[tonic::async_trait]
impl UndropTableStrategy for UndropTableReq {
    fn table_name_ident(&self) -> &TableNameIdent {
        &self.name_ident
    }
    fn force_replace(&self) -> bool {
        false
    }
    async fn refresh_target_db_meta<'a>(
        &'a self,
        kv_api: &'a (impl kvapi::KVApi<Error = MetaError> + ?Sized),
    ) -> Result<(u64, SeqV<DatabaseMeta>), KVAppError> {
        // for plain un-drop table (by name), database meta is refreshed by name
        let (seq_db_id, db_meta) =
            get_db_or_err(kv_api, &self.name_ident.db_name_ident(), "undrop_table").await?;
        Ok((*seq_db_id.data, db_meta))
    }

    fn extract_and_validate_table_id(
        &self,
        tb_id_list: &mut TableIdList,
    ) -> Result<u64, KVAppError> {
        // for plain un-drop table (by name), the last item of
        // tb_id_list should be used.
        let table_id = match tb_id_list.last() {
            Some(table_id) => *table_id,
            None => {
                return Err(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                    UndropTableHasNoHistory::new(&self.name_ident.table_name),
                )));
            }
        };
        Ok(table_id)
    }
}

#[tonic::async_trait]
impl UndropTableStrategy for UndropTableByIdReq {
    fn table_name_ident(&self) -> &TableNameIdent {
        &self.name_ident
    }

    fn force_replace(&self) -> bool {
        self.force_replace
    }
    async fn refresh_target_db_meta<'a>(
        &'a self,
        kv_api: &'a (impl kvapi::KVApi<Error = MetaError> + ?Sized),
    ) -> Result<(u64, SeqV<DatabaseMeta>), KVAppError> {
        // for un-drop table by id, database meta is refreshed by database id
        let (db_meta_seq, db_meta) =
            get_db_by_id_or_err(kv_api, self.db_id, "undrop_table_by_id").await?;
        Ok((self.db_id, SeqV::new(db_meta_seq, db_meta)))
    }

    fn extract_and_validate_table_id(
        &self,
        tb_id_list: &mut TableIdList,
    ) -> Result<u64, KVAppError> {
        // for un-drop table by id, assumes that the last item of tb_id_list should
        // be the table id which is requested to be un-dropped.
        let target_table_id = self.table_id;
        match tb_id_list.last() {
            Some(table_id) if *table_id == target_table_id => Ok(target_table_id),
            _ => Err(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                UndropTableHasNoHistory::new(&self.name_ident.table_name),
            ))),
        }
    }
}

pub async fn handle_undrop_table(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    req: impl UndropTableStrategy + std::fmt::Debug,
) -> Result<(), KVAppError> {
    let tenant_dbname_tbname = req.table_name_ident();

    let mut trials = txn_backoff(None, func_name!());
    loop {
        trials.next().unwrap()?.await;

        // Get db by name to ensure presence

        let (db_id, seq_db_meta) = req.refresh_target_db_meta(kv_api).await?;

        // Get table by tenant,db_id, table_name to assert presence.

        let dbid_tbname = DBIdTableName {
            db_id,
            table_name: tenant_dbname_tbname.table_name.clone(),
        };

        let (dbid_tbname_seq, table_id) = get_u64_value(kv_api, &dbid_tbname).await?;
        if !req.force_replace() {
            // If table id already exists, return error.
            if dbid_tbname_seq > 0 || table_id > 0 {
                return Err(KVAppError::AppError(AppError::UndropTableAlreadyExists(
                    UndropTableAlreadyExists::new(&tenant_dbname_tbname.table_name),
                )));
            }
        }

        // get table id list from _fd_table_id_list/db_id/table_name
        let dbid_tbname_idlist = TableIdHistoryIdent {
            database_id: db_id,
            table_name: tenant_dbname_tbname.table_name.clone(),
        };

        let Some(seq_list) = kv_api.get_pb(&dbid_tbname_idlist).await? else {
            return Err(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                UndropTableHasNoHistory::new(&tenant_dbname_tbname.table_name),
            )));
        };
        let mut tb_id_list = seq_list.data;

        let table_id = req.extract_and_validate_table_id(&mut tb_id_list)?;

        // get tb_meta of the last table id
        let tbid = TableId { table_id };
        let seq_table_meta = kv_api.get_pb(&tbid).await?;
        let Some(mut seq_table_meta) = seq_table_meta else {
            return Err(
                AppError::from(UnknownTableId::new(tbid.table_id, "when undrop table")).into(),
            );
        };

        debug!(
            ident :% =(&tbid),
            name :% =(tenant_dbname_tbname);
            "undrop table"
        );

        // Check vacuum retention guard before allowing undrop
        let drop_marker = *seq_table_meta
            .data
            .drop_on
            .as_ref()
            .unwrap_or(&seq_table_meta.data.updated_on);

        // Read vacuum timestamp with seq for concurrent safety
        let vacuum_ident = VacuumWatermarkIdent::new_global(tenant_dbname_tbname.tenant().clone());
        let seq_vacuum_retention = kv_api.get_pb(&vacuum_ident).await?;

        // Early retention guard check for fast failure
        if let Some(ref sr) = seq_vacuum_retention {
            let retention_time = sr.data.time;

            if drop_marker <= retention_time {
                return Err(KVAppError::AppError(AppError::UndropTableRetentionGuard(
                    UndropTableRetentionGuard::new(
                        &tenant_dbname_tbname.table_name,
                        drop_marker,
                        retention_time,
                    ),
                )));
            }
        }

        {
            // reset drop on time
            seq_table_meta.drop_on = None;

            // Prepare conditions for concurrent safety
            let vacuum_seq = seq_vacuum_retention.as_ref().map(|sr| sr.seq).unwrap_or(0);

            let txn = TxnRequest::new(
                vec![
                    // db has not to change, i.e., no new table is created.
                    // Renaming db is OK and does not affect the seq of db_meta.
                    txn_cond_eq_seq(&DatabaseId { db_id }, seq_db_meta.seq),
                    // still this table id
                    txn_cond_eq_seq(&dbid_tbname, dbid_tbname_seq),
                    // table is not changed
                    txn_cond_eq_seq(&tbid, seq_table_meta.seq),
                    // Concurrent safety: vacuum timestamp seq must not change during undrop
                    // - If vacuum_retention exists: seq must remain the same (no update by vacuum)
                    // - If vacuum_retention is None: seq must remain 0 (no creation by vacuum)
                    txn_cond_eq_seq(&vacuum_ident, vacuum_seq),
                ],
                vec![
                    // Changing a table in a db has to update the seq of db_meta,
                    // to block the batch-delete-tables when deleting a db.
                    txn_op_put_pb(&DatabaseId { db_id }, &seq_db_meta.data, None)?, /* (db_id) -> db_meta */
                    txn_op_put(&dbid_tbname, serialize_u64(table_id)?), /* (tenant, db_id, tb_name) -> tb_id */
                    txn_op_put_pb(&tbid, &seq_table_meta.data, None)?, /* (tenant, db_id, tb_id) -> tb_meta */
                ],
            );

            let (succ, _responses) = send_txn(kv_api, txn).await?;

            debug!(
                name :? =(tenant_dbname_tbname),
                id :? =(&tbid),
                succ = succ;
                "undrop_table"
            );

            if succ {
                return Ok(());
            }
        }
    }
}

/// add __fd_marked_deleted_index/<table_id>/<index_id> -> marked_deleted_index_meta
pub fn mark_index_as_deleted(
    tenant: &Tenant,
    table_id: u64,
    index_id: u64,
) -> Result<(String, Vec<u8>), MetaError> {
    let marked_deleted_index_id_ident = MarkedDeletedIndexIdIdent::new_generic(
        tenant,
        MarkedDeletedIndexId::new(table_id, index_id),
    );
    let marked_deleted_index_meta = MarkedDeletedIndexMeta {
        dropped_on: Utc::now(),
        index_type: MarkedDeletedIndexType::AGGREGATING,
    };

    Ok((
        marked_deleted_index_id_ident.to_string_key(),
        serialize_struct(&marked_deleted_index_meta)?,
    ))
}

/// add __fd_marked_deleted_table_index/<table_id>/<index_name>/<index_version> -> marked_deleted_table_index_meta
pub fn mark_table_index_as_deleted(
    tenant: &Tenant,
    table_id: u64,
    index_name: &str,
    index_type: &TableIndexType,
    index_version: &str,
) -> Result<(String, Vec<u8>), MetaError> {
    let marked_deleted_table_index_id_ident = MarkedDeletedTableIndexIdIdent::new_generic(
        tenant,
        MarkedDeletedTableIndexId::new(table_id, index_name.to_owned(), index_version.to_owned()),
    );
    let deleted_index_type = match index_type {
        TableIndexType::Inverted => MarkedDeletedIndexType::INVERTED,
        TableIndexType::Ngram => MarkedDeletedIndexType::NGRAM,
        TableIndexType::Vector => MarkedDeletedIndexType::VECTOR,
    };
    let marked_deleted_table_index_meta = MarkedDeletedIndexMeta {
        dropped_on: Utc::now(),
        index_type: deleted_index_type,
    };

    Ok((
        marked_deleted_table_index_id_ident.to_string_key(),
        serialize_struct(&marked_deleted_table_index_meta)?,
    ))
}
