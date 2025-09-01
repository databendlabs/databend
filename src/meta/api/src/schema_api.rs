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

use std::any::type_name;
use std::convert::Infallible;
use std::fmt::Display;
use std::ops::Range;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use databend_common_base::vec_ext::VecExt;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::DropTableWithDropTime;
use databend_common_meta_app::app_error::TableAlreadyExists;
use databend_common_meta_app::app_error::UndropTableAlreadyExists;
use databend_common_meta_app::app_error::UndropTableHasNoHistory;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::index_id_ident::IndexIdIdent;
use databend_common_meta_app::schema::index_id_to_name_ident::IndexIdToNameIdent;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_common_meta_app::schema::marked_deleted_index_id::MarkedDeletedIndexId;
use databend_common_meta_app::schema::marked_deleted_index_ident::MarkedDeletedIndexIdIdent;
use databend_common_meta_app::schema::marked_deleted_table_index_id::MarkedDeletedTableIndexId;
use databend_common_meta_app::schema::marked_deleted_table_index_ident::MarkedDeletedTableIndexIdIdent;
use databend_common_meta_app::schema::table_niv::TableNIV;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseIdHistoryIdent;
use databend_common_meta_app::schema::DatabaseIdToName;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::IndexNameIdent;
use databend_common_meta_app::schema::LeastVisibleTime;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_app::schema::MarkedDeletedIndexMeta;
use databend_common_meta_app::schema::MarkedDeletedIndexType;
use databend_common_meta_app::schema::TableCopiedFileNameIdent;
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
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use databend_common_proto_conv::FromToProto;
use display_more::DisplaySliceExt;
use fastrace::func_name;
use futures::StreamExt;
use futures::TryStreamExt;
use log::debug;
use log::error;
use log::info;
use log::warn;
use seq_marked::SeqValue;
use ConditionResult::Eq;

use crate::catalog_api::CatalogApi;
use crate::database_api::DatabaseApi;
use crate::database_util::get_db_or_err;
use crate::dictionary_api::DictionaryApi;
use crate::get_u64_value;
use crate::index_api::IndexApi;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_crud_api::KVPbCrudApi;
use crate::lock_api::LockApi;
use crate::meta_txn_error::MetaTxnError;
use crate::security_api::SecurityApi;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::table_api::TableApi;
use crate::txn_backoff::txn_backoff;
use crate::txn_cond_eq_seq;
use crate::txn_cond_seq;
use crate::txn_op_del;
use crate::txn_op_put;
use crate::util::db_id_has_to_exist;
use crate::util::is_drop_time_retainable;
use crate::util::txn_delete_exact;
use crate::util::txn_op_put_pb;
use crate::util::txn_put_pb;
use crate::util::unknown_database_error;

pub const ORPHAN_POSTFIX: &str = "orphan";

impl<KV> SchemaApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
    Self: CatalogApi,
    Self: DatabaseApi,
    Self: DictionaryApi,
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
    Self: IndexApi,
    Self: LockApi,
    Self: SecurityApi,
    Self: TableApi,
{
    #[fastrace::trace]
    async fn gc_drop_tables(&self, req: GcDroppedTableReq) -> Result<(), KVAppError> {
        for drop_id in req.drop_ids {
            match drop_id {
                DroppedId::Db { db_id, db_name } => {
                    gc_dropped_db_by_id(self, db_id, &req.tenant, &req.catalog, db_name).await?
                }
                DroppedId::Table { name, id } => {
                    gc_dropped_table_by_id(self, &req.tenant, &req.catalog, &name, &id).await?
                }
            }
        }
        Ok(())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn set_table_lvt(
        &self,
        name_ident: &LeastVisibleTimeIdent,
        value: &LeastVisibleTime,
    ) -> Result<LeastVisibleTime, KVAppError> {
        debug!(req :? =(&name_ident, &value); "SchemaApi: {}", func_name!());

        let transition = self
            .crud_upsert_with::<Infallible>(name_ident, |t: Option<SeqV<LeastVisibleTime>>| {
                let curr = t.into_value().unwrap_or_default();
                if curr.time >= value.time {
                    Ok(None)
                } else {
                    Ok(Some(value.clone()))
                }
            })
            .await?;

        return Ok(transition.unwrap().result.into_value().unwrap_or_default());
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get<K>(&self, name_ident: &K) -> Result<Option<K::ValueType>, MetaError>
    where
        K: kvapi::Key + Sync + 'static,
        K::ValueType: FromToProto + 'static,
    {
        debug!(req :? =(&name_ident); "SchemaApi::get::<{}>()", typ::<K>());

        let seq_lvt = self.get_pb(name_ident).await?;
        Ok(seq_lvt.into_value())
    }

    fn name(&self) -> String {
        "SchemaApiImpl".to_string()
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
                    .push(txn_op_del(&MarkedDeletedIndexIdIdent::new_generic(
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
                    .push(txn_op_del(&MarkedDeletedTableIndexIdIdent::new_generic(
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
    Ok((tb_id_seq, table_id))
}

/// Remove copied files for a dropped table.
///
/// Dropped table can not be accessed by any query,
/// so it is safe to remove all the copied files in multiple sub transactions.
async fn remove_copied_files_for_dropped_table(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    table_id: &TableId,
) -> Result<(), MetaError> {
    let batch_size = 1024;

    // Loop until:
    // - all cleaned
    // - or table is removed from meta-service
    // - or is no longer in `droppped` state.
    for i in 0..usize::MAX {
        let mut txn = TxnRequest::default();

        let seq_meta = kv_api.get_pb(table_id).await?;
        let Some(seq_table_meta) = seq_meta else {
            return Ok(());
        };

        // TODO: enable this check. Currently when gc db, the table may not be dropped.
        // if seq_table_meta.data.drop_on.is_none() {
        //     return Ok(());
        // }

        // Make sure the table meta is not changed, such as being un-dropped.
        txn.condition
            .push(txn_cond_eq_seq(table_id, seq_table_meta.seq));

        let copied_file_ident = TableCopiedFileNameIdent {
            table_id: table_id.table_id,
            file: "dummy".to_string(),
        };

        let dir_name = DirName::new(copied_file_ident);

        let key_stream = kv_api.list_pb_keys(&dir_name).await?;
        let copied_files = key_stream.take(batch_size).try_collect::<Vec<_>>().await?;

        if copied_files.is_empty() {
            return Ok(());
        }

        for copied_ident in copied_files.iter() {
            // It is a dropped table, thus there is no data will be written to the table.
            // Therefore, we only need to assert the table_meta seq, and there is no need to assert
            // seq of each copied file.
            txn.if_then.push(txn_op_del(copied_ident));
        }

        info!(
            "remove_copied_files_for_dropped_table {}: {}-th batch remove: {} items: {}",
            table_id,
            i,
            copied_files.len(),
            copied_files.display()
        );

        send_txn(kv_api, txn).await?;
    }
    unreachable!()
}

/// Get db id and its seq by name, returns (db_id_seq, db_id)
///
/// If the db does not exist, returns AppError::UnknownDatabase
pub(crate) async fn get_db_id_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &DatabaseNameIdent,
    msg: impl Display,
) -> Result<SeqV<DatabaseId>, KVAppError> {
    let seq_db_id = kv_api.get_pb(name_key).await?;

    let seq_db_id = seq_db_id.ok_or_else(|| unknown_database_error(name_key, msg))?;

    Ok(seq_db_id.map(|x| x.into_inner()))
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

/// Return OK if a table_id or table_meta does not exist by checking the seq.
///
/// Otherwise returns TableAlreadyExists error
pub fn table_has_to_not_exist(
    seq: u64,
    name_ident: &TableNameIdent,
    ctx: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        Ok(())
    } else {
        debug!(seq = seq, name_ident :? =(name_ident); "exist");

        Err(KVAppError::AppError(AppError::TableAlreadyExists(
            TableAlreadyExists::new(&name_ident.table_name, format!("{}: {}", ctx, name_ident)),
        )))
    }
}

pub fn build_upsert_table_deduplicated_label(deduplicated_label: String) -> TxnOp {
    TxnOp::put_with_ttl(
        deduplicated_label,
        1_i8.to_le_bytes().to_vec(),
        Some(Duration::from_secs(86400)),
    )
}

/// Lists all dropped and non-dropped tables belonging to a Database,
/// returns those tables that are eligible for garbage collection,
/// i.e., whose dropped time is in the specified range.
#[logcall::logcall(input = "")]
#[fastrace::trace]
pub async fn get_history_tables_for_gc(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    drop_time_range: Range<Option<DateTime<Utc>>>,
    db_id: u64,
    limit: usize,
) -> Result<Vec<TableNIV>, KVAppError> {
    let ident = TableIdHistoryIdent {
        database_id: db_id,
        table_name: "dummy".to_string(),
    };
    let dir_name = DirName::new(ident);
    let table_history_kvs = kv_api.list_pb_vec(&dir_name).await?;

    let mut args = vec![];

    for (ident, table_history) in table_history_kvs {
        for table_id in table_history.id_list.iter() {
            args.push((TableId::new(*table_id), ident.table_name.clone()));
        }
    }

    let mut filter_tb_infos = vec![];
    const BATCH_SIZE: usize = 1000;

    // Process in batches to avoid performance issues
    for chunk in args.chunks(BATCH_SIZE) {
        // Get table metadata for current batch
        let table_id_idents = chunk.iter().map(|(table_id, _)| table_id.clone());
        let seq_metas = kv_api.get_pb_values_vec(table_id_idents).await?;

        // Filter by drop_time_range for current batch
        for (seq_meta, (table_id, table_name)) in seq_metas.into_iter().zip(chunk.iter()) {
            let Some(seq_meta) = seq_meta else {
                error!(
                    "batch_filter_table_info cannot find {:?} table_meta",
                    table_id
                );
                continue;
            };

            if !drop_time_range.contains(&seq_meta.data.drop_on) {
                info!("table {:?} is not in drop_time_range", seq_meta.data);
                continue;
            }

            filter_tb_infos.push(TableNIV::new(
                DBIdTableName::new(db_id, table_name.clone()),
                table_id.clone(),
                seq_meta,
            ));

            // Check if we have reached the limit
            if filter_tb_infos.len() >= limit {
                return Ok(filter_tb_infos);
            }
        }
    }

    Ok(filter_tb_infos)
}

/// Permanently remove a dropped database from the meta-service.
///
/// Upon calling this method, the dropped database must be already marked as `gc_in_progress`,
/// then remove all **dropped and non-dropped** tables in the database.
async fn gc_dropped_db_by_id(
    kv_api: &(impl SchemaApi + ?Sized),
    db_id: u64,
    tenant: &Tenant,
    catalog: &String,
    db_name: String,
) -> Result<(), KVAppError> {
    // List tables by tenant, db_id, table_name.
    let db_id_history_ident = DatabaseIdHistoryIdent::new(tenant, db_name);
    let Some(seq_dbid_list) = kv_api.get_pb(&db_id_history_ident).await? else {
        return Ok(());
    };

    let mut db_id_list = seq_dbid_list.data;

    // If the db_id is not in the list, return.
    if db_id_list.id_list.remove_first(&db_id).is_none() {
        return Ok(());
    }

    let dbid = DatabaseId { db_id };
    let Some(seq_db_meta) = kv_api.get_pb(&dbid).await? else {
        return Ok(());
    };

    if seq_db_meta.drop_on.is_none() {
        // If db is not marked as dropped, just ignore the gc request and return directly.
        // In subsequent KV transactions, we also verify that db_meta hasn't changed
        // to ensure we don't reclaim metadata of the given database that might have been
        // successfully undropped in a parallel operation.
        return Ok(());
    }

    // TODO: enable this when gc_in_progress is set.
    // if !seq_db_meta.gc_in_progress {
    //     let err = UnknownDatabaseId::new(
    //         db_id,
    //         "database is not in gc_in_progress state, \
    //         can not gc. \
    //         First mark the database as gc_in_progress, \
    //         then gc is allowed.",
    //     );
    //     return Err(AppError::from(err).into());
    // }

    let id_to_name = DatabaseIdToName { db_id };
    let Some(seq_name) = kv_api.get_pb(&id_to_name).await? else {
        return Ok(());
    };

    let table_history_ident = TableIdHistoryIdent {
        database_id: db_id,
        table_name: "dummy".to_string(),
    };
    let dir_name = DirName::new(table_history_ident);

    let table_history_items = kv_api.list_pb_vec(&dir_name).await?;

    let mut txn = TxnRequest::default();

    for (ident, table_history) in table_history_items {
        for tb_id in table_history.id_list.iter() {
            let table_id_ident = TableId { table_id: *tb_id };

            // TODO: mark table as gc_in_progress

            remove_copied_files_for_dropped_table(kv_api, &table_id_ident).await?;
            let _ = remove_data_for_dropped_table(
                kv_api,
                tenant,
                catalog,
                db_id,
                &table_id_ident,
                &mut txn,
            )
            .await?;
            remove_index_for_dropped_table(kv_api, tenant, &table_id_ident, &mut txn).await?;
        }

        txn.condition
            .push(txn_cond_eq_seq(&ident, table_history.seq));
        txn.if_then.push(txn_op_del(&ident));
    }

    txn.condition
        .push(txn_cond_eq_seq(&db_id_history_ident, seq_dbid_list.seq));
    if db_id_list.id_list.is_empty() {
        txn.if_then.push(txn_op_del(&db_id_history_ident));
    } else {
        // save new db id list
        txn.if_then
            .push(txn_put_pb(&db_id_history_ident, &db_id_list)?);
    }

    // Verify db_meta hasn't changed since we started this operation.
    // This establishes a condition for the transaction that will prevent it from committing
    // if the database metadata was modified by another concurrent operation (like un-dropping).
    txn.condition.push(txn_cond_eq_seq(&dbid, seq_db_meta.seq));
    txn.if_then.push(txn_op_del(&dbid));
    txn.condition
        .push(txn_cond_eq_seq(&id_to_name, seq_name.seq));
    txn.if_then.push(txn_op_del(&id_to_name));

    let _resp = kv_api.transaction(txn).await?;

    Ok(())
}

/// Permanently remove a dropped table from the meta-service.
///
/// The data of the table should already have been removed before calling this method.
async fn gc_dropped_table_by_id(
    kv_api: &(impl SchemaApi + ?Sized),
    tenant: &Tenant,
    catalog: &String,
    db_id_table_name: &DBIdTableName,
    table_id_ident: &TableId,
) -> Result<(), KVAppError> {
    // First remove all copied files for the dropped table.
    // These markers are not part of the table and can be removed in separate transactions.
    remove_copied_files_for_dropped_table(kv_api, table_id_ident).await?;

    let mut trials = txn_backoff(None, func_name!());
    loop {
        trials.next().unwrap()?.await;

        let mut txn = TxnRequest::default();

        // 1)
        let _ = remove_data_for_dropped_table(
            kv_api,
            tenant,
            catalog,
            db_id_table_name.db_id,
            table_id_ident,
            &mut txn,
        )
        .await?;

        // 2)
        let table_id_history_ident = TableIdHistoryIdent {
            database_id: db_id_table_name.db_id,
            table_name: db_id_table_name.table_name.clone(),
        };

        update_txn_to_remove_table_history(
            kv_api,
            &mut txn,
            &table_id_history_ident,
            table_id_ident,
        )
        .await?;

        // 3)
        remove_index_for_dropped_table(kv_api, tenant, table_id_ident, &mut txn).await?;

        let (succ, _responses) = send_txn(kv_api, txn).await?;

        if succ {
            return Ok(());
        }
    }
}

/// Fill in condition and operations to TxnRequest to remove table history.
///
/// If the table history does not exist or does not include the table id, do nothing.
///
/// This function does not submit the txn.
async fn update_txn_to_remove_table_history(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    txn: &mut TxnRequest,
    table_id_history_ident: &TableIdHistoryIdent,
    table_id_ident: &TableId,
) -> Result<(), MetaError> {
    let seq_id_list = kv_api.get_pb(table_id_history_ident).await?;

    let Some(seq_id_list) = seq_id_list else {
        return Ok(());
    };

    let seq = seq_id_list.seq;
    let mut history = seq_id_list.data;

    // remove table_id from tb_id_list:
    {
        let table_id = table_id_ident.table_id;
        let pos = history.id_list.iter().position(|&x| x == table_id);
        let Some(index) = pos else {
            return Ok(());
        };

        history.id_list.remove(index);
    }

    // construct the txn request
    txn.condition.push(
        // condition: table id list not changed
        txn_cond_eq_seq(table_id_history_ident, seq),
    );

    if history.id_list.is_empty() {
        txn.if_then.push(txn_op_del(table_id_history_ident));
    } else {
        // save new table id list
        txn.if_then
            .push(txn_op_put_pb(table_id_history_ident, &history, None)?);
    }

    Ok(())
}

/// Update TxnRequest to remove a dropped table's own data.
///
/// This function returns the updated TxnRequest,
/// or Err of the reason in string if it can not proceed.
async fn remove_data_for_dropped_table(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    tenant: &Tenant,
    catalog: &String,
    db_id: u64,
    table_id: &TableId,
    txn: &mut TxnRequest,
) -> Result<Result<(), String>, MetaError> {
    let seq_meta = kv_api.get_pb(table_id).await?;

    let Some(seq_meta) = seq_meta else {
        let err = format!("cannot find TableMeta by id: {:?}, ", table_id);
        error!("{}", err);
        return Ok(Err(err));
    };

    // TODO: enable this check. Currently when gc db, the table may not be dropped.
    // if seq_meta.data.drop_on.is_none() {
    //     let err = format!("Table {:?} is not dropped, can not remove", table_id);
    //     warn!("{}", err);
    //     return Ok(Err(err));
    // }

    txn_delete_exact(txn, table_id, seq_meta.seq);

    // Get id -> name mapping
    let id_to_name = TableIdToName {
        table_id: table_id.table_id,
    };
    let seq_name = kv_api.get_pb(&id_to_name).await?;

    // consider only when TableIdToName exist
    if let Some(seq_name) = seq_name {
        txn_delete_exact(txn, &id_to_name, seq_name.seq);
    }

    // Remove table ownership
    {
        let table_ownership = OwnershipObject::Table {
            // if catalog is default, encode_key is b.push_raw("table-by-id").push_u64(*table_id)
            // else encode_key is b.push_raw("table-by-catalog-id").push_str(catalog_name).push_u64(*table_id)
            catalog_name: catalog.to_string(),
            db_id,
            table_id: table_id.table_id,
        };

        let table_ownership_key = TenantOwnershipObjectIdent::new(tenant, table_ownership);
        let table_ownership_seq_meta = {
            let seq_meta = kv_api.get_pb(&table_ownership_key).await?;
            let Some(seq_meta) = seq_meta else {
                let err = format!(
                    "cannot find OwnershipInfo of object: {:?}, ",
                    table_ownership_key.to_string_key()
                );
                error!("{}", err);
                return Ok(Err(err));
            };
            seq_meta
        };

        txn_delete_exact(txn, &table_ownership_key, table_ownership_seq_meta.seq);
    }

    Ok(Ok(()))
}

async fn remove_index_for_dropped_table(
    kv_api: &(impl SchemaApi + ?Sized),
    tenant: &Tenant,
    table_id: &TableId,
    txn: &mut TxnRequest,
) -> Result<(), KVAppError> {
    let name_id_metas = kv_api
        .list_indexes(ListIndexesReq {
            tenant: tenant.clone(),
            table_id: Some(table_id.table_id),
        })
        .await?;

    for (name, index_id, _) in name_id_metas {
        let name_ident = IndexNameIdent::new_generic(tenant, name);
        let id_ident = IndexIdIdent::new_generic(tenant, index_id);
        let id_to_name_ident = IndexIdToNameIdent::new_generic(tenant, index_id);

        txn.if_then.push(txn_op_del(&name_ident)); // (tenant, index_name) -> index_id
        txn.if_then.push(txn_op_del(&id_ident)); // (index_id) -> index_meta
        txn.if_then.push(txn_op_del(&id_to_name_ident)); // __fd_index_id_to_name/<index_id> -> (tenant,index_name)
    }

    Ok(())
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

        {
            // reset drop on time
            seq_table_meta.drop_on = None;

            let txn = TxnRequest::new(
                vec![
                    // db has not to change, i.e., no new table is created.
                    // Renaming db is OK and does not affect the seq of db_meta.
                    txn_cond_eq_seq(&DatabaseId { db_id }, seq_db_meta.seq),
                    // still this table id
                    txn_cond_eq_seq(&dbid_tbname, dbid_tbname_seq),
                    // table is not changed
                    txn_cond_eq_seq(&tbid, seq_table_meta.seq),
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

fn typ<K>() -> &'static str {
    type_name::<K>()
        .rsplit("::")
        .next()
        .unwrap_or("UnknownType")
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
