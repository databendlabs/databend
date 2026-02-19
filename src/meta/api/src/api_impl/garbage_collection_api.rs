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
use std::convert::Infallible;
use std::ops::Range;

use chrono::DateTime;
use chrono::Utc;
use databend_common_base::vec_ext::VecExt;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::CleanDbIdTableNamesFailed;
use databend_common_meta_app::app_error::MarkDatabaseMetaAsGCInProgressFailed;
use databend_common_meta_app::data_mask::MaskPolicyIdTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableIdIdent;
use databend_common_meta_app::principal::AutoIncrementKey;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableIdIdent;
use databend_common_meta_app::row_access_policy::row_access_policy_table_id_ident::RowAccessPolicyIdTableId;
use databend_common_meta_app::schema::AutoIncrementStorageIdent;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseIdHistoryIdent;
use databend_common_meta_app::schema::DatabaseIdToName;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::IndexNameIdent;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_app::schema::ObjectTagIdRef;
use databend_common_meta_app::schema::ObjectTagIdRefIdent;
use databend_common_meta_app::schema::TableCopiedFileNameIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdHistoryIdent;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TagIdObjectRef;
use databend_common_meta_app::schema::TagIdObjectRefIdent;
use databend_common_meta_app::schema::TaggableObject;
use databend_common_meta_app::schema::VacuumWatermark;
use databend_common_meta_app::schema::index_id_ident::IndexIdIdent;
use databend_common_meta_app::schema::index_id_to_name_ident::IndexIdToNameIdent;
use databend_common_meta_app::schema::table_niv::TableNIV;
use databend_common_meta_app::schema::vacuum_watermark_ident::VacuumWatermarkIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnRequest;
use databend_meta_types::txn_op::Request;
use databend_meta_types::txn_op_response::Response;
use display_more::DisplaySliceExt;
use fastrace::func_name;
use futures::StreamExt;
use futures::TryStreamExt;
use log::debug;
use log::error;
use log::info;
use log::warn;

use super::index_api::IndexApi;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_crud_api::KVPbCrudApi;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_core_util::send_txn;
use crate::txn_core_util::txn_delete_exact;
use crate::txn_core_util::txn_replace_exact;
use crate::txn_del;
use crate::txn_get;
use crate::txn_op_builder_util::txn_put_pb;
use crate::txn_op_builder_util::txn_put_pb_with_ttl;

/// GarbageCollectionApi defines APIs for garbage collection operations.
///
/// This trait handles:
/// - Garbage collection of dropped tables and databases
#[async_trait::async_trait]
pub trait GarbageCollectionApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    /// Garbage collect dropped tables.
    ///
    /// Returns the approximate number of metadata keys removed.
    /// Note: DeleteByPrefix operations count as 1 but may remove multiple keys.
    #[fastrace::trace]
    async fn gc_drop_tables(&self, req: GcDroppedTableReq) -> Result<usize, KVAppError> {
        let mut num_meta_key_removed = 0;
        for drop_id in req.drop_ids {
            match drop_id {
                DroppedId::Db { db_id, db_name } => {
                    num_meta_key_removed +=
                        gc_dropped_db_by_id(self, db_id, &req.tenant, &req.catalog, db_name).await?
                }
                DroppedId::Table { name, id } => {
                    num_meta_key_removed +=
                        gc_dropped_table_by_id(self, &req.tenant, &req.catalog, &name, &id).await?
                }
            }
        }
        Ok(num_meta_key_removed)
    }

    /// Fetch and conditionally set the vacuum retention timestamp.
    ///
    /// This method implements the monotonic timestamp update semantics:
    /// - Only updates the timestamp if the new value is greater than the current one
    /// - Returns the OLD timestamp value
    /// - Ensures atomicity using compare-and-swap operations
    #[fastrace::trace]
    async fn fetch_set_vacuum_timestamp(
        &self,
        tenant: &Tenant,
        new_timestamp: DateTime<Utc>,
    ) -> Result<Option<VacuumWatermark>, KVAppError> {
        let ident = VacuumWatermarkIdent::new_global(tenant.clone());

        // Use crud_upsert_with for atomic compare-and-swap semantics
        let transition = self
            .crud_upsert_with::<Infallible>(&ident, |current: Option<SeqV<VacuumWatermark>>| {
                let current_retention: Option<VacuumWatermark> = current.map(|v| v.data);

                // Check if we should update based on monotonic property
                let should_update = match current_retention {
                    None => true, // Never set before, always update
                    Some(existing) => new_timestamp > existing.time, // Only update if new timestamp is greater
                };

                if should_update {
                    let new_retention = VacuumWatermark::new(new_timestamp);
                    Ok(Some(new_retention))
                } else {
                    // Return None to indicate no update needed
                    Ok(None)
                }
            })
            .await?
            // Safe to unwrap: type of business logic error is `Infallible`
            .unwrap();

        // Extract the old value to return
        let old_retention = transition.prev.map(|v| v.data);
        Ok(old_retention)
    }
}

#[async_trait::async_trait]
impl<KV> GarbageCollectionApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}

pub const ORPHAN_POSTFIX: &str = "orphan";

/// Remove copied files for a dropped table.
///
/// Dropped table can not be accessed by any query,
/// so it is safe to remove all the copied files in multiple sub transactions.
///
/// Returns the number of copied file entries removed.
async fn remove_copied_files_for_dropped_table(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    table_id: &TableId,
) -> Result<usize, MetaError> {
    let batch_size = 1024;

    let mut num_removed_copied_files = 0;
    // Loop until:
    // - all cleaned
    // - or table is removed from meta-service
    // - or is no longer in `dropped` state.
    for i in 0..usize::MAX {
        let mut txn = TxnRequest::default();

        let seq_meta = kv_api.get_pb(table_id).await?;
        let Some(seq_table_meta) = seq_meta else {
            return Ok(num_removed_copied_files);
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

        let key_stream = kv_api
            .list_pb_keys(ListOptions::unlimited(&dir_name))
            .await?;
        let copied_files = key_stream.take(batch_size).try_collect::<Vec<_>>().await?;

        if copied_files.is_empty() {
            return Ok(num_removed_copied_files);
        }

        for copied_ident in copied_files.iter() {
            // It is a dropped table, thus there is no data will be written to the table.
            // Therefore, we only need to assert the table_meta seq, and there is no need to assert
            // seq of each copied file.
            txn.if_then.push(txn_del(copied_ident));
        }

        info!(
            "remove_copied_files_for_dropped_table {}: {}-th batch remove: {} items: {}",
            table_id,
            i,
            copied_files.len(),
            copied_files.display()
        );

        num_removed_copied_files += copied_files.len();

        // Txn failures are ignored for simplicity, since copied files kv pairs are put with ttl,
        // they will not be leaked permanently, will be cleaned eventually.
        send_txn(kv_api, txn).await?;
    }
    unreachable!()
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
    db_is_dropped: bool,
) -> Result<Vec<TableNIV>, KVAppError> {
    info!(
        "get_history_tables_for_gc: db_id {}, limit {}",
        db_id, limit
    );

    let ident = TableIdHistoryIdent {
        database_id: db_id,
        table_name: "dummy".to_string(),
    };
    let dir_name = DirName::new(ident);
    let table_history_kvs = kv_api
        .list_pb_vec(ListOptions::unlimited(&dir_name))
        .await?;

    let mut args = vec![];

    // For table ids of each table with the same name, we keep the last one, e.g.
    // ```
    //  create or replace table t ... ; -- table_id a
    //  create or replace table t ... ; -- table_id b
    // ```
    // table_id `b` will be kept for table t,
    //
    // Please note that the largest table id might be dropped, e.g.
    // ```
    //  create or replace table t ... ; -- table_id 1
    //  ...
    //  drop table t ... ;
    // ```

    let mut latest_table_ids = HashSet::with_capacity(table_history_kvs.len());

    for (ident, table_history) in table_history_kvs {
        let id_list = &table_history.id_list;
        if let Some(last_id) = id_list.last() {
            latest_table_ids.insert(*last_id);
            for table_id in id_list.iter() {
                args.push((TableId::new(*table_id), ident.table_name.clone()));
            }
        }
    }

    let mut filter_tb_infos = vec![];
    const BATCH_SIZE: usize = 1000;

    let args_len = args.len();
    let mut num_out_of_time_range = 0;
    let mut num_processed = 0;

    info!(
        "get_history_tables_for_gc: {} items to process in db {}",
        args_len, db_id
    );

    // Process in batches to avoid performance issues
    for chunk in args.chunks(BATCH_SIZE) {
        // Get table metadata for current batch
        let table_id_idents = chunk.iter().map(|(table_id, _)| table_id.clone());
        let seq_metas = kv_api.get_pb_values_vec(table_id_idents).await?;

        // Filter by drop_time_range for current batch
        for (seq_meta, (table_id, table_name)) in seq_metas.into_iter().zip(chunk.iter()) {
            let Some(seq_meta) = seq_meta else {
                warn!(
                    "batch_filter_table_info cannot find {:?} table_meta",
                    table_id
                );
                continue;
            };

            if !db_is_dropped {
                // We shall not vacuum it if the table id is the largest table id of some table, but not marked with drop_on,
                {
                    let is_visible_last_active_table = seq_meta.data.drop_on.is_none()
                        && latest_table_ids.contains(&table_id.table_id);

                    if is_visible_last_active_table {
                        debug!(
                            "Table id {:?} of {} is the last visible one, not available for vacuum",
                            table_id, table_name,
                        );
                        num_out_of_time_range += 1;
                        continue;
                    }
                }

                // Now the table id is
                // - Either marked with drop_on
                //   We use its `drop_on` to do the time range filtering
                // - Or has no drop_on, but is not the last visible one of the table
                //   It is still available for vacuum, and we use its `updated_on` to do the time range filtering

                let time_point = if seq_meta.drop_on.is_none() {
                    &Some(seq_meta.updated_on)
                } else {
                    &seq_meta.drop_on
                };

                if !drop_time_range.contains(time_point) {
                    debug!("table {:?} is not in drop_time_range", seq_meta.data);
                    num_out_of_time_range += 1;
                    continue;
                }
            }

            filter_tb_infos.push(TableNIV::new(
                DBIdTableName::new(db_id, table_name.clone()),
                table_id.clone(),
                seq_meta,
            ));

            // Check if we have reached the limit
            if filter_tb_infos.len() >= limit {
                info!(
                    "get_history_tables_for_gc: reach limit {}, so far collected {}",
                    limit,
                    filter_tb_infos.len()
                );
                return Ok(filter_tb_infos);
            }
        }

        num_processed += chunk.len();
        info!(
            "get_history_tables_for_gc: process: {}/{}, {} items filtered by time range condition",
            num_processed, args_len, num_out_of_time_range
        );
    }

    Ok(filter_tb_infos)
}

/// Permanently remove a dropped database from the meta-service.
/// then remove all **dropped and non-dropped** tables in the database.
///
/// Returns the approximate number of metadata keys removed.
/// Note: DeleteByPrefix operations count as 1 but may remove multiple keys.
async fn gc_dropped_db_by_id(
    kv_api: &(impl GarbageCollectionApi + IndexApi + ?Sized),
    db_id: u64,
    tenant: &Tenant,
    catalog: &String,
    db_name: String,
) -> Result<usize, KVAppError> {
    let mut num_meta_keys_removed = 0;
    // List tables by tenant, db_id, table_name.
    let db_id_history_ident = DatabaseIdHistoryIdent::new(tenant, db_name.clone());
    let Some(seq_dbid_list) = kv_api.get_pb(&db_id_history_ident).await? else {
        info!("db_id_history_ident not found for db_id {}", db_id);
        return Ok(num_meta_keys_removed);
    };

    let mut db_id_list = seq_dbid_list.data;

    // If the db_id is not in the list, return.
    if db_id_list.id_list.remove_first(&db_id).is_none() {
        info!("db_id_history_ident of db_id {} is empty", db_id);
        return Ok(num_meta_keys_removed);
    }

    let dbid = DatabaseId { db_id };
    let Some(seq_db_meta) = kv_api.get_pb(&dbid).await? else {
        info!("database meta of db_id {} is empty", db_id);
        return Ok(num_meta_keys_removed);
    };

    if seq_db_meta.drop_on.is_none() {
        // If db is not marked as dropped, just ignore the gc request and return directly.
        // In subsequent KV transactions, we also verify that db_meta hasn't changed
        // to ensure we don't reclaim metadata of the given database that might have been
        // successfully undropped in a parallel operation.
        info!("database of db_id {} is not marked as dropped", db_id);
        return Ok(num_meta_keys_removed);
    }

    // Mark database meta as gc_in_progress if necessary
    let mut db_meta_seq = seq_db_meta.seq;
    if !seq_db_meta.gc_in_progress {
        // Mark db_meta as gc_in_process, in which state that db can no longer be undropped.
        let mut new_db_meta = seq_db_meta;
        new_db_meta.gc_in_progress = true;
        let mut txn = TxnRequest::default();
        txn_replace_exact(&mut txn, &dbid, new_db_meta.seq, &new_db_meta.data)?;
        txn.if_then.push(txn_get(&dbid));
        let (success, mut responses) = send_txn(kv_api, txn).await?;
        if !success {
            return Err(KVAppError::AppError(AppError::from(
                MarkDatabaseMetaAsGCInProgressFailed::new(format!(
                    "Failed to mark database {}[{}] as gc_in_progress",
                    db_name, db_id
                )),
            )));
        }

        // Grab the sequence number of new database meta key value pair
        let resp = responses.pop().unwrap();
        let Some(Response::Get(get_resp)) = resp.response else {
            unreachable!(
                "internal error: expect TxnGetResponseGet of get database meta by db_id, but got {:?}",
                resp.response
            )
        };
        db_meta_seq = get_resp
            .value
            .expect("txn op response of get(&dbid) should have value")
            .seq
    };

    // Cleaning up DbIdTableName keys
    {
        let db_id_table_name = DBIdTableName {
            db_id,
            // Going to use 1 level DirName as list prefix, thus the table_name field does not matter
            table_name: "dummy".to_string(),
        };
        let dir_name = DirName::new_with_level(db_id_table_name, 1);

        let mut num_db_id_table_name_keys_removed = 0;
        let batch_size = 1024;
        let key_stream = kv_api
            .list_pb_keys(ListOptions::unlimited(&dir_name))
            .await?;
        let mut chunks = key_stream.chunks(batch_size);
        while let Some(targets) = chunks.next().await {
            let mut txn = TxnRequest::default();
            use itertools::Itertools;
            let targets: Vec<DBIdTableName> = targets.into_iter().try_collect()?;
            num_db_id_table_name_keys_removed += targets.len();
            for target in &targets {
                txn.if_then.push(txn_del(target));
            }
            let (succ, _resp) = send_txn(kv_api, txn).await?;
            if !succ {
                return Err(KVAppError::AppError(AppError::from(
                    CleanDbIdTableNamesFailed::new(format!(
                        "Failed to clean dbIdTableNames of database {}[{}]",
                        db_name, db_id
                    )),
                )));
            } else {
                // Audit log, output all the items in the batch intentionally
                info!(
                    "DbIdTableNames cleaned (database {}[{}]): {:?}",
                    db_name, db_id, targets
                );
            }
        }
        info!(
            "{} DbIdTableNames cleaned for database {}[{}]",
            num_db_id_table_name_keys_removed, db_name, db_id,
        );
        num_meta_keys_removed += num_db_id_table_name_keys_removed;
    }

    let id_to_name = DatabaseIdToName { db_id };
    let Some(seq_name) = kv_api.get_pb(&id_to_name).await? else {
        info!("id_to_name not found for db_id {}", db_id);
        return Ok(num_meta_keys_removed);
    };

    let table_history_ident = TableIdHistoryIdent {
        database_id: db_id,
        table_name: "dummy".to_string(),
    };
    let dir_name = DirName::new(table_history_ident);

    let table_history_items = kv_api
        .list_pb_vec(ListOptions::unlimited(&dir_name))
        .await?;

    let mut txn = TxnRequest::default();

    for (ident, table_history) in table_history_items {
        for tb_id in table_history.id_list.iter() {
            let table_id_ident = TableId { table_id: *tb_id };

            let num_removed_copied_files =
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
            num_meta_keys_removed += num_removed_copied_files;
        }

        txn.condition
            .push(txn_cond_eq_seq(&ident, table_history.seq));
        txn.if_then.push(txn_del(&ident));
    }

    txn.condition
        .push(txn_cond_eq_seq(&db_id_history_ident, seq_dbid_list.seq));
    if db_id_list.id_list.is_empty() {
        txn.if_then.push(txn_del(&db_id_history_ident));
    } else {
        // save new db id list
        txn.if_then
            .push(txn_put_pb(&db_id_history_ident, &db_id_list)?);
    }

    // Verify database_meta hasn't changed since the mark database meta as gc_in_progress phase.
    // This establishes a condition for the transaction that will prevent it from committing
    // if the database metadata was modified by another concurrent operation (like un-dropping).
    txn.condition.push(txn_cond_eq_seq(&dbid, db_meta_seq));
    txn.if_then.push(txn_del(&dbid));
    txn.condition
        .push(txn_cond_eq_seq(&id_to_name, seq_name.seq));
    txn.if_then.push(txn_del(&id_to_name));

    // Clean up tag references for the dropped database (handles orphan data from
    // databases dropped before tag cleanup was added to drop_database_meta)
    {
        let taggable_object = TaggableObject::Database { db_id };
        let obj_tag_prefix = ObjectTagIdRefIdent::new_generic(
            tenant.clone(),
            ObjectTagIdRef::new(taggable_object.clone(), 0),
        );
        let obj_tag_dir = DirName::new(obj_tag_prefix);
        let mut tag_stream = kv_api.list_pb(ListOptions::unlimited(&obj_tag_dir)).await?;
        while let Some(entry) = tag_stream.try_next().await? {
            let tag_id = entry.key.name().tag_id;
            // Delete object -> tag reference
            let obj_ref_key = ObjectTagIdRefIdent::new_generic(
                tenant.clone(),
                ObjectTagIdRef::new(taggable_object.clone(), tag_id),
            );
            // Delete tag -> object reference
            let tag_ref_key = TagIdObjectRefIdent::new_generic(
                tenant.clone(),
                TagIdObjectRef::new(tag_id, taggable_object.clone()),
            );
            txn.if_then.push(txn_del(&obj_ref_key));
            txn.if_then.push(txn_del(&tag_ref_key));
        }
    }

    // Count removed keys (approximate for DeleteByPrefix operations)
    for op in &txn.if_then {
        if let Some(Request::Delete(_) | Request::DeleteByPrefix(_)) = &op.request {
            num_meta_keys_removed += 1;
        }
    }

    let _resp = kv_api.transaction(txn).await?;

    Ok(num_meta_keys_removed)
}

/// Permanently remove a dropped table from the meta-service.
///
/// The data of the table should already have been removed before calling this method.
///
/// Returns the approximate number of metadata keys removed.
/// Note: DeleteByPrefix operations count as 1 but may remove multiple keys.
async fn gc_dropped_table_by_id(
    kv_api: &(impl GarbageCollectionApi + IndexApi + ?Sized),
    tenant: &Tenant,
    catalog: &String,
    db_id_table_name: &DBIdTableName,
    table_id_ident: &TableId,
) -> Result<usize, KVAppError> {
    // First remove all copied files for the dropped table.
    // These markers are not part of the table and can be removed in separate transactions.
    let num_removed_copied_files =
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

        // Count removed keys (approximate for DeleteByPrefix operations)
        let mut num_meta_keys_removed = 0;
        for op in &txn.if_then {
            if let Some(Request::Delete(_) | Request::DeleteByPrefix(_)) = &op.request {
                num_meta_keys_removed += 1;
            }
        }
        num_meta_keys_removed += num_removed_copied_files;

        let (succ, _responses) = send_txn(kv_api, txn).await?;

        if succ {
            return Ok(num_meta_keys_removed);
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
        txn.if_then.push(txn_del(table_id_history_ident));
    } else {
        // save new table id list
        txn.if_then
            .push(txn_put_pb_with_ttl(table_id_history_ident, &history, None)?);
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

    // Remove table auto increment sequences
    {
        // clear the sequence associated with auto increment in the table field
        let auto_increment_key = AutoIncrementKey::new(table_id.table_id, 0);
        let dir_name = DirName::new(AutoIncrementStorageIdent::new_generic(
            tenant,
            auto_increment_key,
        ));
        let mut auto_increments = kv_api
            .list_pb_keys(ListOptions::unlimited(&dir_name))
            .await?;

        while let Some(auto_increment_ident) = auto_increments.try_next().await? {
            txn.if_then.push(txn_del(&auto_increment_ident));
        }
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

    // Clean up tag references for the dropped table
    {
        let taggable_object = TaggableObject::Table {
            table_id: table_id.table_id,
        };
        let obj_tag_prefix = ObjectTagIdRefIdent::new_generic(
            tenant.clone(),
            ObjectTagIdRef::new(taggable_object.clone(), 0),
        );
        let obj_tag_dir = DirName::new(obj_tag_prefix);
        let mut tag_stream = kv_api.list_pb(ListOptions::unlimited(&obj_tag_dir)).await?;
        while let Some(entry) = tag_stream.try_next().await? {
            let tag_id = entry.key.name().tag_id;
            // Delete object -> tag reference
            let obj_ref_key = ObjectTagIdRefIdent::new_generic(
                tenant.clone(),
                ObjectTagIdRef::new(taggable_object.clone(), tag_id),
            );
            // Delete tag -> object reference
            let tag_ref_key = TagIdObjectRefIdent::new_generic(
                tenant.clone(),
                TagIdObjectRef::new(tag_id, taggable_object.clone()),
            );
            txn.if_then.push(txn_del(&obj_ref_key));
            txn.if_then.push(txn_del(&tag_ref_key));
        }
    }

    let tb_meta = &seq_meta.data;

    // Clean up policy references for the dropped table.
    // These records may have been orphaned if the table was dropped via DROP DATABASE.

    // Delete masking policy references
    {
        let policy_ids: HashSet<u64> = tb_meta
            .column_mask_policy_columns_ids
            .values()
            .map(|policy_map| policy_map.policy_id)
            .collect();

        txn.if_then.extend(policy_ids.into_iter().map(|policy_id| {
            txn_del(&MaskPolicyTableIdIdent::new_generic(
                tenant.clone(),
                MaskPolicyIdTableId {
                    policy_id,
                    table_id: table_id.table_id,
                },
            ))
        }));
    }

    // Delete row access policy reference
    if let Some(policy_map) = &tb_meta.row_access_policy_columns_ids {
        txn.if_then
            .push(txn_del(&RowAccessPolicyTableIdIdent::new_generic(
                tenant.clone(),
                RowAccessPolicyIdTableId {
                    policy_id: policy_map.policy_id,
                    table_id: table_id.table_id,
                },
            )));
    }

    Ok(Ok(()))
}

async fn remove_index_for_dropped_table(
    kv_api: &(impl IndexApi + ?Sized),
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

        txn.if_then.push(txn_del(&name_ident)); // (tenant, index_name) -> index_id
        txn.if_then.push(txn_del(&id_ident)); // (index_id) -> index_meta
        txn.if_then.push(txn_del(&id_to_name_ident)); // __fd_index_id_to_name/<index_id> -> (tenant,index_name)
    }

    Ok(())
}
