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

use chrono::Utc;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::DropDbWithDropTime;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseIdHistoryIdent;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DbIdList;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::ConditionResult::Eq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnRequest;
use log::debug;
use log::warn;

use crate::error_util::unknown_database_error;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::serialize_struct;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_op_del;
use crate::txn_op_put;

pub(crate) async fn drop_database_meta(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    tenant_dbname: &DatabaseNameIdent,
    catalog_name: Option<String>,
    if_exists: bool,
    drop_name_key: bool,
    txn: &mut TxnRequest,
) -> Result<u64, KVAppError> {
    let res = get_db_or_err(
        kv_api,
        tenant_dbname,
        format!("drop_database: {}", tenant_dbname.display()),
    )
    .await;

    let (seq_db_id, mut db_meta) = match res {
        Ok(x) => x,
        Err(e) => {
            if let KVAppError::AppError(AppError::UnknownDatabase(_)) = e {
                if if_exists {
                    return Ok(0);
                }
            }

            return Err(e);
        }
    };

    // remove db_name -> db id
    if drop_name_key {
        txn.condition
            .push(txn_cond_seq(tenant_dbname, Eq, seq_db_id.seq));
        txn.if_then.push(txn_op_del(tenant_dbname)); // (tenant, db_name) -> db_id
    }

    // Delete db by these operations:
    // del (tenant, db_name) -> db_id
    // set db_meta.drop_on = now and update (db_id) -> db_meta

    let db_id_key = seq_db_id.data;

    debug!(
        seq_db_id :? = seq_db_id,
        name_key :? =(tenant_dbname);
        "drop_database"
    );

    {
        // drop a table with drop time
        if db_meta.drop_on.is_some() {
            return Err(KVAppError::AppError(AppError::DropDbWithDropTime(
                DropDbWithDropTime::new(tenant_dbname.database_name()),
            )));
        }
        // update drop on time
        db_meta.drop_on = Some(Utc::now());

        txn.condition
            .push(txn_cond_seq(&db_id_key, Eq, db_meta.seq));

        txn.if_then
            .push(txn_op_put(&db_id_key, serialize_struct(&*db_meta)?)); // (db_id) -> db_meta
    }

    // add DbIdListKey if not exists
    let dbid_idlist =
        DatabaseIdHistoryIdent::new(tenant_dbname.tenant(), tenant_dbname.database_name());
    let (db_id_list_seq, db_id_list_opt) = kv_api.get_pb_seq_and_value(&dbid_idlist).await?;

    if db_id_list_seq == 0 || db_id_list_opt.is_none() {
        warn!(
            "drop db:{:?}, seq_db_id:{:?} has no DbIdListKey",
            tenant_dbname, seq_db_id
        );

        let mut db_id_list = DbIdList::new();
        db_id_list.append(*seq_db_id.data);

        txn.condition
            .push(txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq));
        // _fd_db_id_list/<tenant>/<db_name> -> db_id_list
        txn.if_then
            .push(txn_op_put(&dbid_idlist, serialize_struct(&db_id_list)?));
    };

    // Clean up ownership if catalog_name is provided (CREATE OR REPLACE case)
    if let Some(catalog_name) = catalog_name {
        let ownership_object = OwnershipObject::Database {
            catalog_name,
            db_id: *seq_db_id.data,
        };
        let ownership_key =
            TenantOwnershipObjectIdent::new(tenant_dbname.tenant(), ownership_object);
        txn.if_then.push(txn_op_del(&ownership_key));
    }

    Ok(*seq_db_id.data)
}

/// Returns (db_id_seq, db_id, db_meta_seq, db_meta)
pub(crate) async fn get_db_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &DatabaseNameIdent,
    msg: impl Display,
) -> Result<(SeqV<DatabaseId>, SeqV<DatabaseMeta>), KVAppError> {
    let seq_db_id = kv_api.get_pb(name_key).await?;
    let seq_db_id = seq_db_id.ok_or_else(|| unknown_database_error(name_key, &msg))?;

    let id_key = seq_db_id.data.into_inner();

    let seq_db_meta = kv_api.get_pb(&id_key).await?;
    let seq_db_meta = seq_db_meta.ok_or_else(|| unknown_database_error(name_key, &msg))?;

    Ok((seq_db_id.map(|x| x.into_inner()), seq_db_meta))
}
