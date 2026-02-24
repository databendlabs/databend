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
use databend_common_meta_app::schema::ObjectTagIdRef;
use databend_common_meta_app::schema::ObjectTagIdRefIdent;
use databend_common_meta_app::schema::TagIdObjectRef;
use databend_common_meta_app::schema::TagIdObjectRefIdent;
use databend_common_meta_app::schema::TaggableObject;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::ConditionResult::Eq;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnRequest;
use futures::TryStreamExt;
use log::debug;
use log::warn;

use crate::error_util::unknown_database_error;
use crate::kv_app_error::KVAppError;
use crate::kv_app_error::KVAppResultExt;
use crate::kv_pb_api::KVPbApi;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_del;
use crate::txn_put_pb;

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

    let (seq_db_id, mut db_meta) = match res.into_nested()? {
        Ok(x) => x,
        Err(AppError::UnknownDatabase(_)) if if_exists => return Ok(0),
        Err(app_err) => return Err(app_err.into()),
    };

    // remove db_name -> db id
    if drop_name_key {
        txn.condition
            .push(txn_cond_seq(tenant_dbname, Eq, seq_db_id.seq));
        txn.if_then.push(txn_del(tenant_dbname)); // (tenant, db_name) -> db_id
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

        txn.if_then.push(txn_put_pb(&db_id_key, &*db_meta)?); // (db_id) -> db_meta
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
        txn.if_then.push(txn_put_pb(&dbid_idlist, &db_id_list)?);
    };

    // Clean up ownership if catalog_name is provided (CREATE OR REPLACE case)
    if let Some(catalog_name) = catalog_name {
        let ownership_object = OwnershipObject::Database {
            catalog_name,
            db_id: *seq_db_id.data,
        };
        let ownership_key =
            TenantOwnershipObjectIdent::new(tenant_dbname.tenant(), ownership_object);
        txn.if_then.push(txn_del(&ownership_key));
    }

    // Clean up tag references (UNDROP won't restore them; small race window is acceptable,
    // VACUUM handles orphans). See `set_object_tags` in tag_api.rs for concurrency design.
    let db_id = *seq_db_id.data;
    let taggable_object = TaggableObject::Database { db_id };
    let obj_tag_prefix = ObjectTagIdRefIdent::new_generic(
        tenant_dbname.tenant().clone(),
        ObjectTagIdRef::new(taggable_object.clone(), 0),
    );
    let obj_tag_dir = DirName::new(obj_tag_prefix);
    let strm = kv_api.list_pb(ListOptions::unlimited(&obj_tag_dir)).await?;
    let tag_entries: Vec<_> = strm.try_collect().await?;
    for entry in tag_entries {
        let tag_id = entry.key.name().tag_id;
        // Delete object -> tag reference
        let obj_ref_key = ObjectTagIdRefIdent::new_generic(
            tenant_dbname.tenant().clone(),
            ObjectTagIdRef::new(taggable_object.clone(), tag_id),
        );
        // Delete tag -> object reference
        let tag_ref_key = TagIdObjectRefIdent::new_generic(
            tenant_dbname.tenant().clone(),
            TagIdObjectRef::new(tag_id, taggable_object.clone()),
        );
        txn.if_then.push(txn_del(&obj_ref_key));
        txn.if_then.push(txn_del(&tag_ref_key));
    }

    Ok(db_id)
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
