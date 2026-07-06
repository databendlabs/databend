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

use std::io::Error;
use std::io::ErrorKind;

use chrono::Utc;
use databend_common_meta_app::KeyExistsBuilder;
use databend_common_meta_app::KeyUnknownBuilder;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::EmptyProto;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::MVDefinitionIdent;
use databend_common_meta_app::schema::MVId;
use databend_common_meta_app::schema::MVMeta;
use databend_common_meta_app::schema::MVMetaIdent;
use databend_common_meta_app::schema::MVSourceIndex;
use databend_common_meta_app::schema::MVSourceIndexIdent;
use databend_common_meta_app::schema::MarkedDeletedMVId;
use databend_common_meta_app::schema::MarkedDeletedMVIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdHistoryIdent;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::materialized_view::mv_meta_ident;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_app::tenant_key::errors::UnknownError;
use databend_meta_client::kvapi;
use databend_meta_client::types::ConditionResult::Eq;
use databend_meta_client::types::InvalidReply;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnRequest;

use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_del;
use crate::txn_put_pb;
use crate::util::IdempotentKVTxnResponse;
use crate::util::IdempotentKVTxnSender;

fn invalid_mv_metadata(message: impl ToString) -> MetaError {
    let message = message.to_string();
    let source = Error::new(ErrorKind::InvalidData, message.clone());
    InvalidReply::new(message, &source).into()
}

async fn build_commit_materialized_view_txn<KV>(
    kv_api: &KV,
    mv_meta_ident: &MVMetaIdent,
    expected_prev_mv_id: Option<MVId>,
    orphan_ident: &TableIdHistoryIdent,
    mv_meta: &MVMeta,
    definition: &MVDefinition,
) -> Result<Result<TxnRequest, ExistError<mv_meta_ident::Resource, MVId>>, MetaError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    let tenant = mv_meta_ident.tenant();
    let mv_id = *mv_meta_ident.name();

    let source_ident = TableId::new(mv_meta.source_table_id);
    let (source_seq, source_meta) = kv_api.get_pb_seq_and_value(&source_ident).await?;
    if source_meta.is_none() {
        return Err(invalid_mv_metadata(format!(
            "source table {} does not exist",
            mv_meta.source_table_id
        )));
    }
    if source_seq != mv_meta.source_progress.table_meta_seq {
        return Err(invalid_mv_metadata(format!(
            "source table {} seq changed from {} to {} while committing materialized view {}",
            mv_meta.source_table_id, mv_meta.source_progress.table_meta_seq, source_seq, mv_id
        )));
    }

    let definition_ident = MVDefinitionIdent::new_generic(tenant, mv_id);
    let source_index_ident =
        MVSourceIndexIdent::new_generic(tenant, MVSourceIndex::new(mv_meta.source_table_id, mv_id));

    let existing_mv_meta = kv_api.get_pb(mv_meta_ident).await?;
    let existing_definition = kv_api.get_pb(&definition_ident).await?;
    let existing_source_index = kv_api.get_pb(&source_index_ident).await?;
    if existing_mv_meta.is_some()
        || existing_definition.is_some()
        || existing_source_index.is_some()
    {
        return Ok(Err(mv_meta_ident.exist_error("commit materialized view")));
    }

    let mv_ident = TableId::new(*mv_id);
    let mv_id_to_name_ident = TableIdToName { table_id: *mv_id };
    let mv_name = kv_api
        .get_pb(&mv_id_to_name_ident)
        .await?
        .ok_or_else(|| invalid_mv_metadata(format!("TableIdToName({mv_id}) does not exist")))?;
    let db_id_mv_name = mv_name.data;
    if orphan_ident.database_id != db_id_mv_name.db_id {
        return Err(invalid_mv_metadata(format!(
            "orphan database id {} does not match target database id {}",
            orphan_ident.database_id, db_id_mv_name.db_id
        )));
    }

    let db_ident = DatabaseId::new(db_id_mv_name.db_id);
    let db_meta = kv_api.get_pb(&db_ident).await?.ok_or_else(|| {
        invalid_mv_metadata(format!("database {} does not exist", db_id_mv_name.db_id))
    })?;
    let (mv_name_seq, current_mv_id) = kv_api.get_pb_seq_and_value(&db_id_mv_name).await?;
    if current_mv_id.map(|id| MVId::new(id.table_id)) != expected_prev_mv_id {
        return Ok(Err(mv_meta_ident.exist_error(format!(
            "current object for {db_id_mv_name} changed while committing materialized view"
        ))));
    }

    let orphan_history = kv_api.get_pb(orphan_ident).await?.ok_or_else(|| {
        invalid_mv_metadata(format!("orphan record {orphan_ident} does not exist"))
    })?;
    if orphan_history.data.id_list.as_slice() != [*mv_id] {
        return Err(invalid_mv_metadata(format!(
            "orphan record {orphan_ident} does not contain only materialized view {mv_id}"
        )));
    }

    let staged_mv_table_meta = kv_api
        .get_pb(&mv_ident)
        .await?
        .ok_or_else(|| invalid_mv_metadata(format!("TableMeta({mv_id}) does not exist")))?;
    if staged_mv_table_meta.data.drop_on.is_none() {
        return Err(invalid_mv_metadata(format!(
            "staged TableMeta({mv_id}) has no drop time"
        )));
    }
    let mut published_mv_table_meta = staged_mv_table_meta.data;
    published_mv_table_meta.drop_on = None;

    let mut txn = TxnRequest::new(
        vec![
            txn_cond_seq(&source_ident, Eq, source_seq), // source unchanged
            txn_cond_seq(mv_meta_ident, Eq, 0),          // MVMeta absent
            txn_cond_seq(&definition_ident, Eq, 0),      // definition absent
            txn_cond_seq(&source_index_ident, Eq, 0),    // source index absent
            txn_cond_seq(&db_ident, Eq, db_meta.seq),    // database unchanged
            txn_cond_seq(&db_id_mv_name, Eq, mv_name_seq), // live MV name unchanged
            txn_cond_seq(&mv_ident, Eq, staged_mv_table_meta.seq), // staged MV unchanged
            txn_cond_seq(orphan_ident, Eq, orphan_history.seq), // orphan unchanged
        ],
        vec![
            txn_put_pb(&db_ident, &db_meta.data), // DatabaseId -> DatabaseMeta; bump seq
            txn_put_pb(&db_id_mv_name, &mv_ident), // DBIdTableName -> MVId
            txn_put_pb(&mv_ident, &published_mv_table_meta), // MVId -> live TableMeta
            txn_del(orphan_ident),                // delete the staged table's orphan record
            txn_put_pb(mv_meta_ident, mv_meta),   // MVMetaIdent(mv_id) -> MVMeta
            txn_put_pb(&definition_ident, definition), // MVDefinitionIdent(mv_id) -> MVDefinition
            txn_put_pb(&source_index_ident, &EmptyProto {}), /* MVSourceIndexIdent(source_id, mv_id) -> marker */
        ],
    );

    if let Some(prev_mv_id) = expected_prev_mv_id {
        let prev_ident = MVMetaIdent::new_generic(tenant, prev_mv_id);
        let prev_meta = kv_api.get_pb(&prev_ident).await?.ok_or_else(|| {
            invalid_mv_metadata(format!("replaced MVMeta({prev_mv_id}) does not exist"))
        })?;
        let prev_definition_ident = MVDefinitionIdent::new_generic(tenant, prev_mv_id);
        let prev_definition = kv_api
            .get_pb(&prev_definition_ident)
            .await?
            .ok_or_else(|| {
                invalid_mv_metadata(format!(
                    "replaced MVDefinition({prev_mv_id}) does not exist"
                ))
            })?;
        let prev_mv_ident = TableId::new(*prev_mv_id);
        let prev_mv_table_meta = kv_api.get_pb(&prev_mv_ident).await?.ok_or_else(|| {
            invalid_mv_metadata(format!("replaced TableMeta({prev_mv_id}) does not exist"))
        })?;
        let mut dropped_prev_mv_table_meta = prev_mv_table_meta.data;
        dropped_prev_mv_table_meta.drop_on = Some(Utc::now());

        let prev_source_index_ident = MVSourceIndexIdent::new_generic(
            tenant,
            MVSourceIndex::new(prev_meta.data.source_table_id, prev_mv_id),
        );
        let tombstone_ident = MarkedDeletedMVIdent::new_generic(
            tenant,
            MarkedDeletedMVId::new(db_id_mv_name.db_id, prev_mv_id),
        );

        txn.condition.extend([
            txn_cond_seq(&prev_ident, Eq, prev_meta.seq), // old MVMeta unchanged
            txn_cond_seq(&prev_definition_ident, Eq, prev_definition.seq), // old definition unchanged
            txn_cond_seq(&prev_mv_ident, Eq, prev_mv_table_meta.seq), // old MV unchanged
        ]);
        txn.if_then.extend([
            txn_del(&prev_ident),                                    // delete old MVMeta
            txn_del(&prev_definition_ident),                         // delete old definition
            txn_del(&prev_source_index_ident),                       // delete old source index
            txn_put_pb(&prev_mv_ident, &dropped_prev_mv_table_meta), // mark old MV dropped
            txn_put_pb(&tombstone_ident, &EmptyProto {}), // MarkedDeletedMVIdent -> marker
        ]);
    }

    Ok(Ok(txn))
}

async fn build_drop_materialized_view_txn<KV>(
    kv_api: &KV,
    mv_meta_ident: &MVMetaIdent,
) -> Result<Result<TxnRequest, UnknownError<mv_meta_ident::Resource, MVId>>, MetaError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    let tenant = mv_meta_ident.tenant();
    let mv_id = *mv_meta_ident.name();
    let Some(mv_meta) = kv_api.get_pb(mv_meta_ident).await? else {
        return Ok(Err(mv_meta_ident.unknown_error("drop materialized view")));
    };

    let definition_ident = MVDefinitionIdent::new_generic(tenant, mv_id);
    let Some(definition) = kv_api.get_pb(&definition_ident).await? else {
        return Ok(Err(
            mv_meta_ident.unknown_error("drop materialized view definition")
        ));
    };
    let mv_ident = TableId::new(*mv_id);
    let Some(mv_table_meta) = kv_api.get_pb(&mv_ident).await? else {
        return Ok(Err(
            mv_meta_ident.unknown_error("drop materialized view table metadata")
        ));
    };
    if mv_table_meta.data.drop_on.is_some() {
        return Ok(Err(
            mv_meta_ident.unknown_error("materialized view is already dropped")
        ));
    }

    let mv_id_to_name_ident = TableIdToName { table_id: *mv_id };
    let Some(mv_name) = kv_api.get_pb(&mv_id_to_name_ident).await? else {
        return Ok(Err(
            mv_meta_ident.unknown_error("drop materialized view name mapping")
        ));
    };
    let db_id_mv_name: DBIdTableName = mv_name.data;
    let (mv_name_seq, current_mv_id) = kv_api.get_pb_seq_and_value(&db_id_mv_name).await?;
    if current_mv_id.map(|id| id.table_id) != Some(*mv_id) {
        return Ok(Err(mv_meta_ident.unknown_error(
            "materialized view is no longer the current object for its name",
        )));
    }

    let db_ident = DatabaseId::new(db_id_mv_name.db_id);
    let Some(db_meta) = kv_api.get_pb(&db_ident).await? else {
        return Ok(Err(
            mv_meta_ident.unknown_error("drop materialized view database")
        ));
    };

    let mut dropped_mv_table_meta = mv_table_meta.data;
    dropped_mv_table_meta.drop_on = Some(Utc::now());
    let source_index_ident = MVSourceIndexIdent::new_generic(
        tenant,
        MVSourceIndex::new(mv_meta.data.source_table_id, mv_id),
    );
    let tombstone_ident = MarkedDeletedMVIdent::new_generic(
        tenant,
        MarkedDeletedMVId::new(db_id_mv_name.db_id, mv_id),
    );

    Ok(Ok(TxnRequest::new(
        vec![
            txn_cond_seq(mv_meta_ident, Eq, mv_meta.seq),
            txn_cond_seq(&definition_ident, Eq, definition.seq),
            txn_cond_seq(&mv_ident, Eq, mv_table_meta.seq),
            txn_cond_seq(&db_id_mv_name, Eq, mv_name_seq),
            txn_cond_seq(&db_ident, Eq, db_meta.seq),
        ],
        vec![
            txn_put_pb(&db_ident, &db_meta.data),
            txn_put_pb(&mv_ident, &dropped_mv_table_meta),
            txn_del(&db_id_mv_name),
            txn_del(mv_meta_ident),
            txn_del(&definition_ident),
            txn_del(&source_index_ident),
            txn_put_pb(&tombstone_ident, &EmptyProto {}),
        ],
    )))
}

/// APIs for metadata that belongs exclusively to materialized views.
#[async_trait::async_trait]
pub trait MaterializedViewApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    #[logcall::logcall]
    #[fastrace::trace]
    async fn commit_materialized_view(
        &self,
        ident: &MVMetaIdent,
        expected_prev_mv_id: Option<MVId>,
        orphan_ident: &TableIdHistoryIdent,
        mv_meta: &MVMeta,
        definition: &MVDefinition,
    ) -> Result<Result<(), ExistError<mv_meta_ident::Resource, MVId>>, MetaTxnError> {
        let txn_sender = IdempotentKVTxnSender::new();
        let mut trials = txn_backoff(None, "commit_materialized_view");

        loop {
            trials.next().unwrap()?.await;
            let txn = match build_commit_materialized_view_txn(
                self,
                ident,
                expected_prev_mv_id,
                orphan_ident,
                mv_meta,
                definition,
            )
            .await?
            {
                Ok(txn) => txn,
                Err(err) => return Ok(Err(err)),
            };

            let txn_response = txn_sender.send_txn(self, txn).await?;
            match txn_response {
                IdempotentKVTxnResponse::Success(_) | IdempotentKVTxnResponse::AlreadyCommitted => {
                    return Ok(Ok(()));
                }
                IdempotentKVTxnResponse::Failed(_) => {}
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_materialized_view(
        &self,
        ident: &MVMetaIdent,
    ) -> Result<Result<(), UnknownError<mv_meta_ident::Resource, MVId>>, MetaTxnError> {
        let mut trials = txn_backoff(None, "drop_materialized_view");

        loop {
            trials.next().unwrap()?.await;
            let txn = match build_drop_materialized_view_txn(self, ident).await? {
                Ok(txn) => txn,
                Err(err) => return Ok(Err(err)),
            };
            let (succ, _) = send_txn(self, txn).await?;
            if succ {
                return Ok(Ok(()));
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_mv_meta(&self, ident: &MVMetaIdent) -> Result<Option<SeqV<MVMeta>>, MetaError> {
        self.get_pb(ident).await
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_mv_definition(
        &self,
        ident: &MVDefinitionIdent,
    ) -> Result<Option<SeqV<MVDefinition>>, MetaError> {
        self.get_pb(ident).await
    }
}

impl<KV> MaterializedViewApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
