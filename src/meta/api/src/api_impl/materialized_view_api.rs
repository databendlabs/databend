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

use chrono::Utc;
use databend_common_meta_app::KeyExistsBuilder;
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
use databend_meta_client::kvapi;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnCondition;
use databend_meta_client::types::TxnOp;
use databend_meta_client::types::TxnRequest;

use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_core_util::send_txn;
use crate::txn_del;
use crate::txn_put_pb;
use crate::util::IdempotentKVTxnResponse;
use crate::util::IdempotentKVTxnSender;

async fn build_replace_materialized_view_txn<KV>(
    kv_api: &KV,
    mv_meta_ident: &MVMetaIdent,
    db_id: u64,
    prev_mv_id: MVId,
    new_source_table_id: u64,
    new_source_index: &mut MVSourceIndex,
) -> Result<Option<(Vec<TxnCondition>, Vec<TxnOp>)>, MetaError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    let tenant = mv_meta_ident.tenant();
    let prev_ident = MVMetaIdent::new_generic(tenant, prev_mv_id);
    let prev_definition_ident = MVDefinitionIdent::new_generic(tenant, prev_mv_id);
    let prev_mv_ident = TableId::new(*prev_mv_id);
    let (Some(prev_meta), Some(prev_definition), Some(prev_mv_table_meta)) = (
        kv_api.get_pb(&prev_ident).await?,
        kv_api.get_pb(&prev_definition_ident).await?,
        kv_api.get_pb(&prev_mv_ident).await?,
    ) else {
        return Ok(None);
    };

    let mut dropped_prev_mv_table_meta = prev_mv_table_meta.data.clone();
    dropped_prev_mv_table_meta.drop_on = Some(Utc::now());
    let tombstone_ident =
        MarkedDeletedMVIdent::new_generic(tenant, MarkedDeletedMVId::new(db_id, prev_mv_id));
    let conditions = vec![
        txn_cond_eq_seq(&prev_ident, prev_meta.seq),
        txn_cond_eq_seq(&prev_definition_ident, prev_definition.seq),
        txn_cond_eq_seq(&prev_mv_ident, prev_mv_table_meta.seq),
    ];
    let ops = vec![
        txn_del(&prev_ident),
        txn_del(&prev_definition_ident),
        txn_put_pb(&prev_mv_ident, &dropped_prev_mv_table_meta),
        txn_put_pb(&tombstone_ident, &EmptyProto {}),
    ];

    if prev_meta.data.source_table_id == new_source_table_id {
        if !new_source_index.remove(prev_mv_id) {
            return Ok(None);
        }
        return Ok(Some((conditions, ops)));
    }

    let prev_source_index_ident =
        MVSourceIndexIdent::new_generic(tenant, prev_meta.data.source_table_id);
    let Some(prev_source_index_seqv) = kv_api.get_pb(&prev_source_index_ident).await? else {
        return Ok(None);
    };
    let mut prev_source_index = prev_source_index_seqv.data;
    if !prev_source_index.remove(prev_mv_id) {
        return Ok(None);
    }

    let condition = txn_cond_eq_seq(&prev_source_index_ident, prev_source_index_seqv.seq);
    let op = if prev_source_index.is_empty() {
        txn_del(&prev_source_index_ident)
    } else {
        txn_put_pb(&prev_source_index_ident, &prev_source_index)
    };

    Ok(Some((
        conditions.into_iter().chain([condition]).collect(),
        ops.into_iter().chain([op]).collect(),
    )))
}

async fn build_commit_materialized_view_txn<KV>(
    kv_api: &KV,
    mv_meta_ident: &MVMetaIdent,
    expected_prev_mv_id: Option<MVId>,
    orphan_ident: &TableIdHistoryIdent,
    mv_meta: &MVMeta,
    definition: &MVDefinition,
) -> Result<Result<Option<TxnRequest>, ExistError<mv_meta_ident::Resource, MVId>>, MetaError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    let tenant = mv_meta_ident.tenant();
    let mv_id = *mv_meta_ident.name();

    let source_ident = TableId::new(mv_meta.source_table_id);
    let (source_seq, source_meta) = kv_api.get_pb_seq_and_value(&source_ident).await?;
    if source_meta.is_none() || source_seq != mv_meta.source_progress.table_meta_seq {
        return Ok(Ok(None));
    }

    let definition_ident = MVDefinitionIdent::new_generic(tenant, mv_id);
    let source_index_ident = MVSourceIndexIdent::new_generic(tenant, mv_meta.source_table_id);

    let existing_mv_meta = kv_api.get_pb(mv_meta_ident).await?;
    let existing_definition = kv_api.get_pb(&definition_ident).await?;
    if existing_mv_meta.is_some() || existing_definition.is_some() {
        return Ok(Err(mv_meta_ident.exist_error("commit materialized view")));
    }

    let (source_index_seq, mut source_index) = match kv_api.get_pb(&source_index_ident).await? {
        Some(seqv) => (seqv.seq, seqv.data),
        None => (0, Default::default()),
    };
    if source_index.contains(mv_id) {
        return Ok(Err(mv_meta_ident.exist_error("commit materialized view")));
    }

    let mv_ident = TableId::new(*mv_id);
    let mv_id_to_name_ident = TableIdToName { table_id: *mv_id };
    let Some(mv_name) = kv_api.get_pb(&mv_id_to_name_ident).await? else {
        return Ok(Ok(None));
    };
    let db_id_mv_name = mv_name.data;

    let db_ident = DatabaseId::new(db_id_mv_name.db_id);
    let (Some(db_meta), Some(orphan_history), Some(staged_mv_table_meta)) = (
        kv_api.get_pb(&db_ident).await?,
        kv_api.get_pb(orphan_ident).await?,
        kv_api.get_pb(&mv_ident).await?,
    ) else {
        return Ok(Ok(None));
    };
    let (mv_name_seq, current_mv_id) = kv_api.get_pb_seq_and_value(&db_id_mv_name).await?;
    if orphan_ident.database_id != db_id_mv_name.db_id
        || current_mv_id.map(|id| MVId::new(id.table_id)) != expected_prev_mv_id
        || orphan_history.data.id_list.as_slice() != [*mv_id]
        || staged_mv_table_meta.data.drop_on.is_none()
    {
        return Ok(Ok(None));
    }
    let mut published_mv_table_meta = staged_mv_table_meta.data;
    published_mv_table_meta.drop_on = None;

    // CREATE OR REPLACE atomically removes the previously published MV.
    let (previous_conditions, previous_ops) = match expected_prev_mv_id {
        Some(prev_mv_id) => match build_replace_materialized_view_txn(
            kv_api,
            mv_meta_ident,
            db_id_mv_name.db_id,
            prev_mv_id,
            mv_meta.source_table_id,
            &mut source_index,
        )
        .await?
        {
            Some(replace_txn) => replace_txn,
            None => return Ok(Ok(None)),
        },
        None => (vec![], vec![]),
    };

    source_index.add(mv_id);

    let txn_req = TxnRequest::new(
        vec![
            txn_cond_eq_seq(&source_ident, source_seq),
            txn_cond_eq_seq(mv_meta_ident, 0),
            txn_cond_eq_seq(&definition_ident, 0),
            txn_cond_eq_seq(&source_index_ident, source_index_seq),
            txn_cond_eq_seq(&db_ident, db_meta.seq),
            txn_cond_eq_seq(&db_id_mv_name, mv_name_seq),
            txn_cond_eq_seq(&mv_ident, staged_mv_table_meta.seq),
            txn_cond_eq_seq(orphan_ident, orphan_history.seq),
        ]
        .into_iter()
        .chain(previous_conditions)
        .collect(),
        vec![
            txn_put_pb(&db_ident, &db_meta.data),
            txn_put_pb(&db_id_mv_name, &mv_ident),
            txn_put_pb(&mv_ident, &published_mv_table_meta),
            txn_del(orphan_ident),
            txn_put_pb(mv_meta_ident, mv_meta),
            txn_put_pb(&definition_ident, definition),
            txn_put_pb(&source_index_ident, &source_index),
        ]
        .into_iter()
        .chain(previous_ops)
        .collect(),
    );

    Ok(Ok(Some(txn_req)))
}

async fn build_drop_materialized_view_txn<KV>(
    kv_api: &KV,
    mv_meta_ident: &MVMetaIdent,
) -> Result<Option<TxnRequest>, MetaError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    let tenant = mv_meta_ident.tenant();
    let mv_id = *mv_meta_ident.name();
    let definition_ident = MVDefinitionIdent::new_generic(tenant, mv_id);
    let mv_ident = TableId::new(*mv_id);
    let mv_id_to_name_ident = TableIdToName { table_id: *mv_id };
    let (Some(mv_meta), Some(definition), Some(mv_table_meta), Some(mv_name)) = (
        kv_api.get_pb(mv_meta_ident).await?,
        kv_api.get_pb(&definition_ident).await?,
        kv_api.get_pb(&mv_ident).await?,
        kv_api.get_pb(&mv_id_to_name_ident).await?,
    ) else {
        return Ok(None);
    };
    let db_id_mv_name: DBIdTableName = mv_name.data;
    let (mv_name_seq, current_mv_id) = kv_api.get_pb_seq_and_value(&db_id_mv_name).await?;
    if mv_table_meta.data.drop_on.is_some() || current_mv_id.map(|id| id.table_id) != Some(*mv_id) {
        return Ok(None);
    }

    let db_ident = DatabaseId::new(db_id_mv_name.db_id);
    let Some(db_meta) = kv_api.get_pb(&db_ident).await? else {
        return Ok(None);
    };

    let mut dropped_mv_table_meta = mv_table_meta.data;
    dropped_mv_table_meta.drop_on = Some(Utc::now());
    let source_index_ident = MVSourceIndexIdent::new_generic(tenant, mv_meta.data.source_table_id);
    let Some(source_index_seqv) = kv_api.get_pb(&source_index_ident).await? else {
        return Ok(None);
    };
    let source_index_seq = source_index_seqv.seq;
    let mut source_index = source_index_seqv.data;
    if !source_index.remove(mv_id) {
        return Ok(None);
    }
    let tombstone_ident = MarkedDeletedMVIdent::new_generic(
        tenant,
        MarkedDeletedMVId::new(db_id_mv_name.db_id, mv_id),
    );

    Ok(Some(TxnRequest::new(
        vec![
            txn_cond_eq_seq(mv_meta_ident, mv_meta.seq),
            txn_cond_eq_seq(&definition_ident, definition.seq),
            txn_cond_eq_seq(&mv_ident, mv_table_meta.seq),
            txn_cond_eq_seq(&db_id_mv_name, mv_name_seq),
            txn_cond_eq_seq(&db_ident, db_meta.seq),
            txn_cond_eq_seq(&source_index_ident, source_index_seq),
        ],
        {
            let mut ops = vec![
                txn_put_pb(&db_ident, &db_meta.data),
                txn_put_pb(&mv_ident, &dropped_mv_table_meta),
                txn_del(&db_id_mv_name),
                txn_del(mv_meta_ident),
                txn_del(&definition_ident),
                txn_put_pb(&tombstone_ident, &EmptyProto {}),
            ];
            if source_index.is_empty() {
                ops.push(txn_del(&source_index_ident));
            } else {
                ops.push(txn_put_pb(&source_index_ident, &source_index));
            }
            ops
        },
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
    ) -> Result<Result<Option<MVId>, ExistError<mv_meta_ident::Resource, MVId>>, MetaTxnError> {
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
                Ok(Some(txn)) => txn,
                Ok(None) => return Ok(Ok(None)),
                Err(err) => return Ok(Err(err)),
            };

            let txn_response = txn_sender.send_txn(self, txn).await?;
            match txn_response {
                IdempotentKVTxnResponse::Success(_) | IdempotentKVTxnResponse::AlreadyCommitted => {
                    return Ok(Ok(Some(*ident.name())));
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
    ) -> Result<Option<MVId>, MetaTxnError> {
        let mut trials = txn_backoff(None, "drop_materialized_view");

        loop {
            trials.next().unwrap()?.await;
            let Some(txn) = build_drop_materialized_view_txn(self, ident).await? else {
                return Ok(None);
            };
            let (succ, _) = send_txn(self, txn).await?;
            if succ {
                return Ok(Some(*ident.name()));
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
