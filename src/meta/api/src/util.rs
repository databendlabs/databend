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
use std::fmt::Display;
use std::time::Duration;

use databend_common_base::base::uuid::Uuid;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::UnknownDatabase;
use databend_common_meta_app::app_error::UnknownDatabaseId;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::primitive::Id;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::anyerror::AnyError;
use databend_common_meta_types::txn_condition::Target;
use databend_common_meta_types::txn_op_response::Response;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::InvalidArgument;
use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnGetResponse;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnOpResponse;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use databend_common_proto_conv::FromToProto;
use display_more::DisplaySliceExt;
use log::debug;
use log::info;

use crate::kv_app_error::KVAppError;
use crate::reply::unpack_txn_reply;

pub const DEFAULT_MGET_SIZE: usize = 256;

/// Get value that its type is `u64`.
///
/// It expects the kv-value's type is `u64`, such as:
/// `__fd_table/<db_id>/<table_name> -> (seq, table_id)`,
/// `__fd_database/<tenant>/<db_name> -> (seq, db_id)`, or
/// `__fd_index/<tenant>/<index_name> -> (seq, index_id)`.
///
/// It returns (seq, `u64` value).
/// If not found, (0,0) is returned.
pub async fn get_u64_value<T: kvapi::Key>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &T,
) -> Result<(u64, u64), MetaError> {
    let res = kv_api.get_kv(&key.to_string_key()).await?;

    if let Some(seq_v) = res {
        Ok((seq_v.seq, *deserialize_u64(&seq_v.data)?))
    } else {
        Ok((0, 0))
    }
}

#[allow(clippy::type_complexity)]
pub fn deserialize_struct_get_response<K>(
    resp: TxnGetResponse,
) -> Result<(K, Option<SeqV<K::ValueType>>), MetaError>
where
    K: kvapi::Key,
    K::ValueType: FromToProto,
{
    let key = K::from_str_key(&resp.key).map_err(|e| {
        let inv = InvalidReply::new(
            format!("fail to parse {} key, {}", type_name::<K>(), resp.key),
            &e,
        );
        MetaNetworkError::InvalidReply(inv)
    })?;

    if let Some(pb_seqv) = resp.value {
        let seqv = SeqV::from(pb_seqv);
        let value = deserialize_struct::<K::ValueType>(&seqv.data)?;
        let seqv = SeqV::new_with_meta(seqv.seq, seqv.meta, value);
        Ok((key, Some(seqv)))
    } else {
        Ok((key, None))
    }
}

pub fn deserialize_id_get_response<K>(
    resp: TxnGetResponse,
) -> Result<(K, Option<SeqV<Id>>), MetaError>
where K: kvapi::Key {
    let key = K::from_str_key(&resp.key).map_err(|e| {
        let inv = InvalidReply::new(
            format!("fail to parse {} key, {}", type_name::<K>(), resp.key),
            &e,
        );
        MetaNetworkError::InvalidReply(inv)
    })?;

    if let Some(pb_seqv) = resp.value {
        let seqv = SeqV::from(pb_seqv);
        let id = deserialize_u64(&seqv.data)?;
        let seqv = SeqV::new_with_meta(seqv.seq, seqv.meta, id);
        Ok((key, Some(seqv)))
    } else {
        Ok((key, None))
    }
}

/// Batch get values that are encoded with FromToProto.
pub async fn mget_pb_values<T>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    keys: &[String],
) -> Result<Vec<(u64, Option<T>)>, MetaError>
where
    T: FromToProto,
{
    let seq_bytes = kv_api.mget_kv(keys).await?;
    let mut seq_values = Vec::with_capacity(keys.len());
    for seq_v in seq_bytes {
        if let Some(seq_v) = seq_v {
            let seq = seq_v.seq;
            let v = deserialize_struct(&seq_v.data)?;
            seq_values.push((seq, Some(v)))
        } else {
            seq_values.push((0, None));
        }
    }

    Ok(seq_values)
}

/// List kvs whose value's type is `u64`.
///
/// It expects the kv-value' type is `u64`, such as:
/// `__fd_table/<db_id>/<table_name> -> (seq, table_id)`,
/// `__fd_database/<tenant>/<db_name> -> (seq, db_id)`, or
/// `__fd_index/<tenant>/<index_name> -> (seq, index_id)`.
///
/// It returns a vec of structured key(such as DatabaseNameIdent) and a vec of `u64`.
pub async fn list_u64_value<K: kvapi::Key>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &K,
) -> Result<(Vec<K>, Vec<u64>), MetaError> {
    let res = kv_api.list_kv_collect(&key.to_string_key()).await?;

    let n = res.len();

    let mut structured_keys = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);

    for (str_key, seqv) in res.iter() {
        let id = *deserialize_u64(&seqv.data)?;
        values.push(id);

        // Parse key
        let struct_key = K::from_str_key(str_key).map_err(|e| {
            let inv = InvalidReply::new("list_u64_value", &e);
            MetaNetworkError::InvalidReply(inv)
        })?;
        structured_keys.push(struct_key);
    }

    Ok((structured_keys, values))
}

pub fn serialize_u64(value: impl Into<Id>) -> Result<Vec<u8>, MetaNetworkError> {
    let v = serde_json::to_vec(&*value.into()).map_err(|e| {
        let inv = InvalidArgument::new(e, "");
        MetaNetworkError::InvalidArgument(inv)
    })?;
    Ok(v)
}

pub fn deserialize_u64(v: &[u8]) -> Result<Id, MetaNetworkError> {
    let id = serde_json::from_slice(v).map_err(|e| {
        let inv = InvalidReply::new("", &e);
        MetaNetworkError::InvalidReply(inv)
    })?;
    Ok(Id::new(id))
}

/// Generate an id on metasrv.
///
/// Ids are categorized by generators.
/// Ids may not be consecutive.
pub async fn fetch_id<T: kvapi::Key>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    generator: T,
) -> Result<u64, MetaError> {
    let res = kv_api
        .upsert_kv(UpsertKV::update(generator.to_string_key(), b""))
        .await?;

    // seq: MatchSeq::Any always succeeds
    let seq_v = res.result.unwrap();
    Ok(seq_v.seq)
}

pub fn serialize_struct<T>(value: &T) -> Result<Vec<u8>, MetaNetworkError>
where T: FromToProto + 'static {
    let p = value.to_pb().map_err(|e| {
        let inv = InvalidArgument::new(e, "");
        MetaNetworkError::InvalidArgument(inv)
    })?;
    let mut buf = vec![];
    prost::Message::encode(&p, &mut buf).map_err(|e| {
        let inv = InvalidArgument::new(e, "");
        MetaNetworkError::InvalidArgument(inv)
    })?;
    Ok(buf)
}

pub fn deserialize_struct<T>(buf: &[u8]) -> Result<T, MetaNetworkError>
where T: FromToProto {
    let p: T::PB = prost::Message::decode(buf).map_err(|e| {
        let inv = InvalidReply::new("", &e);
        MetaNetworkError::InvalidReply(inv)
    })?;
    let v: T = FromToProto::from_pb(p).map_err(|e| {
        let inv = InvalidReply::new("", &e);
        MetaNetworkError::InvalidReply(inv)
    })?;

    Ok(v)
}

pub async fn send_txn(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    txn_req: TxnRequest,
) -> Result<(bool, Vec<TxnOpResponse>), MetaError> {
    debug!("send txn: {}", txn_req);
    let tx_reply = kv_api.transaction(txn_req).await?;
    let (succ, responses) = unpack_txn_reply(tx_reply);
    debug!("txn success: {}: {}", succ, responses.display_n(20));
    Ok((succ, responses))
}

/// Add a delete operation by key and exact seq to [`TxnRequest`].
pub fn txn_delete_exact(txn: &mut TxnRequest, key: &impl kvapi::Key, seq: u64) {
    txn.condition.push(txn_cond_eq_seq(key, seq));
    txn.if_then.push(txn_op_del(key));
}

/// Add a replace operation by key and exact seq to [`TxnRequest`].
pub fn txn_replace_exact<K>(
    txn: &mut TxnRequest,
    key: &K,
    seq: u64,
    value: &K::ValueType,
) -> Result<(), InvalidArgument>
where
    K: kvapi::Key,
    K::ValueType: FromToProto + 'static,
{
    txn.condition.push(txn_cond_eq_seq(key, seq));
    txn.if_then.push(txn_op_put_pb(key, value, None)?);

    Ok(())
}

/// Build a TxnCondition that compares the seq of a record.
pub fn txn_cond_eq_seq(key: &impl kvapi::Key, seq: u64) -> TxnCondition {
    TxnCondition::eq_seq(key.to_string_key(), seq)
}

/// Build a TxnCondition that compares the seq of a record.
pub fn txn_cond_seq(key: &impl kvapi::Key, op: ConditionResult, seq: u64) -> TxnCondition {
    TxnCondition {
        key: key.to_string_key(),
        expected: op as i32,
        target: Some(Target::Seq(seq)),
    }
}

pub fn txn_put_pb<K>(key: &K, value: &K::ValueType) -> Result<TxnOp, InvalidArgument>
where
    K: kvapi::Key,
    K::ValueType: FromToProto + 'static,
{
    let p = value.to_pb().map_err(|e| InvalidArgument::new(e, ""))?;

    let mut buf = vec![];
    prost::Message::encode(&p, &mut buf).map_err(|e| InvalidArgument::new(e, ""))?;

    Ok(TxnOp::put(key.to_string_key(), buf))
}

/// Deprecate this. Replace it with `txn_put_pb().with_ttl()`
pub fn txn_op_put_pb<K>(
    key: &K,
    value: &K::ValueType,
    ttl: Option<Duration>,
) -> Result<TxnOp, InvalidArgument>
where
    K: kvapi::Key,
    K::ValueType: FromToProto + 'static,
{
    let p = value.to_pb().map_err(|e| InvalidArgument::new(e, ""))?;

    let mut buf = vec![];
    prost::Message::encode(&p, &mut buf).map_err(|e| InvalidArgument::new(e, ""))?;

    Ok(TxnOp::put_with_ttl(key.to_string_key(), buf, ttl))
}

/// Build a txn operation that puts a record.
pub fn txn_op_put(key: &impl kvapi::Key, value: Vec<u8>) -> TxnOp {
    TxnOp::put(key.to_string_key(), value)
}

/// Build a txn operation that gets value by key.
pub fn txn_op_get(key: &impl kvapi::Key) -> TxnOp {
    TxnOp::get(key.to_string_key())
}

/// Build a txn operation that deletes a record.
pub fn txn_op_del(key: &impl kvapi::Key) -> TxnOp {
    TxnOp::delete(key.to_string_key())
}

/// Return OK if a db_id or db_meta exists by checking the seq.
///
/// Otherwise returns UnknownDatabase error
pub fn db_has_to_exist(
    seq: u64,
    db_name_ident: &DatabaseNameIdent,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq = seq, db_name_ident :? =(db_name_ident); "db does not exist");

        Err(KVAppError::AppError(unknown_database_error(
            db_name_ident,
            msg,
        )))
    } else {
        Ok(())
    }
}

pub fn unknown_database_error(db_name_ident: &DatabaseNameIdent, msg: impl Display) -> AppError {
    let e = UnknownDatabase::new(
        db_name_ident.database_name(),
        format!("{}: {}", msg, db_name_ident.display()),
    );

    AppError::UnknownDatabase(e)
}

/// Return OK if a db_id to db_meta exists by checking the seq.
///
/// Otherwise returns UnknownDatabaseId error
pub fn db_id_has_to_exist(seq: u64, db_id: u64, msg: impl Display) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq = seq, db_name_ident :? =(db_id); "db_id does not exist");

        let app_err = AppError::UnknownDatabaseId(UnknownDatabaseId::new(
            db_id,
            format!("{}: {}", msg, db_id),
        ));

        Err(KVAppError::AppError(app_err))
    } else {
        Ok(())
    }
}

/// Return OK if a `table_name_ident->*` exists by checking the seq.
///
/// Otherwise returns [`AppError::UnknownTable`] error
pub fn assert_table_exist(
    seq: u64,
    name_ident: &TableNameIdent,
    ctx: impl Display,
) -> Result<(), AppError> {
    if seq > 0 {
        return Ok(());
    }

    debug!(seq = seq, name_ident :? =(name_ident); "does not exist");

    Err(UnknownTable::new(
        &name_ident.table_name,
        format!("{}: {}", ctx, name_ident),
    ))?
}

pub struct IdempotentKVTxnSender {
    txn_id: String,
    ttl: Duration,
}

pub enum IdempotentKVTxnResponse {
    Success(Vec<TxnOpResponse>),
    AlreadyCommitted,
    Failed(Vec<TxnOpResponse>),
}

impl IdempotentKVTxnSender {
    pub fn new() -> Self {
        let txn_id = Self::gen_txn_id();
        let ttl = Duration::from_secs(300);
        Self { txn_id, ttl }
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        let txn_id = Self::gen_txn_id();
        Self { txn_id, ttl }
    }

    fn gen_txn_id() -> String {
        let uuid = Uuid::now_v7().simple().to_string();
        format!("_txn_id/{}", uuid)
    }

    pub fn get_txn_id(&self) -> &str {
        &self.txn_id
    }

    pub async fn send_txn(
        &self,
        kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
        mut txn: TxnRequest,
    ) -> Result<IdempotentKVTxnResponse, MetaError> {
        // Wrap transaction with unique id
        txn.condition
            .push(TxnCondition::eq_seq(self.txn_id.clone(), 0));

        txn.if_then.push(TxnOp::put_with_ttl(
            self.txn_id.clone(),
            vec![],
            Some(self.ttl),
        ));

        txn.else_then.push(TxnOp::get(self.txn_id.clone()));

        match send_txn(kv_api, txn).await {
            Ok((true, mut then_op_responses)) => Ok({
                // Pop the put_with_ttl txn_id KV pair operation response
                then_op_responses.pop();
                IdempotentKVTxnResponse::Success(then_op_responses)
            }),
            Ok((false, mut else_op_responses)) => {
                // Since we manipulate the transaction, the last operation response SHOULD be the
                // get transaction ID operation response. Let's check it.
                let Some(last_op_resp) = else_op_responses.pop() else {
                    return Err(MetaError::from(InvalidReply::new(
                        "Malformed transaction response",
                        &AnyError::error(format!(
                            "Get transaction ID key response not found {}",
                            self.txn_id
                        )),
                    )));
                };

                let Some(response) = last_op_resp.response else {
                    return Err(MetaError::from(InvalidReply::new(
                        "Malformed get transaction ID response",
                        &AnyError::error(format!(
                            "Get transaction ID key {} operation responses nothing",
                            self.txn_id
                        )),
                    )));
                };

                let Response::Get(get_txn_id) = response else {
                    return Err(MetaError::from(InvalidReply::new(
                        "Malformed get transaction ID response",
                        &AnyError::error(format!(
                            "Expects TxnGetResponse of get transaction ID {}, but got {}",
                            self.txn_id, response
                        )),
                    )));
                };

                if get_txn_id.key != self.txn_id {
                    return Err(MetaError::from(InvalidReply::new(
                        "Transaction ID key mismatch",
                        &AnyError::error(format!(
                            "Transaction ID key mismatch, expects {}, got {}",
                            self.txn_id, get_txn_id.key
                        )),
                    )));
                }

                if get_txn_id.value.is_some() {
                    // The txn_id already exists, indicating some transaction with the same transaction
                    // ID was previously committed successfully.
                    //
                    // Notes about this "idempotency":
                    // 1. This method ALLOWS sending different transaction each time
                    //    while using the same txn_id for duplicate prevention.
                    // 2. It is the CALLER'S RESPONSIBILITY to ensure that all transactions
                    //    sharing the same txn_id do make sense, even if their actual content differs.
                    // 3. This is a best-effort idempotency check, only guaranteed roughly
                    //    within the TTL duration of the transaction ID KV pair.
                    // 4. DO NOT rely on this mechanism for critical safety properties.
                    //    The main purpose of this "idempotency check" is to help identify if this transaction
                    //    has been committed successfully during implicit transaction retry.
                    info!("Transaction with ID {} already exist", self.txn_id);
                    // Operation responses of else-branch are omitted in this case:
                    // Since `AlreadyCommitted` is somehow a "success" response, return the operation
                    // responses of else-branch might be misleading. Currently, no scenarios expect to use it.
                    Ok(IdempotentKVTxnResponse::AlreadyCommitted)
                } else {
                    info!(
                        "Transaction failed, and txn id {} does not present",
                        self.txn_id
                    );
                    // Return operation responses of else branch (last response has been removed)
                    Ok(IdempotentKVTxnResponse::Failed(else_op_responses))
                }
            }
            Err(e) => Err(e),
        }
    }
}

impl Default for IdempotentKVTxnSender {
    fn default() -> Self {
        Self::new()
    }
}
