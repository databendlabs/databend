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

use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::id_generator::IdGeneratorValue;
use databend_common_meta_app::primitive::Id;
use databend_common_proto_conv::FromToProto;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::InvalidReply;
use databend_meta_types::MetaError;
use databend_meta_types::MetaNetworkError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnGetResponse;
use databend_meta_types::errors;
use futures::TryStreamExt;

use crate::deserialize_struct;
use crate::deserialize_u64;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;

/// Get u64 value by key.
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
    let res = kv_api
        .list_kv_collect(ListOptions::unlimited(&key.to_string_key()))
        .await?;

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

/// Batch get u64 values by keys.
///
/// Returns a vec of Option<u64> in the same order as input keys.
/// None means the key does not exist.
pub async fn mget_u64_values<K: kvapi::Key>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    keys: &[K],
) -> Result<Vec<Option<u64>>, MetaError> {
    if keys.is_empty() {
        return Ok(vec![]);
    }

    let str_keys: Vec<String> = keys.iter().map(|k| k.to_string_key()).collect();
    let mut strm = kv_api.get_kv_stream(&str_keys).await?;

    let mut results = Vec::with_capacity(keys.len());
    while let Some(item) = strm.try_next().await? {
        if let Some(seq_v) = item.value {
            let id = *deserialize_u64(&seq_v.data)?;
            results.push(Some(id));
        } else {
            results.push(None);
        }
    }

    if results.len() != keys.len() {
        return Err(
            errors::IncompleteStream::new(keys.len() as u64, results.len() as u64)
                .context(" while mget_u64_values")
                .into(),
        );
    }

    Ok(results)
}

/// Generate an id on metasrv.
///
/// Ids are categorized by generators.
/// Ids may not be consecutive.
pub async fn fetch_id(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    generator: IdGenerator,
) -> Result<u64, MetaError> {
    // Each `upsert` bumps the seq atomically inside metasrv, therefore every caller
    // receives a unique, monotonically increasing id even when multiple sessions
    // fetch from the same generator concurrently.
    let res = kv_api
        .upsert_pb(&UpsertPB::update(generator, IdGeneratorValue))
        .await?;

    // seq: MatchSeq::Any always succeeds
    let seq_v = res.result.unwrap();
    Ok(seq_v.seq)
}
