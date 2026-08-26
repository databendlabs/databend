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
use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::types::InvalidReply;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::MetaNetworkError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnGetResponse;

use crate::deserialize_struct;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;

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
