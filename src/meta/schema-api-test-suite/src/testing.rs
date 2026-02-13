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

//! Supporting utilities for tests.

use databend_common_meta_api::deserialize_struct;
use databend_common_meta_api::kv_app_error::KVAppError;
use databend_common_meta_api::serialization_util::deserialize_u64;
use databend_common_proto_conv::FromToProto;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_types::MetaAPIError;
use databend_meta_types::MetaDataError;
use databend_meta_types::MetaDataReadError;
use databend_meta_types::MetaError;
use databend_meta_types::anyerror::AnyError;

/// Get existing value by key. Panic if key is absent.
pub(crate) async fn get_kv_data<T>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &impl kvapi::Key,
) -> Result<T, KVAppError>
where
    T: FromToProto,
{
    let res = kv_api.get_kv(&key.to_string_key()).await?;
    if let Some(res) = res {
        let s = deserialize_struct(&res.data)?;
        return Ok(s);
    };

    Err(KVAppError::MetaError(MetaError::APIError(
        MetaAPIError::DataError(MetaDataError::ReadError(MetaDataReadError::new(
            "get_kv_data",
            "not found",
            &AnyError::error(""),
        ))),
    )))
}

/// Get existing u64 value by key. Panic if key is absent.
pub(crate) async fn get_kv_u64_data(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &impl kvapi::Key,
) -> Result<u64, KVAppError> {
    let res = kv_api.get_kv(&key.to_string_key()).await?;
    if let Some(res) = res {
        let s = deserialize_u64(&res.data)?;
        return Ok(*s);
    };

    Err(KVAppError::MetaError(MetaError::APIError(
        MetaAPIError::DataError(MetaDataError::ReadError(MetaDataReadError::new(
            "get_kv_u64_data",
            "not found",
            &AnyError::error(""),
        ))),
    )))
}
