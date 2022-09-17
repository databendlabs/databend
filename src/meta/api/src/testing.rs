//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! Supporting utilities for tests.

use common_meta_types::anyerror::AnyError;
use common_meta_types::KVAppError;
use common_meta_types::MetaAPIError;
use common_meta_types::MetaDataError;
use common_meta_types::MetaDataReadError;
use common_proto_conv::FromToProto;

use crate::KVApi;
use crate::KVApiKey;

/// Get existing value by key. Panic if key is absent.
pub(crate) async fn get_kv_data<T>(
    kv_api: &(impl KVApi + ?Sized),
    key: &impl KVApiKey,
) -> Result<T, KVAppError>
where
    T: FromToProto,
    T::PB: common_protos::prost::Message + Default,
{
    let res = kv_api.get_kv(&key.to_key()).await?;
    if let Some(res) = res {
        let s = crate::deserialize_struct(&res.data)?;
        return Ok(s);
    };

    Err(KVAppError::APIError(MetaAPIError::DataError(
        MetaDataError::ReadError(MetaDataReadError::new(
            "get_kv_data",
            "not found",
            &AnyError::error(""),
        )),
    )))
}
