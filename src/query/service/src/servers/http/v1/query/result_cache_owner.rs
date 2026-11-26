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

//! The result cache key of an HTTP query, recorded under the user who ran it.
//!
//! An HTTP session is rebuilt on every request from the state that the client sends back, so the
//! result cache key of the last query, which `RESULT_SCAN` reads, must not come from that state.
//! When a query has a result cache key, the server records it in meta under the authenticated
//! tenant and user, and restoring the session reads it back for the same tenant and user.

use std::time::Duration;

use databend_common_exception::Result;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KeyBuilder;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::types::MatchSeq;
use databend_meta_client::types::MetaSpec;
use databend_meta_client::types::Operation;
use databend_meta_client::types::UpsertKV;

use crate::meta_service_error;

const PREFIX: &str = "_result_scan_owner";

/// Records `cache_key` as the result cache key of query `query_id` that `user` of `tenant` ran.
pub async fn save_result_cache_key(
    tenant: &Tenant,
    user: &str,
    query_id: &str,
    cache_key: &str,
    ttl: Duration,
) -> Result<()> {
    let kv_store = UserApiProvider::instance().get_meta_store_client();
    kv_store
        .upsert_kv(UpsertKV {
            key: owner_key(tenant, user, query_id),
            seq: MatchSeq::GE(0),
            value: Operation::Update(cache_key.as_bytes().to_vec()),
            value_meta: Some(MetaSpec::new_ttl(ttl)),
        })
        .await
        .map_err(meta_service_error)?;
    Ok(())
}

/// Returns the result cache key recorded for query `query_id` that `user` of `tenant` ran.
pub async fn load_result_cache_key(
    tenant: &Tenant,
    user: &str,
    query_id: &str,
) -> Result<Option<String>> {
    let kv_store = UserApiProvider::instance().get_meta_store_client();
    let seqv = kv_store
        .get_kv(&owner_key(tenant, user, query_id))
        .await
        .map_err(meta_service_error)?;
    Ok(seqv.map(|seqv| String::from_utf8_lossy(&seqv.data).into_owned()))
}

/// The user is part of the key, so a user can only read back the keys of their own queries,
/// even with the query id of another user's query.
fn owner_key(tenant: &Tenant, user: &str, query_id: &str) -> String {
    KeyBuilder::new_prefixed(PREFIX)
        .push_str(tenant.tenant_name())
        .push_str(user)
        .push_str(query_id)
        .done()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_owner_key() {
        let tenant = Tenant::new_literal("t1");

        assert_eq!(
            owner_key(&tenant, "u1", "q1"),
            "_result_scan_owner/t1/u1/q1"
        );

        // A "/" in the user name or the query id cannot move a key into another user's path.
        assert_ne!(
            owner_key(&tenant, "u1/u2", "q1"),
            owner_key(&tenant, "u1", "u2/q1")
        );
    }
}
