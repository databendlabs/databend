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

use databend_common_expression::AutoIncrementExpr;
use databend_common_meta_app::principal::AutoIncrementKey;
use databend_common_meta_app::schema::AutoIncrementStorageIdent;
use databend_common_meta_app::tenant::ToTenant;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_types::MetaError;
use databend_meta_types::TxnOp;
use databend_meta_types::TxnRequest;
use databend_meta_types::anyerror::func_name;
use databend_meta_types::protobuf::FetchIncreaseU64Response;
use log::debug;

use super::errors::AutoIncrementError;
use crate::meta_txn_error::MetaTxnError;
use crate::send_txn;

/// The implementation of `next_val` for sequence number.
pub(crate) struct NextVal<'a, KV>
where KV: kvapi::KVApi<Error = MetaError> + ?Sized
{
    pub(crate) kv_api: &'a KV,
    pub(crate) key: AutoIncrementKey,
    pub(crate) expr: AutoIncrementExpr,
}

impl<'a, KV> NextVal<'a, KV>
where KV: kvapi::KVApi<Error = MetaError> + ?Sized
{
    /// AutoIncrement number stores the value in standalone key that support `FetchAddU64`.
    pub(crate) async fn next_val(
        self,
        tenant: impl ToTenant,
        count: u64,
    ) -> Result<Result<FetchIncreaseU64Response, AutoIncrementError>, MetaTxnError> {
        debug!("{}", func_name!());

        // Key for the sequence number value.
        let storage_ident = AutoIncrementStorageIdent::new_generic(tenant, self.key.clone());
        let storage_key = storage_ident.to_string_key();

        let delta = count * self.expr.step as u64;

        let txn = TxnRequest::new(vec![], vec![TxnOp::fetch_add_u64(
            &storage_key,
            delta as i64,
        )]);

        let (succ, responses) = send_txn(self.kv_api, txn).await?;

        debug!(
            "{} txn result: succ: {succ}, ident: {}, update seq by {delta}",
            func_name!(),
            self.key,
        );
        debug_assert!(succ);

        let resp = responses[0].try_as_fetch_increase_u64().unwrap();
        let got_delta = resp.delta();

        if got_delta < delta {
            return Ok(Err(AutoIncrementError::OutOfAutoIncrementRange {
                key: self.key,
                context: format!(
                    "{}: count: {count}; expected delta: {delta}, but got: {resp}",
                    self.expr.step,
                ),
            }));
        }

        Ok(Ok(resp.clone()))
    }
}
