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

use databend_common_meta_app::schema::GetAutoIncrementNextValueReply;
use databend_common_meta_app::schema::GetAutoIncrementNextValueReq;
use databend_meta_kvapi::kvapi;
use databend_meta_types::MetaError;
use fastrace::func_name;
use log::debug;

use super::auto_increment_api::AutoIncrementApi;
use super::auto_increment_nextval_impl::NextVal;
use super::errors::AutoIncrementError;
use crate::meta_txn_error::MetaTxnError;

#[async_trait::async_trait]
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError> + ?Sized> AutoIncrementApi for KV {
    async fn get_auto_increment_next_value(
        &self,
        req: GetAutoIncrementNextValueReq,
    ) -> Result<Result<GetAutoIncrementNextValueReply, AutoIncrementError>, MetaTxnError> {
        debug!(req :? =(&req); "AutoIncrementApi: {}", func_name!());

        let next_val = NextVal {
            kv_api: self,
            key: req.key.clone(),
            expr: req.expr.clone(),
        };

        let (start, end) = match next_val.next_val(&req.tenant, req.count).await? {
            Ok(resp) => (resp.before, resp.after),
            Err(err) => {
                return Ok(Err(err));
            }
        };

        Ok(Ok(GetAutoIncrementNextValueReply {
            start,
            step: req.expr.step,
            end,
        }))
    }
}
