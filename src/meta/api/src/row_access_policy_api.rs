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

use databend_common_meta_app::row_access_policy::row_access_policy_name_ident;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReply;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReq;
use databend_common_meta_app::row_access_policy::RowAccessPolicyId;
use databend_common_meta_app::row_access_policy::RowAccessPolicyMeta;
use databend_common_meta_app::row_access_policy::RowAccessPolicyNameIdent;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;

use crate::meta_txn_error::MetaTxnError;

#[async_trait::async_trait]
pub trait RowAccessPolicyApi: Send + Sync {
    async fn create_row_access(
        &self,
        req: CreateRowAccessPolicyReq,
    ) -> Result<
        Result<CreateRowAccessPolicyReply, ExistError<row_access_policy_name_ident::Resource>>,
        MetaTxnError,
    >;

    /// On success, returns the dropped id and row policy.
    /// Returning None, means nothing is removed.
    async fn drop_row_access(
        &self,
        name_ident: &RowAccessPolicyNameIdent,
    ) -> Result<Option<(SeqV<RowAccessPolicyId>, SeqV<RowAccessPolicyMeta>)>, MetaTxnError>;

    async fn get_row_access(
        &self,
        name_ident: &RowAccessPolicyNameIdent,
    ) -> Result<Option<(SeqV<RowAccessPolicyId>, SeqV<RowAccessPolicyMeta>)>, MetaError>;
}
