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

use databend_common_meta_app::data_mask::CreateDatamaskReply;
use databend_common_meta_app::data_mask::CreateDatamaskReq;
use databend_common_meta_app::data_mask::DataMaskId;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::data_mask::DatamaskMeta;
use databend_common_meta_app::data_mask::data_mask_name_ident;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;

use super::errors::MaskingPolicyError;
use crate::meta_txn_error::MetaTxnError;

#[async_trait::async_trait]
pub trait DatamaskApi: Send + Sync {
    async fn create_data_mask(
        &self,
        req: CreateDatamaskReq,
    ) -> Result<Result<CreateDatamaskReply, ExistError<data_mask_name_ident::Resource>>, MetaError>;

    /// On success, returns the dropped id and data mask.
    /// Returning None, means nothing is removed.
    async fn drop_data_mask(
        &self,
        name_ident: &DataMaskNameIdent,
    ) -> Result<
        Result<Option<(SeqV<DataMaskId>, SeqV<DatamaskMeta>)>, MaskingPolicyError>,
        MetaTxnError,
    >;

    async fn get_data_mask(
        &self,
        name_ident: &DataMaskNameIdent,
    ) -> Result<Option<SeqV<DatamaskMeta>>, MetaError>;

    async fn get_data_mask_id(
        &self,
        name_ident: &DataMaskNameIdent,
    ) -> Result<Option<SeqV<DataMaskId>>, MetaError>;

    async fn get_data_mask_name_by_id(
        &self,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<Option<String>, MetaError>;

    async fn get_data_mask_by_id(
        &self,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<Option<SeqV<DatamaskMeta>>, MetaError>;
}
