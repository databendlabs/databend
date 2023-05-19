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

use common_meta_app::schema::CreateDatamaskReply;
use common_meta_app::schema::CreateDatamaskReq;
use common_meta_app::schema::DropDatamaskReply;
use common_meta_app::schema::DropDatamaskReq;
use common_meta_app::schema::GetDatamaskReply;
use common_meta_app::schema::GetDatamaskReq;

use crate::data_mask_api::DatamaskApi;
use crate::kv_app_error::KVAppError;

/// DatamaskApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls DatamaskApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> DatamaskApi for KV {
    async fn create_data_mask(
        &self,
        req: CreateDatamaskReq,
    ) -> Result<(CreateDatamaskReply, KVAppError)> {
    }

    async fn drop_data_mask(&self, req: DropDatamaskReq)
    -> Result<(DropDatamaskReply, KVAppError)>;

    async fn get_data_mask(&self, req: GetDatamaskReq) -> Result<(GetDatamaskReply, KVAppError)>;
}
