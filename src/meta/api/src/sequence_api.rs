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

use databend_common_meta_app::schema::CreateSequenceReply;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::DropSequenceReply;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::schema::SequenceMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;

use crate::kv_app_error::KVAppError;

#[async_trait::async_trait]
pub trait SequenceApi: Send + Sync {
    async fn create_sequence(
        &self,
        req: CreateSequenceReq,
    ) -> Result<CreateSequenceReply, KVAppError>;

    async fn get_sequence(
        &self,
        req: &SequenceIdent,
    ) -> Result<Option<SeqV<SequenceMeta>>, MetaError>;

    async fn list_sequences(
        &self,
        tenant: &Tenant,
    ) -> Result<Vec<(String, SequenceMeta)>, MetaError>;

    async fn get_sequence_next_value(
        &self,
        req: GetSequenceNextValueReq,
    ) -> Result<GetSequenceNextValueReply, KVAppError>;

    async fn drop_sequence(&self, req: DropSequenceReq) -> Result<DropSequenceReply, KVAppError>;
}
