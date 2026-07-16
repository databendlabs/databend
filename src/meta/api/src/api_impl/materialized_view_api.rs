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

use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::MVDefinitionIdent;
use databend_common_meta_app::schema::SourceTableMVIds;
use databend_common_meta_app::schema::SourceTableMVIdsIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnGetResponse;
use databend_meta_client::types::protobuf as pb;

use crate::deserialize_struct_get_response;
use crate::kv_pb_api::KVPbApi;

/// APIs for metadata that belongs exclusively to materialized views.
#[async_trait::async_trait]
pub trait MaterializedViewApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_mv_definition(
        &self,
        tenant: &Tenant,
        mv_table_id: u64,
    ) -> Result<Option<SeqV<MVDefinition>>, MetaError> {
        let ident = MVDefinitionIdent::new(tenant, mv_table_id);
        self.get_pb(&ident).await
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_mv_ids_by_source_table_id(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<Option<SeqV<SourceTableMVIds>>, MetaError> {
        let ident = SourceTableMVIdsIdent::new(tenant, source_table_id);
        self.get_pb(&ident).await
    }

    /// Batch get MV definitions and table metadata in one `mget_kv` request.
    ///
    /// Returns one entry for every input MV ID, in the same order. The definition
    /// and TableMeta are independently `None` when the corresponding KV record
    /// does not exist.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn mget_mvs(
        &self,
        tenant: &Tenant,
        mv_ids: &[u64],
    ) -> Result<Vec<(u64, Option<SeqV<MVDefinition>>, Option<SeqV<TableMeta>>)>, MetaError> {
        let mut keys = Vec::with_capacity(mv_ids.len() * 2);
        keys.extend(
            mv_ids
                .iter()
                .map(|mv_id| MVDefinitionIdent::new(tenant, *mv_id).to_string_key()),
        );
        keys.extend(
            mv_ids
                .iter()
                .map(|mv_id| TableId::new(*mv_id).to_string_key()),
        );

        let values = self.mget_kv(&keys).await?;
        let mut responses = keys
            .iter()
            .zip(values)
            .map(|(key, value)| TxnGetResponse::new(key, value.map(pb::SeqV::from)))
            .collect::<Vec<_>>();
        let table_meta_responses = responses.split_off(mv_ids.len());
        let definition_responses = responses;
        let mut mvs = Vec::with_capacity(mv_ids.len());

        for (mv_id, (definition_response, table_meta_response)) in mv_ids
            .iter()
            .zip(definition_responses.into_iter().zip(table_meta_responses))
        {
            let (definition_ident, definition) =
                deserialize_struct_get_response::<MVDefinitionIdent>(definition_response)?;
            let (table_ident, table_meta) =
                deserialize_struct_get_response::<TableId>(table_meta_response)?;
            assert_eq!(definition_ident, MVDefinitionIdent::new(tenant, *mv_id));
            assert_eq!(table_ident, TableId::new(*mv_id));

            mvs.push((*mv_id, definition, table_meta));
        }

        Ok(mvs)
    }
}

impl<KV> MaterializedViewApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
