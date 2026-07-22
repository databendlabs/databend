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
use log::warn;

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

    /// Get complete MV definitions and table metadata by source table ID.
    ///
    /// The source index is read first, then all definitions and TableMeta records
    /// are fetched in one `mget_kv` request. An MV is omitted if either record no
    /// longer exists.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn mget_mvs_by_source_table_id(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<Vec<(u64, SeqV<MVDefinition>, SeqV<TableMeta>)>, MetaError> {
        let source_ident = SourceTableMVIdsIdent::new(tenant, source_table_id);
        let Some(source_mv_ids) = self.get_pb(&source_ident).await? else {
            return Ok(vec![]);
        };
        let mv_ids = source_mv_ids.data.mv_ids();
        if mv_ids.is_empty() {
            return Ok(vec![]);
        }

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
            let (_, definition) =
                deserialize_struct_get_response::<MVDefinitionIdent>(definition_response)?;
            let (_, table_meta) = deserialize_struct_get_response::<TableId>(table_meta_response)?;

            let (Some(definition), Some(table_meta)) = (definition, table_meta) else {
                warn!(
                    "source table {} references MV {} with incomplete metadata",
                    source_table_id, mv_id
                );
                continue;
            };
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
