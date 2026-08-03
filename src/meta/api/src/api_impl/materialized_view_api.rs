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

//! An MV reuses ordinary `TableMeta`; its table ID is also its MV ID.
//!
//! ```text
//! __fd_table_by_id/<mv_id>
//!     -> TableMeta
//!        options["materialized_view_source_table_id"] = <source_id>
//! __fd_materialized_view_definition/<tenant>/<mv_id>
//!     -> MVDefinition
//! __fd_materialized_view_by_source/<tenant>/<source_id>/<mv_id>
//!     -> MVSourceBinding { bound_source_generation }
//! __fd_materialized_view_source_binding_version/<tenant>/<source_id>
//!     -> MVSourceBindingVersion { current_source_generation }
//! ```
//!
//! `SourceTableMVIdent` cannot use `EmptyProto`: key existence would express
//! both dependency and validity, forcing source DDL to delete every edge and
//! losing the invalid dependencies needed by management and refresh. Instead,
//! it is a durable dependency edge whose value records the generation to which
//! the MV is bound. Its binding is valid when
//! `bound_source_generation == current_source_generation`. Source DDL defined
//! to invalidate existing MVs can therefore do so in O(1) by advancing only
//! the source generation. DDL that preserves existing bindings does not touch
//! this metadata.
//!
//! The version value cannot be `EmptyProto` with its KV sequence used as the
//! generation. A missing key is observed as generation 0, but the first put
//! receives a nonzero KV sequence, so an edge atomically bound to 0 would be
//! stale immediately. Storing the semantic generation explicitly lets the
//! first CREATE publish version 0 and its edge in one transaction; the KV
//! sequence remains an internal CAS token.
//!
//! The list APIs expose the two dependency views explicitly:
//!
//! - [`MaterializedViewApi::list_mvs_by_source_table_id`] returns every edge,
//!   including invalid ones, for management and lifecycle operations.
//! - [`MaterializedViewApi::get_mv_source_binding_snapshot`] reads the source
//!   generation before and after collecting matching edges.
//!
//! `CreateMaterializedViewMeta::expected_source_generation` provides the same
//! fence for CREATE. `create_table` compares it with the current value and uses
//! the version record's KV sequence only as an internal CAS token. A missing
//! record means generation 0 and is initialized atomically with the first MV.
//! The record remains until source GC to prevent generation ABA.
//!
//! Source-DDL and refresh integration are follow-up work. The required metadata
//! operations are:
//!
//! ```text
//! CREATE MV txn:
//!     assert current_source_generation == expected_source_generation
//!     initialize MVSourceBindingVersion { current_source_generation: 0 }
//!         if the version record is missing
//!     put TableId(new_mv_id) = new_mv_table_meta
//!     put MVDefinitionIdent(tenant, new_mv_id) = definition
//!     put SourceTableMVIdent(tenant, source_id, new_mv_id) =
//!         MVSourceBinding { bound_source_generation: expected_source_generation }
//!
//! REPLACE MV txn:
//!     mark TableId(old_mv_id) dropped
//!     delete MVDefinitionIdent(tenant, old_mv_id)
//!     delete SourceTableMVIdent(tenant, old_source_id, old_mv_id)
//!     append the CREATE MV operations for new_mv_id
//!
//! DROP MV txn:
//!     mark TableId(mv_id) dropped
//!     delete MVDefinitionIdent(tenant, mv_id)
//!     delete SourceTableMVIdent(tenant, source_id, mv_id)
//!
//! REFRESH MV txn:
//!     assert current_source_generation == refresh_source_generation
//!     update the MV snapshot in TableId(mv_id)
//!     replace SourceTableMVIdent(tenant, source_id, mv_id) =
//!         MVSourceBinding { bound_source_generation: refresh_source_generation }
//!
//! RENAME/DROP/MODIFY COLUMN ON SOURCE txn:
//!     read MVSourceBindingVersion (missing means generation 0)
//!     replace it with current_source_generation + 1 using its KV seq as CAS
//!     update the source TableMeta
//!     keep SourceTableMVIdent(tenant, source_id, *)
//!
//! ADD COLUMN ON SOURCE txn:
//!     update the source TableMeta
//!     no MV binding metadata changes
//!     keep SourceTableMVIdent(tenant, source_id, *)
//!
//! RENAME SOURCE TABLE txn:
//!     update the source table-name mappings
//!     no MV binding metadata changes
//!     keep SourceTableMVIdent(tenant, source_id, *)
//!
//! DROP SOURCE TABLE txn:
//!     update the ordinary source-table lifecycle metadata
//!     no MV binding metadata changes
//!     keep SourceTableMVIdent(tenant, source_id, *)
//!
//! UNDROP SOURCE TABLE txn:
//!     update the ordinary source-table lifecycle metadata
//!     no MV binding metadata changes
//!     keep SourceTableMVIdent(tenant, source_id, *)
//!
//! GC MV txn:
//!     delete MVDefinitionIdent(tenant, mv_id)
//!     delete SourceTableMVIdent(tenant, source_id, mv_id)
//!     delete the ordinary table metadata
//!
//! GC source txn:
//!     delete SourceTableMVIdent(tenant, source_id, *)
//!     delete MVSourceBindingVersionIdent(tenant, source_id)
//! ```

use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::MVDefinitionIdent;
use databend_common_meta_app::schema::MVInfo;
use databend_common_meta_app::schema::MVSourceBindingVersionIdent;
use databend_common_meta_app::schema::SourceTableMV;
use databend_common_meta_app::schema::SourceTableMVIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::DirName;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnGetResponse;
use databend_meta_client::types::protobuf as pb;
use log::warn;

use crate::deserialize_struct_get_response;
use crate::kv_pb_api::KVPbApi;

/// A generation-consistent view of active MV bindings for one source table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVSourceBindingSnapshot {
    /// Source binding generation at which `materialized_views` was collected.
    pub generation: u64,
    /// Empty when the generation changed while MV metadata was being collected.
    pub materialized_views: Vec<MVInfo>,
}

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

    /// List every MV that depends on a source table, including invalid MVs.
    ///
    /// This unfiltered view is intended for management, SHOW, GC, and refresh
    /// discovery. SELECT optimization and source-table maintenance must use
    /// [`MaterializedViewApi::get_mv_source_binding_snapshot`] instead.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_mvs_by_source_table_id(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<Vec<MVInfo>, MetaError> {
        list_mvs_by_source_table_id_impl(self, tenant, source_table_id, None).await
    }

    /// Get MVs valid at a stable source binding generation.
    ///
    /// The generation is read before and after collecting MV metadata. If it
    /// changes during collection, the returned MV list is empty and
    /// `generation` contains the final observed value. A caller that later
    /// writes data must still compare this generation when committing.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_mv_source_binding_snapshot(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<MVSourceBindingSnapshot, MetaError> {
        let generation_before = get_mv_source_generation(self, tenant, source_table_id).await?;
        let mut materialized_views = list_mvs_by_source_table_id_impl(
            self,
            tenant,
            source_table_id,
            Some(generation_before),
        )
        .await?;
        let generation_after = get_mv_source_generation(self, tenant, source_table_id).await?;

        if generation_before != generation_after {
            materialized_views.clear();
        }

        Ok(MVSourceBindingSnapshot {
            generation: generation_after,
            materialized_views,
        })
    }
}

async fn get_mv_source_generation<KV>(
    kv_api: &KV,
    tenant: &Tenant,
    source_table_id: u64,
) -> Result<u64, MetaError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    let generation_ident = MVSourceBindingVersionIdent::new(tenant, source_table_id);
    Ok(kv_api
        .get_pb(&generation_ident)
        .await?
        .map(|record| record.data.current_source_generation)
        .unwrap_or(0))
}

async fn list_mvs_by_source_table_id_impl<KV>(
    kv_api: &KV,
    tenant: &Tenant,
    source_table_id: u64,
    expected_source_generation: Option<u64>,
) -> Result<Vec<MVInfo>, MetaError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    let source_mv_prefix = DirName::new(SourceTableMVIdent::new_generic(
        tenant,
        SourceTableMV::new(source_table_id, 0),
    ));
    let source_mvs = kv_api
        .list_pb_vec(ListOptions::unlimited(&source_mv_prefix))
        .await?;
    let mv_ids = source_mvs
        .iter()
        .filter_map(|(ident, binding)| {
            expected_source_generation
                .is_none_or(|expected| binding.data.bound_source_generation == expected)
                .then_some(ident.name().mv_table_id)
        })
        .collect::<Vec<_>>();
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

    let values = kv_api.mget_kv(&keys).await?;
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
        mvs.push(MVInfo {
            mv_id: *mv_id,
            definition,
            table_meta,
        });
    }

    Ok(mvs)
}

impl<KV> MaterializedViewApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
