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
//! losing invalid dependencies needed by management and lifecycle cleanup.
//! Instead, it is a durable dependency edge whose value records the generation
//! to which the MV is bound. Its binding is valid when
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
//! The APIs expose the dependency views explicitly:
//!
//! - [`MaterializedViewApi::list_mvs_by_source_table_id`] returns every edge,
//!   including invalid ones, for management and lifecycle operations.
//! - [`MaterializedViewApi::get_mv_definition_snapshot`] reads one definition,
//!   its exact dependency edge, and the current source generation in one
//!   read-only transaction so query can decide whether the definition is valid.
//! - [`MaterializedViewApi::get_mv_source_binding_snapshot`] returns all active
//!   MVs for source-driven query rewrite when the source generation stays stable
//!   throughout metadata collection.
//!
//! `CreateMaterializedViewMeta::expected_source_generation` provides the same
//! fence for CREATE. `create_table` compares it with the current value and uses
//! the version record's KV sequence only as an internal CAS token. A missing
//! record means generation 0 and is initialized atomically with the first MV.
//! The record remains until source GC to prevent generation ABA.
//!
//! Invalidating source-schema updates advance the source generation atomically
//! with the source `TableMeta` update.
//!
//! The metadata operations are:
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
//! RENAME/DROP/MODIFY COLUMN ON SOURCE txn:
//!     read MVSourceBindingVersion (missing means generation 0)
//!     put current_source_generation + 1
//!     update the source TableMeta using its exact seq as CAS
//!     keep SourceTableMVIdent(tenant, source_id, *)
//!     the source TableMeta CAS serializes generation increments
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

use std::collections::BTreeSet;
use std::collections::HashMap;

use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::ListedMVSource;
use databend_common_meta_app::schema::ListedMaterializedView;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::MVDefinitionIdent;
use databend_common_meta_app::schema::MVDefinitionSnapshot;
use databend_common_meta_app::schema::MVInfo;
use databend_common_meta_app::schema::MVSourceBindingSnapshot;
use databend_common_meta_app::schema::MVSourceBindingVersionIdent;
use databend_common_meta_app::schema::MaterializedViewListFilter;
use databend_common_meta_app::schema::SourceTableMV;
use databend_common_meta_app::schema::SourceTableMVIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::DirName;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnGetResponse;
use databend_meta_client::types::TxnOpResponse;
use databend_meta_client::types::TxnRequest;
use databend_meta_client::types::protobuf as pb;
use log::warn;

use crate::DEFAULT_MGET_SIZE;
use crate::deserialize_struct_get_response;
use crate::error_util::invalid_reply;
use crate::kv_pb_api::KVPbApi;
use crate::send_txn;
use crate::txn_get;

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

    /// Get one MV definition and both source generations at one transaction point.
    ///
    /// This method returns raw metadata and does not decide whether the definition is valid.
    /// Query-facing catalog implementations compare the immutable bound generation with the
    /// current source generation. A missing current generation is always invalid.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_mv_definition_snapshot(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
        mv_table_id: u64,
    ) -> Result<MVDefinitionSnapshot, MetaError> {
        let definition_ident = MVDefinitionIdent::new(tenant, mv_table_id);
        let binding_ident = SourceTableMVIdent::new_generic(
            tenant,
            SourceTableMV::new(source_table_id, mv_table_id),
        );
        let generation_ident = MVSourceBindingVersionIdent::new(tenant, source_table_id);

        let txn = TxnRequest::new(vec![], vec![
            txn_get(&definition_ident),
            txn_get(&binding_ident),
            txn_get(&generation_ident),
        ]);
        let (success, responses) = send_txn(self, txn).await?;
        if !success {
            return Err(invalid_reply(
                "unconditional read-only transaction returned an unsuccessful result",
            )
            .into());
        }

        let [definition_response, binding_response, generation_response]: [TxnOpResponse; 3] =
            responses.try_into().map_err(|responses: Vec<_>| {
                invalid_reply(format!(
                    "materialized-view definition snapshot expected 3 responses, got {}",
                    responses.len()
                ))
            })?;
        let (Some(definition_response), Some(binding_response), Some(generation_response)) = (
            definition_response.into_get(),
            binding_response.into_get(),
            generation_response.into_get(),
        ) else {
            return Err(invalid_reply(
                "materialized-view definition snapshot contained a non-get response",
            )
            .into());
        };
        if definition_response.key != definition_ident.to_string_key()
            || binding_response.key != binding_ident.to_string_key()
            || generation_response.key != generation_ident.to_string_key()
        {
            return Err(invalid_reply(
                "materialized-view definition snapshot response key mismatch",
            )
            .into());
        }

        let (_, definition) =
            deserialize_struct_get_response::<MVDefinitionIdent>(definition_response)?;
        let (_, binding) = deserialize_struct_get_response::<SourceTableMVIdent>(binding_response)?;
        let (_, generation) =
            deserialize_struct_get_response::<MVSourceBindingVersionIdent>(generation_response)?;

        Ok(MVDefinitionSnapshot {
            definition,
            bound_source_generation: binding.map(|binding| binding.bound_source_generation),
            current_source_generation: generation
                .map(|generation| generation.current_source_generation),
        })
    }

    /// Get the current semantic MV-binding generation of one source table.
    ///
    /// Return `None` if the generation record has not been created.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_mv_current_source_generation(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<Option<u64>, MetaError> {
        let generation_ident = MVSourceBindingVersionIdent::new(tenant, source_table_id);
        Ok(self
            .get_pb(&generation_ident)
            .await?
            .map(|record| record.data.current_source_generation))
    }

    /// Get the immutable source generation stored on one exact MV dependency edge.
    ///
    /// This is a single point read. It does not read the source's current
    /// generation or decide whether the binding is active; callers compare the
    /// returned value with the current generation for their operation.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_mv_bound_source_generation(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
        mv_table_id: u64,
    ) -> Result<Option<u64>, MetaError> {
        let ident = SourceTableMVIdent::new_generic(
            tenant,
            SourceTableMV::new(source_table_id, mv_table_id),
        );
        Ok(self
            .get_pb(&ident)
            .await?
            .map(|binding| binding.data.bound_source_generation))
    }

    /// List MVs valid at the source generation observed by this call.
    ///
    /// This source-wide API is intended for discovering query-rewrite
    /// candidates. A generation change while collecting the bindings and MV
    /// metadata produces an empty candidate list at the final generation.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_mv_source_binding_snapshot(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<MVSourceBindingSnapshot, MetaError> {
        let Some(generation_before) = self
            .get_mv_current_source_generation(tenant, source_table_id)
            .await?
        else {
            return Ok(MVSourceBindingSnapshot {
                generation: 0,
                materialized_views: vec![],
            });
        };
        let mvs = list_mvs_by_source_table_id_impl(
            self,
            tenant,
            source_table_id,
            Some(generation_before),
        )
        .await?;
        let generation_after = self
            .get_mv_current_source_generation(tenant, source_table_id)
            .await?;
        match generation_after {
            Some(generation_after) if generation_after == generation_before => {
                Ok(MVSourceBindingSnapshot {
                    generation: generation_after,
                    materialized_views: mvs,
                })
            }
            _ => Ok(MVSourceBindingSnapshot {
                generation: 0,
                materialized_views: vec![],
            }),
        }
    }

    /// List every MV that depends on a source table, including invalid MVs.
    ///
    /// This unfiltered view is intended for management, SHOW, and GC.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_mvs_by_source_table_id(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<Vec<MVInfo>, MetaError> {
        list_mvs_by_source_table_id_impl(self, tenant, source_table_id, None).await
    }

    /// List materialized views for management and system-table consumers.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_materialized_views(
        &self,
        tenant: &Tenant,
        filter: &MaterializedViewListFilter,
    ) -> Result<Vec<ListedMaterializedView>, MetaError> {
        list_materialized_views_impl(self, tenant, filter).await
    }
}

struct ListedMVTarget {
    mv: MVInfo,
    database_id: u64,
    database_name: String,
    name: String,
    source_table_id: u64,
}

type RawMVTarget = (
    u64,
    Option<SeqV<MVDefinition>>,
    Option<SeqV<TableMeta>>,
    Option<SeqV<DBIdTableName>>,
);

async fn mget_responses<KV>(
    kv_api: &KV,
    keys: &[String],
) -> Result<Vec<TxnGetResponse>, MetaError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    if keys.is_empty() {
        return Ok(vec![]);
    }

    let mut values = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(DEFAULT_MGET_SIZE) {
        values.extend(kv_api.mget_kv(chunk).await?);
    }
    if values.len() != keys.len() {
        return Err(invalid_reply(format!(
            "materialized-view MGET expected {} responses, got {}",
            keys.len(),
            values.len()
        ))
        .into());
    }

    Ok(keys
        .iter()
        .zip(values)
        .map(|(key, value)| TxnGetResponse::new(key, value.map(pb::SeqV::from)))
        .collect())
}

async fn list_database_mappings<KV>(
    kv_api: &KV,
    tenant: &Tenant,
) -> Result<HashMap<u64, String>, MetaError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    let prefix = DirName::new(DatabaseNameIdent::new(tenant, "dummy"));
    Ok(kv_api
        .list_pb_vec(ListOptions::unlimited(&prefix))
        .await?
        .into_iter()
        .map(|(ident, database_id)| (database_id.data.db_id, ident.database_name().to_string()))
        .collect())
}

fn decode_database_responses(
    responses: Vec<TxnGetResponse>,
) -> Result<HashMap<u64, String>, MetaError> {
    let mut mappings = HashMap::with_capacity(responses.len());
    for response in responses {
        let (ident, database_id) = deserialize_struct_get_response::<DatabaseNameIdent>(response)?;
        if let Some(database_id) = database_id {
            mappings.insert(database_id.data.db_id, ident.database_name().to_string());
        }
    }
    Ok(mappings)
}

fn build_target(
    mv_id: u64,
    definition: Option<SeqV<MVDefinition>>,
    table_meta: Option<SeqV<TableMeta>>,
    table_name: Option<SeqV<DBIdTableName>>,
    database_mappings: &HashMap<u64, String>,
    filter: &MaterializedViewListFilter,
) -> Option<ListedMVTarget> {
    let definition = definition?;
    let (Some(table_meta), Some(table_name)) = (table_meta, table_name) else {
        warn!("materialized view {} has incomplete target metadata", mv_id);
        return None;
    };
    if table_meta.data.drop_on.is_some() || !is_materialized_view_engine(&table_meta.data.engine) {
        return None;
    }

    let Some(database_name) = database_mappings.get(&table_name.data.db_id) else {
        if filter.database_names.is_none() {
            warn!(
                "materialized view {} references inactive database {}",
                mv_id, table_name.data.db_id
            );
        }
        return None;
    };
    let source_table_id = match table_meta.data.materialized_view_source_table_id() {
        Ok(source_table_id) => source_table_id,
        Err(error) => {
            warn!(
                "materialized view {} has invalid source metadata: {}",
                mv_id, error
            );
            return None;
        }
    };

    if filter
        .names
        .as_ref()
        .is_some_and(|names| !names.contains(&table_name.data.table_name))
        || filter
            .source_table_ids
            .as_ref()
            .is_some_and(|ids| !ids.contains(&source_table_id))
    {
        return None;
    }

    Some(ListedMVTarget {
        mv: MVInfo {
            mv_id,
            definition,
            table_meta,
        },
        database_id: table_name.data.db_id,
        database_name: database_name.clone(),
        name: table_name.data.table_name,
        source_table_id,
    })
}

async fn list_materialized_views_impl<KV>(
    kv_api: &KV,
    tenant: &Tenant,
    filter: &MaterializedViewListFilter,
) -> Result<Vec<ListedMaterializedView>, MetaError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    let exact_mv_ids = filter.materialized_view_ids.as_ref();
    if exact_mv_ids.is_some_and(BTreeSet::is_empty) {
        return Ok(vec![]);
    }

    let exact_database_names = filter.database_names.as_ref();
    if exact_database_names.is_some_and(BTreeSet::is_empty) {
        return Ok(vec![]);
    }

    let (raw_targets, database_mappings): (Vec<RawMVTarget>, HashMap<u64, String>) =
        if let Some(mv_ids) = exact_mv_ids {
            let mv_ids = mv_ids.iter().copied().collect::<Vec<_>>();
            let mut keys = Vec::with_capacity(
                mv_ids.len() * 3 + exact_database_names.map_or(0, BTreeSet::len),
            );
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
            keys.extend(
                mv_ids
                    .iter()
                    .map(|mv_id| TableIdToName { table_id: *mv_id }.to_string_key()),
            );
            if let Some(database_names) = exact_database_names {
                keys.extend(
                    database_names
                        .iter()
                        .map(|name| DatabaseNameIdent::new(tenant, name).to_string_key()),
                );
            }

            let (mut responses, listed_database_mappings) = if exact_database_names.is_some() {
                (mget_responses(kv_api, &keys).await?, None)
            } else {
                let (responses, mappings) = futures::try_join!(
                    mget_responses(kv_api, &keys),
                    list_database_mappings(kv_api, tenant)
                )?;
                (responses, Some(mappings))
            };

            let database_responses = responses.split_off(mv_ids.len() * 3);
            let table_name_responses = responses.split_off(mv_ids.len() * 2);
            let table_meta_responses = responses.split_off(mv_ids.len());
            let definition_responses = responses;
            let database_mappings = match listed_database_mappings {
                Some(mappings) => mappings,
                None => decode_database_responses(database_responses)?,
            };

            let mut targets = Vec::with_capacity(mv_ids.len());
            for (mv_id, ((definition, table_meta), table_name)) in mv_ids.into_iter().zip(
                definition_responses
                    .into_iter()
                    .zip(table_meta_responses)
                    .zip(table_name_responses),
            ) {
                let (_, definition) =
                    deserialize_struct_get_response::<MVDefinitionIdent>(definition)?;
                let (_, table_meta) = deserialize_struct_get_response::<TableId>(table_meta)?;
                let (_, table_name) = deserialize_struct_get_response::<TableIdToName>(table_name)?;
                targets.push((mv_id, definition, table_meta, table_name));
            }
            (targets, database_mappings)
        } else {
            let prefix = DirName::new(MVDefinitionIdent::new(tenant, 0));
            let definitions = kv_api.list_pb_vec(ListOptions::unlimited(&prefix)).await?;
            if definitions.is_empty() {
                return Ok(vec![]);
            }

            let mv_ids = definitions
                .iter()
                .map(|(ident, _)| *ident.name())
                .collect::<Vec<_>>();
            let mut keys = Vec::with_capacity(
                mv_ids.len() * 2 + exact_database_names.map_or(0, BTreeSet::len),
            );
            keys.extend(
                mv_ids
                    .iter()
                    .map(|mv_id| TableId::new(*mv_id).to_string_key()),
            );
            keys.extend(
                mv_ids
                    .iter()
                    .map(|mv_id| TableIdToName { table_id: *mv_id }.to_string_key()),
            );
            if let Some(database_names) = exact_database_names {
                keys.extend(
                    database_names
                        .iter()
                        .map(|name| DatabaseNameIdent::new(tenant, name).to_string_key()),
                );
            }

            let (mut responses, listed_database_mappings) = if exact_database_names.is_some() {
                (mget_responses(kv_api, &keys).await?, None)
            } else {
                let (responses, mappings) = futures::try_join!(
                    mget_responses(kv_api, &keys),
                    list_database_mappings(kv_api, tenant)
                )?;
                (responses, Some(mappings))
            };

            let database_responses = responses.split_off(mv_ids.len() * 2);
            let table_name_responses = responses.split_off(mv_ids.len());
            let table_meta_responses = responses;
            let database_mappings = match listed_database_mappings {
                Some(mappings) => mappings,
                None => decode_database_responses(database_responses)?,
            };

            let mut targets = Vec::with_capacity(mv_ids.len());
            for (((ident, definition), table_meta), table_name) in definitions
                .into_iter()
                .zip(table_meta_responses)
                .zip(table_name_responses)
            {
                let (_, table_meta) = deserialize_struct_get_response::<TableId>(table_meta)?;
                let (_, table_name) = deserialize_struct_get_response::<TableIdToName>(table_name)?;
                targets.push((*ident.name(), Some(definition), table_meta, table_name));
            }
            (targets, database_mappings)
        };

    let targets = raw_targets
        .into_iter()
        .filter_map(|(mv_id, definition, table_meta, table_name)| {
            build_target(
                mv_id,
                definition,
                table_meta,
                table_name,
                &database_mappings,
                filter,
            )
        })
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Ok(vec![]);
    }

    let source_ids = targets
        .iter()
        .map(|target| target.source_table_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut source_keys = Vec::with_capacity(targets.len() + source_ids.len() * 2);
    source_keys.extend(targets.iter().map(|target| {
        SourceTableMVIdent::new_generic(
            tenant,
            SourceTableMV::new(target.source_table_id, target.mv.mv_id),
        )
        .to_string_key()
    }));
    source_keys.extend(
        source_ids
            .iter()
            .map(|source_id| MVSourceBindingVersionIdent::new(tenant, *source_id).to_string_key()),
    );
    source_keys.extend(source_ids.iter().map(|source_id| {
        TableIdToName {
            table_id: *source_id,
        }
        .to_string_key()
    }));

    let mut responses = mget_responses(kv_api, &source_keys).await?;
    let source_name_responses = responses.split_off(targets.len() + source_ids.len());
    let generation_responses = responses.split_off(targets.len());
    let binding_responses = responses;

    let mut source_facts = HashMap::with_capacity(source_ids.len());
    for (source_id, (generation, table_name)) in source_ids
        .into_iter()
        .zip(generation_responses.into_iter().zip(source_name_responses))
    {
        let (_, generation) =
            deserialize_struct_get_response::<MVSourceBindingVersionIdent>(generation)?;
        let (_, table_name) = deserialize_struct_get_response::<TableIdToName>(table_name)?;
        source_facts.insert(source_id, (generation, table_name));
    }

    let mut listed = Vec::with_capacity(targets.len());
    for (target, binding) in targets.into_iter().zip(binding_responses) {
        let (_, binding) = deserialize_struct_get_response::<SourceTableMVIdent>(binding)?;
        let (generation, table_name) = source_facts
            .get(&target.source_table_id)
            .cloned()
            .unwrap_or((None, None));
        listed.push(ListedMaterializedView {
            mv: target.mv,
            database_id: target.database_id,
            database_name: target.database_name,
            name: target.name,
            source: ListedMVSource {
                table_id: target.source_table_id,
                table_name: table_name.map(|name| name.data.table_name),
                bound_source_generation: binding
                    .map(|binding| binding.data.bound_source_generation),
                current_source_generation: generation
                    .map(|generation| generation.data.current_source_generation),
            },
        });
    }

    Ok(listed)
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

    let mut responses = mget_responses(kv_api, &keys).await?;
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
