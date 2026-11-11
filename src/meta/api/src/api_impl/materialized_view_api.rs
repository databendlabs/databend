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
//!        options["materialized_view_source_database_name"] = <bound db name>
//!        options["materialized_view_source_table_name"] = <bound table name>
//!        options["materialized_view_source_columns"] = <source columns as JSON>
//!        options["materialized_view_invalid_reason"] = <reason>, while invalid
//! __fd_materialized_view_definition/<tenant>/<mv_id>
//!     -> MVDefinition { query, logical_schema }
//! __fd_materialized_view_by_source/<tenant>/<source_id>/<mv_id>
//!     -> MVSourceBinding (no state)
//! ```
//!
//! Whether an MV is valid is recorded in its `TableMeta` options, on a best-effort
//! basis. Source DDL does not touch MV metadata in its own transaction: a source
//! may have many MVs, and the DDL must not grow with them or retry on their
//! REFRESH commits. After a SWAP of the source, or a change of a column the MV
//! reads, the interpreter that ran it sets the invalid reason of each dependent MV
//! through the ordinary table option API; a failure is only logged. Both reasons are
//! final. A drop, undrop, or rename of the source or of its database, and CREATE OR
//! REPLACE TABLE, which drops the table it replaces, write no reason: readers find
//! a dropped source from its `drop_on`, and a moved one from binding the definition
//! by name, and accept the MV again once the source is back.
//!
//! Because the reason may be missing, readers do not rely on it alone: they bind
//! the definition by the source name and check the source table id, and compare
//! the source schema with the columns the MV reads (`materialized_view_source_columns`).
//! An MV created by an older version has no recorded columns; the query layer
//! records all the source columns, from the schema before the DDL, at the first
//! column change of its source.

use std::collections::BTreeSet;
use std::collections::HashMap;

use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseIdToName;
use databend_common_meta_app::schema::ListedMVSource;
use databend_common_meta_app::schema::ListedMaterializedView;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::MVDefinitionIdent;
use databend_common_meta_app::schema::MVInfo;
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
use databend_meta_client::types::protobuf as pb;
use log::warn;

use crate::DEFAULT_MGET_SIZE;
use crate::deserialize_struct_get_response;
use crate::error_util::invalid_reply;
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

    /// List the valid MVs of a source table, for query rewrite.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_valid_mvs_by_source_table_id(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<Vec<MVInfo>, MetaError> {
        let mut materialized_views =
            list_mvs_by_source_table_id_impl(self, tenant, source_table_id).await?;
        materialized_views.retain(|mv| mv.table_meta.data.is_valid_materialized_view());
        Ok(materialized_views)
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
        list_mvs_by_source_table_id_impl(self, tenant, source_table_id).await
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

async fn mv_ids_of_source<KV>(
    kv_api: &KV,
    tenant: &Tenant,
    source_table_id: u64,
) -> Result<Vec<u64>, MetaError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    let source_mv_prefix = DirName::new(SourceTableMVIdent::new_generic(
        tenant,
        SourceTableMV::new(source_table_id, 0),
    ));
    let mv_ids = kv_api
        .list_pb_vec(ListOptions::unlimited(&source_mv_prefix))
        .await?
        .into_iter()
        .map(|(ident, _)| ident.name().mv_table_id)
        .collect();
    Ok(mv_ids)
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
    let mut source_keys = Vec::with_capacity(source_ids.len() * 2);
    source_keys.extend(source_ids.iter().map(|source_id| {
        TableIdToName {
            table_id: *source_id,
        }
        .to_string_key()
    }));
    source_keys.extend(
        source_ids
            .iter()
            .map(|source_id| TableId::new(*source_id).to_string_key()),
    );

    let mut responses = mget_responses(kv_api, &source_keys).await?;
    let source_meta_responses = responses.split_off(source_ids.len());
    let source_name_responses = responses;

    let mut source_facts = HashMap::with_capacity(source_ids.len());
    for (source_id, (table_name, table_meta)) in source_ids
        .into_iter()
        .zip(source_name_responses.into_iter().zip(source_meta_responses))
    {
        let (_, table_name) = deserialize_struct_get_response::<TableIdToName>(table_name)?;
        let (_, table_meta) = deserialize_struct_get_response::<TableId>(table_meta)?;
        let is_active = table_meta.is_some_and(|meta| meta.data.drop_on.is_none());
        source_facts.insert(source_id, (table_name, is_active));
    }

    let source_database_ids = source_facts
        .values()
        .filter_map(|(table_name, _)| table_name.as_ref().map(|name| name.data.db_id))
        .collect::<BTreeSet<_>>();
    let source_database_keys = source_database_ids
        .iter()
        .map(|database_id| DatabaseIdToName::new(*database_id).to_string_key())
        .chain(
            source_database_ids
                .iter()
                .map(|database_id| DatabaseId::new(*database_id).to_string_key()),
        )
        .collect::<Vec<_>>();
    let mut source_database_name_responses = mget_responses(kv_api, &source_database_keys).await?;
    let source_database_meta_responses =
        source_database_name_responses.split_off(source_database_ids.len());
    let mut source_database_names = HashMap::with_capacity(source_database_ids.len());
    // DROP DATABASE marks only DatabaseMeta and keeps both the id-to-name mapping and the tables'
    // TableMeta until GC, so a source in a dropped database must be checked through its database.
    let mut active_source_database_ids = BTreeSet::new();
    for (database_id, (name_response, meta_response)) in source_database_ids.into_iter().zip(
        source_database_name_responses
            .into_iter()
            .zip(source_database_meta_responses),
    ) {
        let (_, database_name) =
            deserialize_struct_get_response::<DatabaseIdToName>(name_response)?;
        if let Some(database_name) = database_name {
            source_database_names
                .insert(database_id, database_name.data.database_name().to_string());
        }
        let (_, database_meta) = deserialize_struct_get_response::<DatabaseId>(meta_response)?;
        if database_meta.is_some_and(|meta| meta.data.drop_on.is_none()) {
            active_source_database_ids.insert(database_id);
        }
    }

    let mut listed = Vec::with_capacity(targets.len());
    for target in targets {
        let (table_name, is_active) = source_facts
            .get(&target.source_table_id)
            .cloned()
            .unwrap_or((None, false));
        let (database_id, database_name, table_name, is_active, database_dropped) = match table_name
        {
            Some(table_name) => {
                let database_id = table_name.data.db_id;
                let database_dropped = !active_source_database_ids.contains(&database_id);
                (
                    Some(database_id),
                    source_database_names.get(&database_id).cloned(),
                    Some(table_name.data.table_name),
                    is_active && !database_dropped,
                    database_dropped,
                )
            }
            None => (None, None, None, is_active, false),
        };
        listed.push(ListedMaterializedView {
            mv: target.mv,
            database_id: target.database_id,
            database_name: target.database_name,
            name: target.name,
            source: ListedMVSource {
                table_id: target.source_table_id,
                database_id,
                database_name,
                table_name,
                is_active,
                database_dropped,
            },
        });
    }

    Ok(listed)
}

async fn list_mvs_by_source_table_id_impl<KV>(
    kv_api: &KV,
    tenant: &Tenant,
    source_table_id: u64,
) -> Result<Vec<MVInfo>, MetaError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    let mv_ids = mv_ids_of_source(kv_api, tenant, source_table_id).await?;
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
