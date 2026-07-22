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

//! Materialized view metadata uses a source-table reverse index to coordinate
//! MV lifecycle changes with source-table writes.
//!
//! Key-value layout:
//! - `__fd_materialized_view_definition/<tenant>/<mv_table_id>` ->
//!   [`MVDefinition`]
//! - `__fd_materialized_view_by_source/<tenant>/<source_table_id>` ->
//!   [`SourceTableMVIds`](databend_common_meta_app::schema::SourceTableMVIds)
//! - `__fd_table_by_id/<mv_table_id>` -> `TableMeta`; an MV reuses ordinary
//!   table metadata and storage, and its table ID is also its MV ID.
//! - `SourceTableMVIds` is both the reverse index for a source table and the
//!   list of MVs that are currently valid for that source table. Its KV
//!   sequence is the consistency version used by source-table writes.
//!
//! MV table publication and drop algorithm:
//! 1. Build the MV as a hidden table and store its definition without adding
//!    the MV table ID to the source-table index.
//! 2. Before publishing the MV table, validate its staged definition against
//!    the current source-table definition and use the source table's `TableMeta`
//!    sequence as a commit condition.
//! 3. Publish the MV table and add its ID to the source-table index in one
//!    transaction. `CREATE OR REPLACE` also removes the previous MV table ID,
//!    including from a different source-table index.
//! 4. Dropping an MV table removes its definition and source-table-index
//!    membership in the same transaction that marks the MV table as dropped.
//!    An empty source-table index is retained so its sequence advances;
//!    source-table GC owns its final deletion.
//!
//! Source-table write algorithm:
//! 1. Read the source-table index and record its sequence.
//! 2. Fetch the definition and `TableMeta` for every listed MV table, then plan
//!    one multi-table operation that writes the source table and all of those MV
//!    tables.
//! 3. Commit the operation with the source-table-index sequence as a condition.
//! 4. A concurrent MV-table create, drop, or replacement changes the sequence,
//!    so the commit fails and the write is planned again with the new MV list.
//!
//! Source-table DDL invalidation and recovery:
//! 1. A source-table DDL that can invalidate an MV definition, such as renaming
//!    the source table or changing its columns, clears the source-table index in
//!    the same transaction as the source-table change while retaining the empty
//!    value.
//! 2. Future writes no longer update those MV tables, an in-progress write fails
//!    its sequence condition, and querying an MV absent from the source-table
//!    index reports it as invalid. An MV table staged against the old
//!    source-table definition also fails validation before publication.
//! 3. To recover, recreate the MV from the current source-table definition. The
//!    new MV becomes valid when its table ID is published in the source-table
//!    index.

use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::MVDefinitionIdent;
use databend_common_meta_app::schema::MVInfo;
use databend_common_meta_app::schema::SourceTableMVIdsIdent;
use databend_common_meta_app::schema::SourceTableMVs;
use databend_common_meta_app::schema::TableId;
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

    /// Get the MVs that depend on a source table and the source index sequence.
    ///
    /// Each [`MVInfo`] contains the definition and `TableMeta` needed for a
    /// multi-table write. The caller must use
    /// [`SourceTableMVs::source_table_mvs_index_seq`] as a transaction condition
    /// so that a concurrent MV create, drop, or replacement invalidates the
    /// write. The sequence is 0 when the source index does not exist.
    ///
    /// Definitions and table metadata are fetched in one `mget_kv` request.
    /// Incomplete MVs are omitted with a warning.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn mget_mvs_by_source_table_id(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<SourceTableMVs, MetaError> {
        let source_ident = SourceTableMVIdsIdent::new(tenant, source_table_id);
        let Some(source_mv_ids) = self.get_pb(&source_ident).await? else {
            return Ok(SourceTableMVs {
                source_table_mvs_index_seq: 0,
                mvs: vec![],
            });
        };
        let source_index_seq = source_mv_ids.seq;
        let mv_ids = source_mv_ids.data.mv_ids();
        if mv_ids.is_empty() {
            return Ok(SourceTableMVs {
                source_table_mvs_index_seq: source_index_seq,
                mvs: vec![],
            });
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
            mvs.push(MVInfo {
                mv_id: *mv_id,
                definition,
                table_meta,
            });
        }

        Ok(SourceTableMVs {
            source_table_mvs_index_seq: source_index_seq,
            mvs,
        })
    }
}

impl<KV> MaterializedViewApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
