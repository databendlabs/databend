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

//! Checks of a materialized view against its source, shared by reads and REFRESH.
//!
//! Only final reasons are stored: a SWAP of the source and a change of a column the MV reads.
//! Reads and REFRESH check the stored reason before binding the definition, which reads the
//! source by name, and the bound source table id after.

use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_meta_api::TableApi;
use databend_common_meta_app::schema::OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_users::UserApiProvider;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_table_meta::table::MATERIALIZED_VIEW_SOURCE_SCHEMA_CHANGED_REASON;
use log::info;
use log::warn;

use crate::BindContext;
use crate::MetadataRef;

/// Reject MV `mv_name`, with `mv_meta`, when it records an invalid reason.
pub fn check_materialized_view_invalid(mv_meta: &TableMeta, mv_name: &str) -> Result<()> {
    match mv_meta.materialized_view_invalid_reason() {
        // A stored reason is final.
        Some(reason) => Err(ErrorCode::InvalidMaterializedView(format!(
            "materialized view {mv_name} is invalid: {reason}; recreate the materialized view"
        ))),
        None => Ok(()),
    }
}

/// Best effort: record that a column MV `mv_id` reads changed. Only this option changes, on the
/// latest `TableMeta` of the MV, so a concurrent REFRESH commit is kept.
pub async fn record_materialized_view_schema_changed(mv_id: u64) {
    let reason = MATERIALIZED_VIEW_SOURCE_SCHEMA_CHANGED_REASON;
    info!(
        "record the invalid reason of materialized view {}: {}",
        mv_id, reason
    );
    if let Err(error) = UserApiProvider::instance()
        .get_meta_store_client()
        .upsert_table_option(UpsertTableOptionReq {
            table_id: mv_id,
            seq: MatchSeq::Any,
            options: HashMap::from([(
                OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON.to_string(),
                Some(reason.to_string()),
            )]),
        })
        .await
    {
        warn!(
            "failed to record the invalid reason of materialized view {}: {}",
            mv_id, error
        );
    }
}

/// Whether the physical definition, bound as `bind_context` with `metadata`, no longer produces
/// the stored schema `mv_schema` of the MV: a source column it reads changed type in a way the
/// definition still binds.
pub fn is_materialized_view_physical_schema_changed(
    bind_context: &BindContext,
    metadata: MetadataRef,
    mv_schema: &TableSchema,
) -> Result<bool> {
    let bound = bind_context.output_table_schema(metadata)?;
    // CREATE stores a bare Null column as nullable String.
    let stored_type = |data_type: &TableDataType| match data_type {
        TableDataType::Null => TableDataType::String.wrap_nullable(),
        data_type => data_type.clone(),
    };
    Ok(bound.fields().len() != mv_schema.fields().len()
        || bound
            .fields()
            .iter()
            .zip(mv_schema.fields())
            .any(|(bound, stored)| {
                bound.name() != stored.name()
                    || &stored_type(bound.data_type()) != stored.data_type()
            }))
}
