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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_NAME;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_SOURCE_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_NAME;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_VER;

use crate::sessions::QueryContext;
use crate::sessions::TableContextLicense;
use crate::sessions::TableContextStream;

/// Build stream consumption updates. `query = true` selects only stream references
/// explicitly marked WITH CONSUME in a SELECT; `query = false` selects all stream
/// references for implicit consumption by DML.
pub async fn build_update_stream_reqs(
    ctx: &Arc<QueryContext>,
    query: bool,
) -> Result<Vec<UpdateStreamMetaReq>> {
    let tables = ctx.get_consume_streams(query)?;
    if tables.is_empty() {
        return Ok(vec![]);
    }

    LicenseManagerSwitch::instance()
        .check_enterprise_enabled(ctx.get_license_key(), Feature::Stream)?;

    let mut reqs = Vec::with_capacity(tables.len());
    for table in tables.into_iter() {
        let stream = StreamTable::try_from_table(table.as_ref())?;
        let stream_info = stream.get_table_info();

        let source_table = stream.source_table(ctx.clone()).await?;
        let inner_fuse = FuseTable::try_from_table(source_table.as_ref())?;

        let table_version = inner_fuse.get_table_info().ident.seq;
        let mut options = stream.options().clone();
        options.insert(OPT_KEY_TABLE_VER.to_string(), table_version.to_string());
        if let Some(snapshot_loc) = inner_fuse.snapshot_loc() {
            options.insert(OPT_KEY_SNAPSHOT_LOCATION.to_string(), snapshot_loc);
        }

        // To be compatible with older versions, set source database id.
        if !options.contains_key(OPT_KEY_SOURCE_DATABASE_ID) {
            let source_db_id = inner_fuse
                .get_table_info()
                .options()
                .get(OPT_KEY_DATABASE_ID)
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "Invalid fuse table, table option {} not found when building update stream req",
                        OPT_KEY_DATABASE_ID
                    ))
                })?;
            options.insert(
                OPT_KEY_SOURCE_DATABASE_ID.to_owned(),
                source_db_id.to_string(),
            );
            options.remove(OPT_KEY_DATABASE_NAME);
            options.remove(OPT_KEY_TABLE_NAME);
        }

        reqs.push(UpdateStreamMetaReq {
            stream_id: stream_info.ident.table_id,
            seq: MatchSeq::Exact(stream_info.ident.seq),
            options,
        });
    }
    Ok(reqs)
}
