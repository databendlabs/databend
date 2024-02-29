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

use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::get_license_manager;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::MetadataRef;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_VER;

use crate::sessions::QueryContext;

pub async fn build_update_stream_meta_seq(
    ctx: Arc<QueryContext>,
    metadata: &MetadataRef,
) -> Result<Vec<UpdateStreamMetaReq>> {
    let tables = get_stream_table(metadata)?;
    if tables.is_empty() {
        return Ok(vec![]);
    }

    let license_manager = get_license_manager();
    license_manager
        .manager
        .check_enterprise_enabled(ctx.get_license_key(), Feature::Stream)?;

    let mut reqs = Vec::with_capacity(tables.len());
    for table in tables.into_iter() {
        let stream = StreamTable::try_from_table(table.as_ref())?;
        let stream_info = stream.get_table_info();
        let source_table = stream.source_table(ctx.get_default_catalog()?).await?;
        let inner_fuse = FuseTable::try_from_table(source_table.as_ref())?;

        let table_version = inner_fuse.get_table_info().ident.seq;
        let mut options = stream.options().clone();
        options.insert(OPT_KEY_TABLE_VER.to_string(), table_version.to_string());
        if let Some(snapshot_loc) = inner_fuse.snapshot_loc().await? {
            options.insert(OPT_KEY_SNAPSHOT_LOCATION.to_string(), snapshot_loc);
        }

        reqs.push(UpdateStreamMetaReq {
            stream_id: stream_info.ident.table_id,
            seq: MatchSeq::Exact(stream_info.ident.seq),
            options,
        });
    }
    Ok(reqs)
}

fn get_stream_table(metadata: &MetadataRef) -> Result<Vec<Arc<dyn Table>>> {
    let r_lock = metadata.read();
    let tables = r_lock.tables();
    let mut streams = vec![];
    for t in tables {
        let table = t.table();
        if table.engine() == STREAM_ENGINE {
            streams.push(table);
        }
    }
    Ok(streams)
}
