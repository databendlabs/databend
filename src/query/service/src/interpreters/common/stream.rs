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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::UpdateStreamMetaReq;
use common_meta_types::MatchSeq;
use common_sql::MetadataRef;
use common_storages_factory::Table;
use common_storages_fuse::FuseTable;
use common_storages_stream::stream_table::StreamTable;
use common_storages_stream::stream_table::OPT_KEY_TABLE_VER;
use common_storages_stream::stream_table::STREAM_ENGINE;
use storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

use crate::sessions::QueryContext;

pub async fn build_update_stream_meta_seq(
    ctx: Arc<QueryContext>,
    metadata: &MetadataRef,
) -> Result<Option<UpdateStreamMetaReq>> {
    let table = get_stream_table(metadata)?;
    if let Some(table) = table {
        let stream = StreamTable::try_from_table(table.as_ref())?;
        let stream_info = stream.get_table_info();
        let source_table = stream.source_table(ctx).await?;
        let inner_fuse = FuseTable::try_from_table(source_table.as_ref())?;

        let table_version = inner_fuse.get_table_info().ident.seq;
        let mut options = stream.options().clone();
        options.insert(OPT_KEY_TABLE_VER.to_string(), table_version.to_string());
        if let Some(snapshot_loc) = inner_fuse.snapshot_loc().await? {
            options.insert(OPT_KEY_SNAPSHOT_LOCATION.to_string(), snapshot_loc);
        }

        Ok(Some(UpdateStreamMetaReq {
            stream_id: stream_info.ident.table_id,
            seq: MatchSeq::Exact(stream_info.ident.seq),
            options,
        }))
    } else {
        Ok(None)
    }
}

fn get_stream_table(metadata: &MetadataRef) -> Result<Option<Arc<dyn Table>>> {
    let r_lock = metadata.read();
    let tables = r_lock.tables();
    let mut streams = vec![];
    for t in tables {
        if t.table().engine() == STREAM_ENGINE {
            streams.push(t);
        }
    }

    match streams.len() {
        0 => Ok(None),
        1 => Ok(Some(streams[0].table())),
        _ => Err(ErrorCode::Unimplemented(
            "Only support single stream queries",
        )),
    }
}
