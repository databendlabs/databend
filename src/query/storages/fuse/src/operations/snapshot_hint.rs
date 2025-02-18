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

use std::io::Read;
use std::io::Write;
use std::time::Instant;

use bytes::Buf;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableMeta;
use log::info;
use log::warn;
use opendal::ErrorKind;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;

use crate::io::TableMetaLocationGenerator;
use crate::FUSE_TBL_LAST_SNAPSHOT_HINT;
use crate::FUSE_TBL_LAST_SNAPSHOT_HINT_V2;

pub struct SnapshotHintWriter<'a> {
    ctx: &'a dyn TableContext,
    dal: &'a Operator,
}

pub struct SnapshotHint {
    pub snapshot_location: String,
    pub entity_comment: EntityComments,
}

impl SnapshotHint {
    fn marshall<W: Write>(&self, writer: W) -> Result<()> {
        todo!()
    }
    fn unmarshall<R: Read>(reader: R) -> Result<Self> {
        todo!()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EntityComments {
    pub table_comment: String,
    pub field_comments: Vec<String>,
}

impl<'a> From<&'a TableMeta> for EntityComments {
    fn from(meta: &'a TableMeta) -> Self {
        EntityComments {
            table_comment: meta.comment.clone(),
            field_comments: meta.field_comments.clone(),
        }
    }
}

pub async fn load_last_snapshot_hint(
    storage_prefix: &str,
    operator: &Operator,
) -> Result<Option<SnapshotHint>> {
    let hint_file_path = format!("{}/{}", storage_prefix, FUSE_TBL_LAST_SNAPSHOT_HINT_V2);
    match operator.read(&hint_file_path).await {
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                try_read_legacy_hint(storage_prefix, operator).await
            } else {
                return Err(e.into());
            }
        }

        Ok(bytes) => Ok(Some(SnapshotHint::unmarshall(bytes.to_bytes().reader())?)),
    }
}

async fn try_read_legacy_hint(
    storage_prefix: &str,
    operator: &Operator,
) -> Result<Option<SnapshotHint>> {
    todo!()
}

impl<'a> SnapshotHintWriter<'a> {
    #[async_backtrace::framed]
    pub async fn write_last_snapshot_hint(
        &self,
        location_generator: &TableMetaLocationGenerator,
        last_snapshot_path: &str,
        table_meta: &TableMeta,
    ) {
        let ctx = &self.ctx;
        if let Ok(false) = ctx.get_settings().get_enable_last_snapshot_location_hint() {
            info!(
                "Write last_snapshot_location_hint disabled. Snapshot {}",
                last_snapshot_path
            );
            return;
        }

        let dal = &self.dal;

        // Just try our best to write down the hint file of last snapshot
        // - will retry in the case of temporary failure
        // but
        // - errors are ignored if writing is eventually failed
        // - errors (if any) will not be propagated to caller
        // - "data race" ignored
        //   if multiple different versions of hints are written concurrently
        //   it is NOT guaranteed that the latest version will be kept

        let hint_path = location_generator.gen_last_snapshot_hint_location();
        let last_snapshot_path = {
            let operator_meta_data = dal.info();
            let storage_prefix = operator_meta_data.root();
            format!("{}{}", storage_prefix, last_snapshot_path)
        };

        let hint = SnapshotHint {
            snapshot_location: last_snapshot_path,
            entity_comment: EntityComments::from(table_meta),
        };

        let mut bytes = vec![];
        hint.marshall(&mut bytes).expect("TODO ");

        dal.write(&hint_path, bytes).await.unwrap_or_else(|e| {
            warn!("write last snapshot hint failure. {}", e);
        });
    }
}
