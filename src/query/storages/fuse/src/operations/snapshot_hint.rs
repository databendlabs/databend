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

use std::collections::BTreeMap;
use std::io::Read;
use std::io::Write;
use std::time::Instant;

use bytes::Buf;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableMeta;
use log::info;
use log::warn;
use opendal::ErrorKind;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;

use crate::FUSE_TBL_LAST_SNAPSHOT_HINT;
use crate::FUSE_TBL_LAST_SNAPSHOT_HINT_V2;
use crate::io::TableMetaLocationGenerator;

pub struct SnapshotHintWriter<'a> {
    ctx: &'a dyn TableContext,
    dal: &'a Operator,
}

impl<'a> SnapshotHintWriter<'a> {
    pub fn new(ctx: &'a dyn TableContext, dal: &'a Operator) -> Self {
        SnapshotHintWriter { ctx, dal }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SnapshotHint {
    pub snapshot_full_path: String,
    pub entity_comments: EntityComments,
    #[serde(default)]
    pub indexes: BTreeMap<String, TableIndex>,
}

impl SnapshotHint {
    fn marshall<W: Write>(&self, writer: W) -> Result<()> {
        serde_json::to_writer(writer, self)?;
        Ok(())
    }
    fn unmarshall<R: Read>(reader: R) -> Result<Self> {
        let v = serde_json::from_reader(reader)?;
        Ok(v)
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
    // First, try to load the v2 format hint file
    let hint_file_path = format!("{}/{}", storage_prefix, FUSE_TBL_LAST_SNAPSHOT_HINT_V2);
    match operator.read(&hint_file_path).await {
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                // if there is no V2 hint file, fallback to read the legacy hint file
                try_read_legacy_hint(storage_prefix, operator).await
            } else {
                Err(e.into())
            }
        }

        Ok(buf) => Ok(Some(SnapshotHint::unmarshall(buf.to_bytes().reader())?)),
    }
}

async fn try_read_legacy_hint(
    storage_prefix: &str,
    operator: &Operator,
) -> Result<Option<SnapshotHint>> {
    let hint_file_path = format!("{}/{}", storage_prefix, FUSE_TBL_LAST_SNAPSHOT_HINT);
    let begin_load_hint = Instant::now();
    let maybe_hint_content = operator.read(&hint_file_path).await;
    info!(
        "{} load last snapshot hint file [{}], time used {:?}",
        if maybe_hint_content.is_ok() {
            "successfully"
        } else {
            "failed to"
        },
        hint_file_path,
        begin_load_hint.elapsed()
    );
    match maybe_hint_content {
        Ok(buf) => {
            let hint_content = buf.to_vec();
            let snapshot_location = String::from_utf8(hint_content)?;

            Ok(Some(SnapshotHint {
                snapshot_full_path: snapshot_location,
                entity_comments: EntityComments {
                    table_comment: "".to_string(),
                    field_comments: vec![],
                },
                indexes: Default::default(),
            }))
        }
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

impl SnapshotHintWriter<'_> {
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
            snapshot_full_path: last_snapshot_path,
            entity_comments: EntityComments::from(table_meta),
            indexes: table_meta.indexes.clone(),
        };

        let mut bytes = vec![];
        if let Err(e) = hint.marshall(&mut bytes) {
            warn!("marshaling last snapshot hint failed. {}", e);
        } else {
            let _ = dal.write(&hint_path, bytes).await.inspect_err(|e| {
                warn!("write last snapshot hint failure. {}", e);
            });
        }
    }
}
