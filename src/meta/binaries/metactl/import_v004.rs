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

use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_common_meta_raft_store::key_spaces::SMEntry;
use databend_common_meta_raft_store::ondisk::OnDisk;
use databend_common_meta_raft_store::raft_log_v004;
use databend_common_meta_raft_store::raft_log_v004::RaftLogV004;
use databend_common_meta_raft_store::sm_v003::adapter::SnapshotUpgradeV002ToV004;
use databend_common_meta_raft_store::sm_v003::SnapshotStoreV004;
use databend_common_meta_raft_store::sm_v003::WriteEntry;
use databend_common_meta_raft_store::state_machine::MetaSnapshotId;
use databend_common_meta_types::raft_types::LogId;
use databend_common_meta_types::sys_data::SysData;

/// Import serialized lines for `DataVersion::V004`
///
/// While importing, the max log id is also returned.
///
/// It writes logs and related entries to WAL based raft log, and state_machine entries to a snapshot.
pub async fn import_v004(
    raft_config: RaftConfig,
    lines: impl IntoIterator<Item = Result<String, io::Error>>,
) -> anyhow::Result<Option<LogId>> {
    OnDisk::ensure_dirs(&raft_config.raft_dir)?;

    let mut n = 0;

    let mut raft_log_importer = {
        let raft_log_config = raft_config.clone().to_raft_log_config();
        let raft_log_config = Arc::new(raft_log_config);
        let raft_log = RaftLogV004::open(raft_log_config)?;

        raft_log_v004::Importer::new(raft_log)
    };

    let snapshot_store = SnapshotStoreV004::new(raft_config);
    let writer = snapshot_store.new_writer()?;
    let (tx, join_handle) = writer.spawn_writer_thread("import_v004");

    let sys_data = Arc::new(Mutex::new(SysData::default()));

    let mut converter = SnapshotUpgradeV002ToV004 {
        sys_data: sys_data.clone(),
    };

    for line in lines {
        let l = line?;
        let (tree_name, kv_entry): (String, RaftStoreEntry) = serde_json::from_str(&l)?;

        if tree_name.starts_with("state_machine/") {
            // Write to snapshot
            let sm_entry: SMEntry = kv_entry.try_into().map_err(|err_str| {
                anyhow::anyhow!("Failed to convert RaftStoreEntry to SMEntry: {}", err_str)
            })?;

            let kv = converter.sm_entry_to_rotbl_kv(sm_entry)?;
            if let Some(kv) = kv {
                tx.send(WriteEntry::Data(kv)).await?;
            }
        } else {
            if let RaftStoreEntry::DataHeader { .. } = kv_entry {
                // Data header is not stored in V004 RaftLog
                continue;
            }
            raft_log_importer.import_raft_store_entry(kv_entry)?;
        }

        n += 1;
    }

    let max_log_id = raft_log_importer.max_log_id;
    raft_log_importer.flush().await?;

    let s = {
        let r = sys_data.lock().unwrap();
        r.clone()
    };

    tx.send(WriteEntry::Finish(s)).await?;
    let temp_snapshot_data = join_handle.await??;

    let last_applied = {
        let r = sys_data.lock().unwrap();
        *r.last_applied_ref()
    };
    let snapshot_id = MetaSnapshotId::new_with_epoch(last_applied);
    let db = temp_snapshot_data.move_to_final_path(snapshot_id.to_string())?;

    eprintln!(
        "Imported {} records, snapshot: {}; snapshot_path: {}; snapshot_stat: {}",
        n,
        snapshot_id,
        db.path(),
        db.stat()
    );
    Ok(max_log_id)
}
