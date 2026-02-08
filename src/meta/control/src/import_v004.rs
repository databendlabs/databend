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

use databend_meta_raft_store::config::RaftConfig;
use databend_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_meta_raft_store::key_spaces::SMEntry;
use databend_meta_raft_store::ondisk::DataVersion;
use databend_meta_raft_store::ondisk::Header;
use databend_meta_raft_store::ondisk::OnDisk;
use databend_meta_raft_store::raft_log_v004;
use databend_meta_raft_store::raft_log_v004::RaftLogV004;
use databend_meta_raft_store::sm_v003::SnapshotStoreV004;
use databend_meta_raft_store::sm_v003::WriteEntry;
use databend_meta_raft_store::sm_v003::adapter::SMEntryV002ToV004;
use databend_meta_raft_store::state_machine::MetaSnapshotId;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_types::raft_types::LogId;
use databend_meta_types::sys_data::SysData;

/// Import serialized lines for `DataVersion::V004`
///
/// While importing, the max log id is also returned.
///
/// It writes logs and related entries to WAL based raft log, and state_machine entries to a snapshot.
pub async fn import_v004<SP: SpawnApi>(
    raft_config: RaftConfig,
    lines: impl IntoIterator<Item = Result<String, io::Error>>,
) -> anyhow::Result<Option<LogId>> {
    let data_version = DataVersion::V004;

    OnDisk::ensure_dirs(&raft_config.raft_dir)?;

    let mut n = 0;

    let mut raft_log_importer = {
        let raft_log_config = raft_config.clone().to_raft_log_config();
        let raft_log_config = Arc::new(raft_log_config);
        let raft_log = RaftLogV004::open(raft_log_config)?;

        raft_log_v004::Importer::new(raft_log)
    };

    let snapshot_store: SnapshotStoreV004<SP> = SnapshotStoreV004::new(raft_config.clone());
    let writer = snapshot_store.new_writer()?;
    let (tx, join_handle) = writer.spawn_writer_thread("import_v004");

    let sys_data_holder = Arc::new(Mutex::new(SysData::default()));

    let mut converter = SMEntryV002ToV004 {
        sys_data: sys_data_holder.clone(),
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
            let kv_entry = kv_entry.upgrade();
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

    let sys_data = {
        let r = sys_data_holder.lock().unwrap();
        r.clone()
    };
    let last_applied = *sys_data.last_applied_ref();
    let snapshot_id = MetaSnapshotId::new_with_epoch(last_applied);

    tx.send(WriteEntry::Finish((snapshot_id.clone(), sys_data)))
        .await?;
    let db = join_handle.await??;

    eprintln!(
        "{data_version}: Imported {} records, snapshot: {}; snapshot_path: {}; snapshot_stat: {}",
        n,
        snapshot_id,
        db.path(),
        db.stat()
    );

    let on_disk = OnDisk::new(
        Header {
            version: DataVersion::V004,
            upgrading: None,
            cleaning: false,
        },
        &raft_config,
    );

    on_disk.write_header(&on_disk.header)?;
    eprintln!("{data_version}: written on-disk header: {}", on_disk.header);

    Ok(max_log_id)
}
