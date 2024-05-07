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
use std::iter::repeat_with;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::LogId;
use databend_common_meta_types::SnapshotData;
use databend_common_meta_types::StoredMembership;
use itertools::Itertools;
use log::info;
use openraft::SnapshotId;
use rotbl::v001::SeqMarked;

use crate::key_spaces::SMEntry;
use crate::leveled_store::rotbl_codec::RotblCodec;
use crate::marked::Marked;
use crate::sm_v003::SnapshotStoreV003;
use crate::sm_v003::WriteEntry;
use crate::state_machine::StateMachineMetaKey;

/// Convert V002 snapshot lines in json of [`SMEntry`]
/// to V003 rotbl key-value pairs. `(String, SeqMarked)`,
/// or update SysData in place.
///
/// It holds a lock of SysData because this converter may be run concurrently in multiple threads.
pub struct SnapshotUpgradeV002ToV003 {
    pub sys_data: Arc<Mutex<SysData>>,
}

impl SnapshotUpgradeV002ToV003 {
    pub fn convert_line(&mut self, s: &str) -> Result<Option<(String, SeqMarked)>, io::Error> {
        let ent: SMEntry =
            serde_json::from_str(s).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let kv = self.sm_entry_to_rotbl_kv(ent)?;
        Ok(kv)
    }

    pub fn sm_entry_to_rotbl_kv(
        &mut self,
        ent: SMEntry,
    ) -> Result<Option<(String, SeqMarked)>, io::Error> {
        match ent {
            SMEntry::DataHeader { .. } => {
                // Snapshot V003 in rotbl format does not store data header
            }
            SMEntry::Nodes { key, value } => {
                let mut s = self.sys_data.lock().unwrap();
                s.nodes_mut().insert(key, value);
            }
            SMEntry::StateMachineMeta { key, value } => {
                let mut s = self.sys_data.lock().unwrap();
                match key {
                    StateMachineMetaKey::LastApplied => {
                        // Infallible
                        let last_applied: LogId = value.try_into().unwrap();
                        *s.last_applied_mut() = Some(last_applied);
                    }
                    StateMachineMetaKey::Initialized => {
                        // Obsolete
                    }
                    StateMachineMetaKey::LastMembership => {
                        // Infallible
                        let last_membership: StoredMembership = value.try_into().unwrap();
                        *s.last_membership_mut() = last_membership;
                    }
                }
            }
            SMEntry::Expire { key, mut value } => {
                // Old version ExpireValue has seq to be 0. replace it with 1.
                // `1` is a valid seq. `0` is used by tombstone.
                // 2023-06-06: by drdr.xp@gmail.com
                if value.seq == 0 {
                    value.seq = 1;
                }

                let marked = Marked::from(value);
                let (k, v) = RotblCodec::encode_key_seq_marked(&key, marked)?;
                return Ok(Some((k, v)));
            }
            SMEntry::GenericKV { key, value } => {
                let marked = Marked::from(value);
                let (k, v) = RotblCodec::encode_key_seq_marked(&key, marked)?;
                return Ok(Some((k, v)));
            }
            SMEntry::Sequences { key: _, value } => {
                let mut s = self.sys_data.lock().unwrap();
                s.update_seq(value.0);
            }
        }
        Ok(None)
    }
}

impl ordq::Work for SnapshotUpgradeV002ToV003 {
    type I = WriteEntry<Vec<String>>;
    type O = Result<WriteEntry<Vec<(String, SeqMarked)>>, io::Error>;

    fn run(&mut self, strings: Self::I) -> Self::O {
        let WriteEntry::Data(strings) = strings else {
            return Ok(WriteEntry::Finish(()));
        };

        let mut res = Vec::with_capacity(strings.len());
        for s in strings {
            if let Some(kv) = self.convert_line(&s)? {
                res.push(kv)
            }
        }

        Ok(WriteEntry::Data(res))
    }
}

/// Upgrade snapshot V002(ndjson) to V003(rotbl).
///
/// After install, the state machine has only one level of data.
pub async fn upgrade_snapshot_data_v002_to_v003(
    snapshot_store: &SnapshotStoreV003,
    data: Box<SnapshotData>,
    snapshot_id: SnapshotId,
) -> Result<DB, io::Error> {
    fn closed_err(e: impl std::error::Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, format!("channel closed: {}", e))
    }

    let data_size = data.data_size().await?;
    info!(
        "upgrade snapshot from v002 to v003, data len: {}",
        data_size
    );

    // It create thread pool with ordq:
    // ```
    // file -> ordq -+-> worker -+--> writer
    //               |           |
    //               `-> worker -'
    // ```

    let sys_data = Arc::new(Mutex::new(SysData::default()));

    // Create a writer to write converted kvs to snapshot v003
    let writer = snapshot_store.new_writer()?;
    let (writer_tx, writer_join_handle) =
        writer.spawn_writer_thread("upgrade_snapshot_data_v002_to_v003");

    // Create a worker pool to convert the ndjson lines.
    let (ordq_tx, ordq_rx) = {
        let queue_depth = 1024;
        let n_workers = 16;

        ordq::new(
            queue_depth,
            repeat_with(|| SnapshotUpgradeV002ToV003 {
                sys_data: sys_data.clone(),
            })
            .take(n_workers),
        )
    };

    // Chain ordq output to writer
    databend_common_base::runtime::spawn_blocking(move || {
        while let Some(res) = ordq_rx.recv() {
            let res = res.map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("ordq recv error: {}", e))
            })?; // ordq recv error

            let ent = res?; // io error

            match ent {
                WriteEntry::Data(lines) => {
                    for (k, v) in lines {
                        writer_tx
                            .blocking_send(WriteEntry::Data((k, v)))
                            .map_err(closed_err)?;
                    }
                }
                WriteEntry::Finish(_) => {
                    let sys_data = sys_data.lock().unwrap().clone();
                    writer_tx
                        .blocking_send(WriteEntry::Finish(sys_data))
                        .map_err(closed_err)?;
                }
            }
        }
        Ok::<(), io::Error>(())
    });

    let f = data.into_std().await;

    // Feed input to the worker pool.
    databend_common_base::runtime::spawn_blocking(move || {
        {
            let mut br = io::BufReader::with_capacity(16 * 1024 * 1024, f);
            let lines = io::BufRead::lines(&mut br);
            for c in &lines.into_iter().chunks(1024) {
                let chunk = c.collect::<Result<Vec<_>, _>>()?;
                ordq_tx
                    .send(WriteEntry::Data(chunk))
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            }

            ordq_tx
                .send(WriteEntry::Finish(()))
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }

        Ok::<_, io::Error>(())
    });

    let temp_snapshot = writer_join_handle.await.map_err(closed_err)??;
    let db = temp_snapshot.move_to_final_path(snapshot_id)?;
    info!(
        "upgraded snapshot from v002 to v003: file_size: {}, db stat: {}, sys_data: {}",
        db.inner().file_size(),
        db.inner().stat(),
        db.inner().meta().user_data(),
    );

    Ok(db)
}
