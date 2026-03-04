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
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use databend_common_meta_process::pb_value_decoder::decode_cmd_values;
use databend_common_meta_process::pb_value_decoder::raw_cmd_values;
use databend_meta_raft_store::raft_log::Config;
use databend_meta_raft_store::raft_log::Dump;
use databend_meta_raft_store::raft_log::DumpApi;
use databend_meta_raft_store::raft_log::WALRecord;
use databend_meta_raft_store::raft_log::dump_writer::write_record_display;
use databend_meta_raft_store::raft_log_v004::RaftLogTypes;
use databend_meta_types::raft_types::EntryPayload;

use crate::args::DumpRaftLogWalArgs;

pub fn dump_raft_log_wal(args: &DumpRaftLogWalArgs) -> anyhow::Result<()> {
    let mut wal_dir = PathBuf::from(&args.raft_dir);
    wal_dir.push("df_meta");
    wal_dir.push("V004");
    wal_dir.push("log");

    dump_wal(&wal_dir, args.decode_values, args.raw, io::stdout())
}

pub fn dump_wal(
    wal_dir: &Path,
    decode_values: bool,
    raw: bool,
    mut w: impl Write,
) -> anyhow::Result<()> {
    let config = Arc::new(Config {
        dir: wal_dir.to_string_lossy().to_string(),
        ..Default::default()
    });

    let dump = Dump::<RaftLogTypes>::new(config)?;

    if !decode_values && !raw {
        dump.write_display(&mut w)?;
        return Ok(());
    }

    writeln!(w, "RaftLog:")?;

    dump.write_with(|chunk_id, idx, res| {
        let mut extra_lines = vec![];

        if let Ok((_seg, WALRecord::Append(_log_id, payload))) = &res
            && let EntryPayload::Normal(log_entry) = &payload.0
        {
            if decode_values {
                extra_lines.extend(decode_cmd_values(&log_entry.cmd));
            }
            if raw {
                extra_lines.extend(raw_cmd_values(&log_entry.cmd));
            }
        }

        write_record_display(&mut w, chunk_id, idx, res)?;

        for line in extra_lines {
            writeln!(w, "{}", line)?;
        }
        Ok(())
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use chrono::TimeZone;
    use chrono::Utc;
    use databend_common_meta_app::schema::DatabaseMeta;
    use databend_common_proto_conv::FromToProto;
    use databend_meta_raft_store::raft_log::Config;
    use databend_meta_raft_store::raft_log::api::raft_log_writer::RaftLogWriter;
    use databend_meta_raft_store::raft_log_v004::Cw;
    use databend_meta_raft_store::raft_log_v004::RaftLogV004;
    use databend_meta_raft_store::raft_log_v004::util::blocking_flush;
    use databend_meta_types::Cmd;
    use databend_meta_types::LogEntry;
    use databend_meta_types::UpsertKV;
    use databend_meta_types::raft_types::EntryPayload;
    use databend_meta_types::raft_types::new_log_id;
    use prost::Message;

    use super::*;

    #[tokio::test]
    async fn test_dump_wal_without_decode() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let wal_dir = tmp.path().join("log");
        std::fs::create_dir_all(&wal_dir)?;

        let config = Arc::new(Config {
            dir: wal_dir.to_string_lossy().to_string(),
            ..Default::default()
        });

        let mut log = RaftLogV004::open(config)?;
        log.append([(Cw(new_log_id(1, 0, 0)), Cw(EntryPayload::Blank))])?;
        blocking_flush(&mut log).await?;
        drop(log);

        let mut buf = Vec::new();
        dump_wal(&wal_dir, false, false, &mut buf)?;
        let output = String::from_utf8(buf)?;

        assert_eq!(
            output,
            concat!(
                "RaftLog:\n",
                "ChunkId(00_000_000_000_000_000_000)\n",
                "  R-00000: [000_000_000, 000_000_018) Size(18): RaftLogState(RaftLogState(vote: None, last: None, committed: None, purged: None, user_data: None))\n",
                "  R-00001: [000_000_018, 000_000_070) Size(52): Append(log_id: T1-N0.0, payload: blank)\n",
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dump_wal_decode_upsert_kv() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let wal_dir = tmp.path().join("log");
        std::fs::create_dir_all(&wal_dir)?;

        let config = Arc::new(Config {
            dir: wal_dir.to_string_lossy().to_string(),
            ..Default::default()
        });

        // Use a fixed timestamp to ensure deterministic protobuf encoding size
        // across platforms. `Utc::now()` in `DatabaseMeta::default()` produces
        // different varint sizes depending on the timestamp value.
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let meta = DatabaseMeta {
            engine: "".to_string(),
            engine_options: BTreeMap::new(),
            options: BTreeMap::new(),
            created_on: ts,
            updated_on: ts,
            comment: "".to_string(),
            drop_on: None,
            gc_in_progress: false,
        };
        let pb = meta.to_pb()?;
        let mut pb_buf = vec![];
        pb.encode(&mut pb_buf)?;

        let cmd = Cmd::UpsertKV(UpsertKV::update("__fd_database_by_id/123", &pb_buf));
        let log_entry = LogEntry::new(cmd);
        let payload = EntryPayload::Normal(log_entry);

        let mut log = RaftLogV004::open(config)?;
        log.append([(Cw(new_log_id(1, 0, 0)), Cw(payload))])?;
        blocking_flush(&mut log).await?;
        drop(log);

        let mut buf = Vec::new();
        dump_wal(&wal_dir, true, false, &mut buf)?;
        let output = String::from_utf8(buf)?;

        assert_eq!(
            output,
            concat!(
                "RaftLog:\n",
                "ChunkId(00_000_000_000_000_000_000)\n",
                "  R-00000: [000_000_000, 000_000_018) Size(18): RaftLogState(RaftLogState(vote: None, last: None, committed: None, purged: None, user_data: None))\n",
                r#"  R-00001: [000_000_018, 000_000_218) Size(200): Append(log_id: T1-N0.0, payload: normal: cmd: upsert_kv:__fd_database_by_id/123(GE(0)) = Update("[binary]") (None))"#,
                "\n",
                r#"    value(__fd_database_by_id/123): DatabaseMeta { engine: "", engine_options: {}, options: {}, created_on: 2024-01-01T00:00:00Z, updated_on: 2024-01-01T00:00:00Z, comment: "", drop_on: None, gc_in_progress: false }"#,
                "\n",
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dump_wal_raw() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let wal_dir = tmp.path().join("log");
        std::fs::create_dir_all(&wal_dir)?;

        let config = Arc::new(Config {
            dir: wal_dir.to_string_lossy().to_string(),
            ..Default::default()
        });

        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let meta = DatabaseMeta {
            engine: "".to_string(),
            engine_options: BTreeMap::new(),
            options: BTreeMap::new(),
            created_on: ts,
            updated_on: ts,
            comment: "".to_string(),
            drop_on: None,
            gc_in_progress: false,
        };
        let pb = meta.to_pb()?;
        let mut pb_buf = vec![];
        pb.encode(&mut pb_buf)?;

        let raw_bytes_str = format!(
            "[{}]",
            pb_buf
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        let cmd = Cmd::UpsertKV(UpsertKV::update("__fd_database_by_id/123", &pb_buf));
        let log_entry = LogEntry::new(cmd);
        let payload = EntryPayload::Normal(log_entry);

        let mut log = RaftLogV004::open(config)?;
        log.append([(Cw(new_log_id(1, 0, 0)), Cw(payload))])?;
        blocking_flush(&mut log).await?;
        drop(log);

        // --raw only
        let mut buf = Vec::new();
        dump_wal(&wal_dir, false, true, &mut buf)?;
        let output = String::from_utf8(buf)?;

        let expected = format!(
            concat!(
                "RaftLog:\n",
                "ChunkId(00_000_000_000_000_000_000)\n",
                "  R-00000: [000_000_000, 000_000_018) Size(18): RaftLogState(RaftLogState(vote: None, last: None, committed: None, purged: None, user_data: None))\n",
                r#"  R-00001: [000_000_018, 000_000_218) Size(200): Append(log_id: T1-N0.0, payload: normal: cmd: upsert_kv:__fd_database_by_id/123(GE(0)) = Update("[binary]") (None))"#,
                "\n",
                "    raw(__fd_database_by_id/123): {}\n",
            ),
            raw_bytes_str
        );

        assert_eq!(output, expected);

        // --decode-values --raw (both)
        let mut buf = Vec::new();
        dump_wal(&wal_dir, true, true, &mut buf)?;
        let output = String::from_utf8(buf)?;

        let expected = format!(
            concat!(
                "RaftLog:\n",
                "ChunkId(00_000_000_000_000_000_000)\n",
                "  R-00000: [000_000_000, 000_000_018) Size(18): RaftLogState(RaftLogState(vote: None, last: None, committed: None, purged: None, user_data: None))\n",
                r#"  R-00001: [000_000_018, 000_000_218) Size(200): Append(log_id: T1-N0.0, payload: normal: cmd: upsert_kv:__fd_database_by_id/123(GE(0)) = Update("[binary]") (None))"#,
                "\n",
                r#"    value(__fd_database_by_id/123): DatabaseMeta {{ engine: "", engine_options: {{}}, options: {{}}, created_on: 2024-01-01T00:00:00Z, updated_on: 2024-01-01T00:00:00Z, comment: "", drop_on: None, gc_in_progress: false }}"#,
                "\n",
                "    raw(__fd_database_by_id/123): {}\n",
            ),
            raw_bytes_str
        );

        assert_eq!(output, expected);

        Ok(())
    }
}
