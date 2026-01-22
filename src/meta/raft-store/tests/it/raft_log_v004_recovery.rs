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

//! Test for raft log recovery after incomplete write.
//!
//! Verifies that the raft log can be successfully reopened after truncation
//! that simulates an incomplete/partial write (e.g., from a crash).
//! The `truncate_incomplete_record` config option enables automatic recovery
//! by truncating incomplete records at the end of the WAL file.

use std::fs;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

use databend_common_meta_raft_store::raft_log_v004::Cw;
use databend_common_meta_raft_store::raft_log_v004::RaftLogConfig;
use databend_common_meta_raft_store::raft_log_v004::RaftLogV004;
use databend_common_meta_raft_store::raft_log_v004::util::blocking_flush;
use databend_common_meta_types::raft_types::EntryPayload;
use databend_common_meta_types::raft_types::new_log_id;
use raft_log::DumpApi;
use raft_log::api::raft_log_writer::RaftLogWriter;

/// Test that a raft log with truncated trailing bytes (simulating partial/incomplete write)
/// can be successfully reopened with truncation enabled.
///
/// Tests multiple truncation sizes to cover different scenarios:
/// - Small values (1-5): minimal truncations within checksum
/// - Checksum boundary (7-10): around 8-byte checksum at end of each record
/// - Mid-record (20, 25, 30): truncation within record payload
/// - Record boundary (50-54): around 52-byte record size
/// - Multiple records (100, 104, 150, 200): truncating several records
#[tokio::test]
async fn test_raft_log_recovery_after_truncation() -> anyhow::Result<()> {
    let truncation_sizes = [
        // Small truncations (within checksum)
        1, 2, 3, 4, 5,
        // Around checksum boundary (8-byte checksum at end of each record)
        7, 8, 9, 10,
        // Mid-record (within payload)
        20, 25, 30,
        // Around record boundary (each record is 52 bytes)
        50, 51, 52, 53, 54,
        // Multiple records
        100, 104, 150, 200,
    ];

    for bytes_to_truncate in truncation_sizes {
        test_truncation_recovery(bytes_to_truncate).await?;
    }

    Ok(())
}

async fn test_truncation_recovery(bytes_to_truncate: u64) -> anyhow::Result<()> {
    println!(
        "\n=== Testing truncation of {} bytes ===",
        bytes_to_truncate
    );

    let temp_dir = tempfile::tempdir()?;
    let log_dir = temp_dir.path().join("log");
    fs::create_dir_all(&log_dir)?;

    let config = Arc::new(RaftLogConfig {
        dir: log_dir.to_str().unwrap().to_string(),
        log_cache_max_items: Some(1000),
        log_cache_capacity: Some(1024 * 1024),
        chunk_max_records: Some(100),
        chunk_max_size: Some(1024 * 1024),
        read_buffer_size: None,
        truncate_incomplete_record: Some(true),
    });

    let num_entries: u64 = 10;

    // Phase 1: Write entries and close the log
    {
        let mut log = RaftLogV004::open(config.clone())?;

        for i in 0..num_entries {
            let log_id = new_log_id(1, 0, i);
            log.append([(Cw(log_id), Cw(EntryPayload::Blank))])?;
        }

        blocking_flush(&mut log).await?;

        let dumped = log.dump().write_to_string()?;
        println!("dump after filling in data: {}", dumped);
    }

    // Phase 2: Truncate the WAL file to simulate incomplete write
    let wal_file = find_wal_file(&log_dir)?;
    truncate_file(&wal_file, bytes_to_truncate)?;

    // Phase 3: Reopen the log - should succeed with automatic truncation
    let log = RaftLogV004::open(config.clone())?;

    let dumped = log.dump().write_to_string()?;
    println!(
        "dump after truncating {} bytes: {}",
        bytes_to_truncate, dumped
    );

    // Phase 4: Verify recovery
    let state = log.log_state();
    let last_log_id = state.last().map(|id| id.0);
    println!("last_log_id: {:?}", last_log_id);

    assert!(
        last_log_id.is_some(),
        "truncate {} bytes: Log should have entries after recovery",
        bytes_to_truncate
    );

    let last_index = last_log_id.unwrap().index;
    assert!(
        last_index < num_entries,
        "truncate {} bytes: Last index {} should be less than {} due to truncation",
        bytes_to_truncate,
        last_index,
        num_entries
    );

    // Verify remaining entries are readable
    let entries: Vec<_> = log.read(0, last_index + 1).collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        entries.len() as u64,
        last_index + 1,
        "truncate {} bytes: Should read all remaining entries",
        bytes_to_truncate
    );

    println!(
        "truncate {} bytes: OK - recovered {} entries",
        bytes_to_truncate,
        last_index + 1
    );

    Ok(())
}

fn find_wal_file(log_dir: &Path) -> anyhow::Result<std::path::PathBuf> {
    for entry in fs::read_dir(log_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(".wal") {
                    return Ok(path);
                }
            }
        }
    }
    anyhow::bail!("No WAL file found in {:?}", log_dir)
}

fn truncate_file(path: &Path, bytes_to_remove: u64) -> anyhow::Result<()> {
    let metadata = fs::metadata(path)?;
    let file_size = metadata.len();

    if file_size < bytes_to_remove {
        anyhow::bail!(
            "File size {} is smaller than bytes to remove {}",
            file_size,
            bytes_to_remove
        );
    }

    let new_size = file_size - bytes_to_remove;
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(new_size)?;
    file.sync_all()?;

    Ok(())
}
