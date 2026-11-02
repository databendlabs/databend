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

//! Entry files: the value and its crc, written under a tmp name and renamed.

use std::fs;

use bytes::Bytes;
use databend_storages_common_cache::disk::disk_file::TMP_SUFFIX;
use databend_storages_common_cache::disk::disk_file::is_own_tmp;
use databend_storages_common_cache::disk::disk_file::read_entry;
use databend_storages_common_cache::disk::disk_file::tmp_path;
use databend_storages_common_cache::disk::disk_file::validate_checksum;
use databend_storages_common_cache::disk::disk_file::write_entry;
use tempfile::TempDir;

#[test]
fn test_entry_round_trip_and_checksum() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("k/entry");
    let tmp = tmp_path(&path, 1);
    assert!(is_own_tmp(tmp.file_name().unwrap().to_str().unwrap()));
    assert!(tmp.to_str().unwrap().ends_with(TMP_SUFFIX));

    let written = write_entry(&path, &tmp, b"value", false).unwrap();
    assert_eq!(written, 9);
    assert!(!tmp.exists());
    assert_eq!(
        read_entry(path.clone(), 9).unwrap(),
        Bytes::from_static(b"value")
    );

    let mut bytes = fs::read(&path).unwrap();
    bytes[0] ^= 1;
    fs::write(&path, bytes).unwrap();
    assert!(read_entry(path, 9).is_err());
    assert!(validate_checksum(b"abc").is_err());
}
