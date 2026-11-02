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

//! Files of the disk cache: one per entry, the value followed by its crc32 (little endian).
//! A file is written under a temporary name and renamed into place, so a reader sees either no
//! file or a complete one.

use std::fs;
use std::fs::File;
use std::io::IoSlice;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

/// Suffix of files being written; files left with it by a crash are removed on restart.
pub(crate) const TMP_SUFFIX: &str = "_tmp";

/// `<path>.<pid>-<seq>_tmp`: unique per write, so concurrent writes of one key do not collide,
/// and tagged with the process, so a restart scan keeps the files this process is writing.
pub(crate) fn tmp_path(path: &Path, seq: u64) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{}-{seq}{TMP_SUFFIX}", std::process::id()));
    path.with_file_name(name)
}

/// Whether a temporary file was named by `tmp_path` in this process.
pub(crate) fn is_own_tmp(name: &str) -> bool {
    name.contains(&format!(".{}-", std::process::id()))
}

/// Reads an entry of `size` bytes, crc included, and returns the value without the crc.
pub(crate) fn read_entry(path: PathBuf, size: usize) -> Result<Bytes> {
    let mut bytes = Vec::with_capacity(size);
    std::io::Read::read_to_end(&mut File::open(path)?, &mut bytes)?;
    validate_checksum(&bytes)?;
    bytes.truncate(bytes.len() - 4);
    Ok(bytes.into())
}

/// Writes `value` and its crc to `tmp`, then renames it to `path`. Returns the bytes written.
pub(crate) fn write_entry(path: &Path, tmp: &Path, value: &[u8], sync_data: bool) -> Result<u64> {
    let crc = crc32fast::hash(value).to_le_bytes();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let written = (|| {
        let mut f = File::create(tmp)?;
        f.write_all_vectored(&mut [IoSlice::new(value), IoSlice::new(&crc)])?;
        if sync_data {
            f.sync_data()?;
        }
        fs::rename(tmp, path)
    })();
    if let Err(e) = written {
        let _ = fs::remove_file(tmp);
        return Err(e.into());
    }
    Ok((value.len() + crc.len()) as u64)
}

/// The crc32 checksum is stored at the end of `bytes` and encoded as le u32.
// Although parquet page has built-in crc, but it is optional (and not generated in parquet2)
pub(crate) fn validate_checksum(bytes: &[u8]) -> Result<()> {
    let total_len = bytes.len();
    if total_len <= 4 {
        Err(ErrorCode::StorageOther(format!(
            "crc checksum validation failure: invalid file length {total_len}"
        )))
    } else {
        // total_len > 4 is ensured
        let crc_bytes: [u8; 4] = bytes[total_len - 4..].try_into().unwrap();
        let crc_provided = u32::from_le_bytes(crc_bytes);
        let crc_calculated = crc32fast::hash(&bytes[0..total_len - 4]);
        if crc_provided == crc_calculated {
            Ok(())
        } else {
            Err(ErrorCode::StorageOther(format!(
                "crc checksum validation failure: provided {crc_provided}, calculated {crc_calculated}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn entry_round_trip_and_checksum() {
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
}
