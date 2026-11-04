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
//! A file is written under a temporary name and renamed into place by the pool when it accepts
//! the entry, so a reader, or the restart scan, sees either no file or a complete one that was
//! accepted.

use std::fs;
use std::fs::File;
use std::io::IoSlice;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

/// Suffix of files being written; files left with it by a crash are removed on restart.
pub const TMP_SUFFIX: &str = "_tmp";

/// Names this run of the process: its pid and start time. A pid alone is reused across
/// restarts, by a container restarting its first process for one.
static RUN_ID: LazyLock<String> = LazyLock::new(|| {
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("{:x}p{started:x}", std::process::id())
});

/// `<path>.<run>-<seq>_tmp`: unique per write, so concurrent writes of one key do not collide,
/// and tagged with the run of the process, so a restart scan keeps the files this run is
/// writing and deletes those of earlier runs.
pub fn tmp_path(path: &Path, seq: u64) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{}-{seq}{TMP_SUFFIX}", *RUN_ID));
    path.with_file_name(name)
}

/// Whether a temporary file was named by `tmp_path` in this run of the process.
pub fn is_own_tmp(name: &str) -> bool {
    name.contains(&format!(".{}-", *RUN_ID))
}

/// Reads an entry of `size` bytes, crc included, and returns the value without the crc.
pub fn read_entry(path: PathBuf, size: usize) -> Result<Bytes> {
    let mut bytes = Vec::with_capacity(size);
    std::io::Read::read_to_end(&mut open_entry(&path)?, &mut bytes)?;
    validate_checksum(&bytes)?;
    bytes.truncate(bytes.len() - 4);
    Ok(bytes.into())
}

/// Opens an entry file to read it. On Linux without updating its access time: a cache hit then
/// writes no metadata, whatever the mount's atime option.
fn open_entry(path: &Path) -> std::io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::Mode;
        use rustix::fs::OFlags;
        let flags = OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOATIME;
        match rustix::fs::open(path, flags, Mode::empty()) {
            Ok(fd) => return Ok(File::from(fd)),
            // Only the owner of a file may leave its access time: open it as usual otherwise.
            Err(rustix::io::Errno::PERM) => {}
            Err(e) => return Err(e.into()),
        }
    }
    File::open(path)
}

/// Writes `value` and its crc to `tmp`, the temporary name of an entry file; the pool renames
/// it into place (see `DiskPool::publish`). Returns the bytes written.
pub fn write_tmp(tmp: &Path, value: &[u8], sync_data: bool) -> Result<u64> {
    let crc = crc32fast::hash(value).to_le_bytes();
    if let Some(parent) = tmp.parent() {
        fs::create_dir_all(parent)?;
    }
    let written = (|| {
        let mut f = File::create(tmp)?;
        f.write_all_vectored(&mut [IoSlice::new(value), IoSlice::new(&crc)])?;
        if sync_data {
            f.sync_data()?;
        }
        Ok::<_, std::io::Error>(())
    })();
    if let Err(e) = written {
        let _ = fs::remove_file(tmp);
        return Err(e.into());
    }
    Ok((value.len() + crc.len()) as u64)
}

/// The crc32 checksum is stored at the end of `bytes` and encoded as le u32.
// Although parquet page has built-in crc, but it is optional (and not generated in parquet2)
pub fn validate_checksum(bytes: &[u8]) -> Result<()> {
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
    use super::*;

    #[test]
    fn test_read_entry_round_trip() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("entry");
        let written = write_tmp(&path, b"value", false).unwrap();
        let read = read_entry(path, written as usize).unwrap();
        assert_eq!(read.as_ref(), b"value");
    }

    /// Entries are read without updating their access time.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_entries_are_opened_without_access_time() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("entry");
        write_tmp(&path, b"value", false).unwrap();
        let file = open_entry(&path).unwrap();
        let flags = rustix::fs::fcntl_getfl(&file).unwrap();
        assert!(flags.contains(rustix::fs::OFlags::NOATIME));
    }
}
