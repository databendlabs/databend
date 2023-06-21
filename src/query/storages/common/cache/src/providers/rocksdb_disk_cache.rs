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

use std::fs;
use std::fs::File;
use std::io::IoSlice;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;

use chrono::Utc;
use common_exception::Result;
use rocksdb::MergeOperands;
use rocksdb::TransactionDBOptions;
use tracing::error;
use tracing::warn;

use super::disk_cache::validate_checksum;
use crate::DiskCacheKey;

static ROCKSDB_META_PATH: &str = "meta";
static ROCKSDB_DATA_PATH: &str = "data";
pub(crate) const ROCKSDB_LRU_ACCESS_COUNT: u8 = 3;

// map key -> (time,id,len)
pub(crate) const KEY2TIME_COLUMN_PREFIX: &str = "_key2time";

// map (time,id) -> key
pub(crate) const TIME2KEY_COLUMN_PREFIX: &str = "_time2key";
// system data key
pub(crate) const DISK_SIZE_KEY: &str = "_system/disk_size";

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct KeyTimeValue {
    pub time: i64,
    pub value_len: usize,
    // key access count, only keys which access count >= `ROCKSDB_LRU_ACCESS_COUNT`
    // can move to head of LRU list.
    pub count: u8,
}

pub struct RocksDbDiskCache {
    db: rocksdb::TransactionDB,
    limit: i64,
    data_root: PathBuf,
}

pub fn i64_merge_operator(
    _: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    // first get the old value
    let mut value = if let Some(v) = existing_val {
        let data: [u8; 8] = v[0..8].try_into().unwrap();
        i64::from_ne_bytes(data)
    } else {
        0
    };
    // second merge operands
    for op in operands {
        value += i64::from_ne_bytes(op[0..8].try_into().unwrap())
    }
    Some(i64::to_ne_bytes(value).to_vec())
}

impl RocksDbDiskCache {
    pub fn new(path: &str, limit: i64) -> Result<RocksDbDiskCache> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_merge_operator_associative("merge i64 operator", i64_merge_operator);
        opts.set_min_write_buffer_number_to_merge(20);

        let rocksdb_meta_path = format!("{}/{}", path, ROCKSDB_META_PATH);
        let db: rocksdb::TransactionDB = rocksdb::TransactionDB::open(
            &opts,
            &TransactionDBOptions::default(),
            rocksdb_meta_path,
        )?;

        Ok(RocksDbDiskCache {
            db,
            limit,
            data_root: PathBuf::from(format!("{}/{}", path, ROCKSDB_DATA_PATH)),
        })
    }

    fn abs_path_of_cache_key(&self, key: &str) -> PathBuf {
        let cache_key = DiskCacheKey::from(key);
        let path = PathBuf::from(&cache_key);
        self.data_root.join(path)
    }

    fn update_cache_key_time(
        &self,
        key2time_key: &String,
        key: &str,
        time_value: Option<&Vec<u8>>,
        value_len: Option<usize>,
    ) -> Result<()> {
        let txn = self.db.transaction();

        let value_len = if let Some(time_value) = time_value {
            let mut key_time_value: KeyTimeValue = serde_json::from_slice(time_value)?;
            key_time_value.count += 1;
            // access count less than `ROCKSDB_LRU_ACCESS_COUNT` cannot move to head of LRU list
            if key_time_value.count < ROCKSDB_LRU_ACCESS_COUNT {
                // in this case just update access count and return
                txn.put(key2time_key, serde_json::to_string(&key_time_value)?)?;
                txn.commit()?;
                return Ok(());
            }

            // delete old time key
            let key_hash = crc32fast::hash(key.as_bytes());
            let old_time_key = format!(
                "{}/{}/{}",
                TIME2KEY_COLUMN_PREFIX, key_time_value.time, key_hash
            );
            txn.delete(old_time_key)?;
            key_time_value.value_len
        } else {
            value_len.unwrap()
        };
        // add new time key
        let key_hash = crc32fast::hash(key.as_bytes());
        let now = Utc::now().timestamp_micros();
        let time_key = format!("{}/{}/{}", TIME2KEY_COLUMN_PREFIX, now, key_hash);
        txn.put(time_key, key)?;
        let new_key_time_value = KeyTimeValue {
            time: now,
            value_len,
            count: 0,
        };
        txn.put(key2time_key, serde_json::to_string(&new_key_time_value)?)?;

        txn.commit()?;

        Ok(())
    }

    fn get_system_disk(&self) -> i64 {
        if let Ok(Some(bytes)) = self.db.get(DISK_SIZE_KEY) {
            return i64::from_ne_bytes(bytes[0..8].try_into().unwrap());
        }
        0
    }

    fn ensure_space(&self, len: usize) -> Result<()> {
        let disk_size = self.get_system_disk();
        if disk_size + len as i64 <= self.limit {
            return Ok(());
        }

        let mut iter = self.db.prefix_iterator(TIME2KEY_COLUMN_PREFIX);
        let mut evicted_len = 0;
        while let Some(Ok((time_key, v))) = iter.next() {
            let key = String::from_utf8(v.as_ref().to_vec())?;
            let key2time_key = format!("{}/{}", KEY2TIME_COLUMN_PREFIX, &key);
            if let Ok(Some(value)) = self.db.get(&key2time_key) {
                let key_time_value: KeyTimeValue = serde_json::from_slice(&value)?;
                let txn = self.db.transaction();

                let _ = txn.delete(&time_key);
                let _ = txn.delete(&key2time_key);

                if let Err(e) = txn.commit() {
                    error!("Error remove key {} meta data {}", key, e);
                }

                // ignore remove meta error, continue to remove disk cache
                let cache_file_path = self.abs_path_of_cache_key(&key);
                if let Err(e) = fs::remove_file(&cache_file_path) {
                    error!(
                        "Error removing file from cache: `{:?}`: {}",
                        cache_file_path, e
                    );
                    continue;
                }

                evicted_len += key_time_value.value_len;

                // update disk size stat
                let add: i64 = -(key_time_value.value_len as i64);
                let _ = self.db.merge(DISK_SIZE_KEY, i64::to_ne_bytes(add));
            }
            if evicted_len >= len {
                break;
            }
        }

        Ok(())
    }

    fn put_value_to_disk(&self, key: &str, bytes: &[&[u8]]) -> Result<()> {
        let path = self.abs_path_of_cache_key(key);
        if let Some(parent_path) = path.parent() {
            fs::create_dir_all(parent_path)?;
        }
        let mut f = File::create(&path)?;
        let mut bufs = Vec::with_capacity(bytes.len());
        for slice in bytes {
            bufs.push(IoSlice::new(slice));
        }
        f.write_all_vectored(&mut bufs)?;
        Ok(())
    }
}

impl RocksDbDiskCache {
    pub fn get<Q: AsRef<str>>(&self, key: Q) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let key2time_key = format!("{}/{}", KEY2TIME_COLUMN_PREFIX, key);
        if let Ok(Some(time_value)) = self.db.get(&key2time_key) {
            let cache_file_path = self.abs_path_of_cache_key(key);
            // check disk cache
            let get_cache_content = || {
                let mut v = vec![];
                let mut file = File::open(cache_file_path)?;
                file.read_to_end(&mut v)?;
                Ok::<_, Box<dyn std::error::Error>>(v)
            };

            return match get_cache_content() {
                Ok(mut bytes) => {
                    if let Err(e) = validate_checksum(bytes.as_slice()) {
                        error!("data cache, of key {key},  crc validation failure: {e}");
                        {
                            // remove the invalid cache, error of removal ignored
                            let r = self.db.delete(&key2time_key);
                            if let Err(e) = r {
                                warn!("failed to remove invalid cache item, key {key}. {e}");
                            }
                        }
                        Ok(None)
                    } else {
                        // trim the checksum bytes and return
                        let total_len = bytes.len();
                        let body_len = total_len - 4;
                        bytes.truncate(body_len);
                        self.update_cache_key_time(&key2time_key, key, Some(&time_value), None)?;
                        Ok(Some(bytes))
                    }
                }
                Err(e) => {
                    error!("get disk cache item failed, cache_key {key}. {e}");
                    Ok(None)
                }
            };
        }

        Ok(None)
    }

    pub fn put(&self, key: &str, value: &Vec<u8>) -> Result<()> {
        // first evict enough space for the put value
        let value_len = value.len();
        self.ensure_space(value_len)?;

        // second: put value to disk
        let crc = crc32fast::hash(value.as_slice());
        let crc_bytes = crc.to_le_bytes();
        self.put_value_to_disk(key, &[value.as_slice(), &crc_bytes])?;

        // third: use txn to update key access time and count
        let key2time_key = format!("{}/{}", KEY2TIME_COLUMN_PREFIX, key);
        self.update_cache_key_time(&key2time_key, key, None, Some(value_len))?;

        // final update disk size stats
        self.db
            .merge(DISK_SIZE_KEY, i64::to_ne_bytes(value_len as i64))?;

        Ok(())
    }

    pub fn evict(&self, _key: &str) -> bool {
        true
    }

    pub fn contains_key(&self, key: &str) -> bool {
        let key2time_key = format!("{}/{}", KEY2TIME_COLUMN_PREFIX, key);
        if let Ok(value) = self.db.get(key2time_key) {
            return value.is_some();
        }
        false
    }

    pub fn size(&self) -> u64 {
        0
    }

    pub fn len(&self) -> usize {
        0
    }
}
