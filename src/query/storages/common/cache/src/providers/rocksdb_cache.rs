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

use chrono::Utc;
use common_exception::Result;
use rocksdb::TransactionDBOptions;
use tracing::error;

use super::rocksdb_disk_cache::i64_merge_operator;
use super::rocksdb_disk_cache::KeyTimeValue;
use super::rocksdb_disk_cache::DISK_SIZE_KEY;
use super::rocksdb_disk_cache::KEY2TIME_COLUMN_PREFIX;
use super::rocksdb_disk_cache::ROCKSDB_LRU_ACCESS_COUNT;
use super::rocksdb_disk_cache::TIME2KEY_COLUMN_PREFIX;

// map key -> value
static KEY2VALUE_COLUMN_PREFIX: &str = "_key2value";

pub struct RocksDbCache {
    db: rocksdb::TransactionDB,
    limit: i64,
}

impl RocksDbCache {
    pub fn new(path: &str, limit: i64) -> Result<RocksDbCache> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_merge_operator_associative("merge i64 operator", i64_merge_operator);
        opts.set_min_write_buffer_number_to_merge(20);

        let db: rocksdb::TransactionDB =
            rocksdb::TransactionDB::open(&opts, &TransactionDBOptions::default(), path)?;

        Ok(RocksDbCache { db, limit })
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

                let key2value_key = format!("{}/{}", KEY2VALUE_COLUMN_PREFIX, &key);

                let _ = txn.delete(&time_key);
                let _ = txn.delete(&key2time_key);
                let _ = txn.delete(&key2value_key);

                if let Err(e) = txn.commit() {
                    error!("Error remove key {} meta data {}", key, e);
                    continue;
                }

                evicted_len += key_time_value.value_len;
                let add: i64 = -(key_time_value.value_len as i64);
                let _ = self.db.merge(DISK_SIZE_KEY, i64::to_ne_bytes(add));
            }
            if evicted_len >= len {
                break;
            }
        }

        Ok(())
    }
}

impl RocksDbCache {
    pub fn get<Q: AsRef<str>>(&self, key: Q) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let key2time_key = format!("{}/{}", KEY2TIME_COLUMN_PREFIX, key);
        if let Ok(Some(time_value)) = self.db.get(&key2time_key) {
            let key2value_key = format!("{}/{}", KEY2VALUE_COLUMN_PREFIX, key);
            if let Ok(Some(value)) = self.db.get(key2value_key) {
                // use txn to update key access time
                let txn = self.db.transaction();

                let mut key_time_value: KeyTimeValue = serde_json::from_slice(&time_value)?;
                key_time_value.count += 1;
                // access count less than ROCKSDB_LRU_ACCESS_COUNT cannot move to head of LRU list
                if key_time_value.count < ROCKSDB_LRU_ACCESS_COUNT {
                    txn.put(key2time_key, serde_json::to_string(&key_time_value)?)?;
                    txn.commit()?;
                    return Ok(Some(value));
                }

                let key_hash = crc32fast::hash(key.as_bytes());
                // first delete old time key
                let old_time_key = format!(
                    "{}/{}/{}",
                    TIME2KEY_COLUMN_PREFIX, key_time_value.time, key_hash
                );
                txn.delete(old_time_key)?;

                // add new time key
                let now = Utc::now().timestamp_micros();
                let time_key = format!("{}/{}/{}", TIME2KEY_COLUMN_PREFIX, now, key_hash);
                txn.put(time_key, key)?;
                let new_key_time_value = KeyTimeValue {
                    time: now,
                    value_len: key_time_value.value_len,
                    count: 0,
                };
                txn.put(&key2time_key, serde_json::to_string(&new_key_time_value)?)?;

                txn.commit()?;

                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    pub fn put(&self, key: &str, value: &Vec<u8>) -> Result<()> {
        // first evict enough space for the put value
        let value_len = value.len();
        self.ensure_space(value_len)?;

        // second: use txn to put value
        let txn = self.db.transaction();

        let key_hash = crc32fast::hash(key.as_bytes());

        let now = Utc::now().timestamp_micros();
        let time_key = format!("{}/{}/{}", TIME2KEY_COLUMN_PREFIX, now, key_hash);
        txn.put(time_key, key)?;
        let key_time_value = KeyTimeValue {
            time: now,
            value_len,
            count: 0,
        };
        let key2time_key = format!("{}/{}", KEY2TIME_COLUMN_PREFIX, key);
        txn.put(key2time_key, serde_json::to_string(&key_time_value)?)?;
        let key2value_key = format!("{}/{}", KEY2VALUE_COLUMN_PREFIX, key);
        txn.put(key2value_key, value)?;

        txn.commit()?;

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
