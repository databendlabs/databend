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

// Japanese user dictionaries for inverted indexes.
//
// A user dictionary is a small Lindera CSV (`surface,part_of_speech,reading` per line) that
// adds vocabulary the embedded IPADIC does not know. The tokenizer must see exactly the same
// dictionary at index time and at query time, so index creation snapshots the file from the
// user's stage into the table's own storage under `_i_i_d/h<uuid_v7>.csv` and records that
// location in the index options.

use std::collections::BTreeMap;
use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::LazyLock;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::catch_unwind;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_table_meta::table::INVERTED_INDEX_OPT_USER_DICTIONARY_LOCATION;
use lindera::dictionary::Dictionary;
use lindera::dictionary::UserDictionary;
use lindera::dictionary::load_dictionary;
use lindera::dictionary::load_user_dictionary_from_csv;
use log::info;
use lru::LruCache;
use opendal::Operator;
use parking_lot::Mutex;

use crate::FuseTable;

/// Upper bound on a user dictionary CSV. Dictionaries are typically a few KiB; MeCab-style
/// domain dictionaries can reach a few MiB.
pub const MAX_INVERTED_INDEX_USER_DICTIONARY_SIZE: usize = 16 * 1024 * 1024;

const USER_DICTIONARY_CACHE_CAPACITY: usize = 64;

pub(crate) static JAPANESE_DICTIONARY: LazyLock<Dictionary> = LazyLock::new(|| {
    load_dictionary("embedded://ipadic").expect("the embedded IPADIC dictionary must be available")
});

/// Built dictionaries keyed by immutable storage location. The LRU bound limits memory for
/// tenants with many dictionaries.
static USER_DICTIONARY_CACHE: LazyLock<Mutex<LruCache<String, Arc<UserDictionary>>>> =
    LazyLock::new(|| {
        Mutex::new(LruCache::new(
            NonZeroUsize::new(USER_DICTIONARY_CACHE_CAPACITY).unwrap(),
        ))
    });

/// A validated user dictionary read from the user's stage, ready to be uploaded.
pub struct InvertedIndexUserDictionary {
    content: Vec<u8>,
}

impl InvertedIndexUserDictionary {
    /// Validates `content` (size, encoding and Lindera CSV format).
    pub fn try_new(content: Vec<u8>) -> Result<Self> {
        if content.len() > MAX_INVERTED_INDEX_USER_DICTIONARY_SIZE {
            return Err(ErrorCode::IndexOptionInvalid(format!(
                "user dictionary is {} bytes, exceeds the {} bytes limit",
                content.len(),
                MAX_INVERTED_INDEX_USER_DICTIONARY_SIZE
            )));
        }
        if std::str::from_utf8(&content).is_err() {
            return Err(ErrorCode::IndexOptionInvalid(
                "user dictionary must be UTF-8 encoded CSV",
            ));
        }
        build_inverted_index_user_dictionary(&content)?;
        Ok(Self { content })
    }

    /// Snapshots the dictionary into the storage of `table` and returns the location to record
    /// in the index options.
    pub async fn upload(&self, table: &FuseTable) -> Result<String> {
        let operator = table.get_operator_ref();
        let location_generator = table.meta_location_generator();

        let location = location_generator.gen_inverted_index_dict_location();
        operator.write(&location, self.content.clone()).await?;
        info!(
            "uploaded inverted index user dictionary: {location}, size={} bytes",
            self.content.len()
        );
        Ok(location)
    }
}

/// Compiles CSV against the embedded IPADIC.
fn build_inverted_index_user_dictionary(content: &[u8]) -> Result<UserDictionary> {
    let mut file = tempfile::NamedTempFile::new().map_err(|e| {
        ErrorCode::StorageOther(format!(
            "failed to create temporary file for user dictionary: {e}"
        ))
    })?;
    file.write_all(content)
        .and_then(|_| file.flush())
        .map_err(|e| {
            ErrorCode::StorageOther(format!(
                "failed to write temporary file for user dictionary: {e}"
            ))
        })?;
    // Lindera 5.3 indexes numeric columns before validating row lengths. Malformed short
    // records can panic; report them as invalid input rather than unwinding the DDL task.
    let dictionary =
        catch_unwind(|| load_user_dictionary_from_csv(&JAPANESE_DICTIONARY.metadata, file.path()))
            .map_err(|_| ErrorCode::IndexOptionInvalid("invalid user dictionary CSV record"))?
            .map_err(|e| ErrorCode::IndexOptionInvalid(format!("invalid user dictionary: {e}")))?;
    validate_user_dictionary_context_ids(&dictionary)?;
    Ok(dictionary)
}

/// Compilation only checks that context IDs fit in u16, not the IPADIC matrix.
fn validate_user_dictionary_context_ids(dictionary: &UserDictionary) -> Result<()> {
    // Lindera 5.3 serializes WordEntry as [u32 word_id, i16 cost, u16 left, u16 right],
    // little endian. Its deserializer is private; use the same layout as remap_context_ids.
    const WORD_ENTRY_LEN: usize = 10;
    let (entries, remainder) = dictionary.dict.vals_data.as_chunks::<WORD_ENTRY_LEN>();
    if !remainder.is_empty() {
        return Err(ErrorCode::IndexOptionInvalid(
            "invalid user dictionary word entry layout",
        ));
    }
    let matrix = &JAPANESE_DICTIONARY.connection_cost_matrix;
    let context_id_map = JAPANESE_DICTIONARY.metadata.context_id_map.as_ref();
    for entry in entries {
        let left = u16::from_le_bytes([entry[6], entry[7]]);
        let right = u16::from_le_bytes([entry[8], entry[9]]);
        if u32::from(left) >= matrix.backward_size || u32::from(right) >= matrix.forward_size {
            return Err(ErrorCode::IndexOptionInvalid(format!(
                "invalid user dictionary context IDs: left={left}, right={right}; IPADIC requires left < {} and right < {}",
                matrix.backward_size, matrix.forward_size
            )));
        }
        // Segmenter remaps context IDs before accessing the matrix. Validate that space too,
        // without mutating the dictionary (Segmenter will perform the actual remapping).
        if let Some(map) = context_id_map {
            let mapped_left = map.left.get(usize::from(left)).copied();
            let mapped_right = map.right.get(usize::from(right)).copied();
            if !mapped_left.is_some_and(|id| u32::from(id) < matrix.backward_size)
                || !mapped_right.is_some_and(|id| u32::from(id) < matrix.forward_size)
            {
                return Err(ErrorCode::IndexOptionInvalid(format!(
                    "invalid user dictionary context IDs after IPADIC remapping: left={left}, right={right}"
                )));
            }
        }
    }
    Ok(())
}

/// Loads the dictionary stored at `location`, building it on first use and caching the result.
async fn load_inverted_index_user_dictionary(
    operator: &Operator,
    location: &str,
) -> Result<Arc<UserDictionary>> {
    if let Some(dictionary) = USER_DICTIONARY_CACHE.lock().get(location) {
        return Ok(dictionary.clone());
    }
    let content = operator.read(location).await?.to_vec();
    let dictionary = Arc::new(build_inverted_index_user_dictionary(&content)?);
    USER_DICTIONARY_CACHE
        .lock()
        .put(location.to_string(), dictionary.clone());
    Ok(dictionary)
}

/// Resolves the dictionary referenced by inverted index `options`, if any.
pub async fn resolve_inverted_index_user_dictionary(
    operator: &Operator,
    options: &BTreeMap<String, String>,
) -> Result<Option<Arc<UserDictionary>>> {
    match options.get(INVERTED_INDEX_OPT_USER_DICTIONARY_LOCATION) {
        Some(location) => Ok(Some(
            load_inverted_index_user_dictionary(operator, location).await?,
        )),
        None => Ok(None),
    }
}

/// Synchronous variant of [`resolve_inverted_index_user_dictionary`] for pipeline construction,
/// which runs outside an `async` context. The cache is consulted first, so the IO runtime is only
/// entered the first time a process sees a given dictionary.
pub fn resolve_inverted_index_user_dictionary_blocking(
    operator: &Operator,
    options: &BTreeMap<String, String>,
) -> Result<Option<Arc<UserDictionary>>> {
    let Some(location) = options.get(INVERTED_INDEX_OPT_USER_DICTIONARY_LOCATION) else {
        return Ok(None);
    };
    if let Some(dictionary) = USER_DICTIONARY_CACHE.lock().get(location) {
        return Ok(Some(dictionary.clone()));
    }
    let operator = operator.clone();
    let location = location.clone();
    let dictionary = GlobalIORuntime::instance()
        .block_on(async move { load_inverted_index_user_dictionary(&operator, &location).await })?;
    Ok(Some(dictionary))
}
