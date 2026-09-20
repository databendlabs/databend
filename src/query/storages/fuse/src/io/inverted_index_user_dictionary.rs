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

//! Japanese user dictionaries for inverted indexes.
//!
//! A user dictionary is a small Lindera CSV (`surface,part_of_speech,reading` per line) that
//! adds vocabulary the embedded IPADIC does not know. The tokenizer must see exactly the same
//! dictionary at index time and at query time, so index creation snapshots the file from the
//! user's stage into the table's own storage under `_i_i_d/<sha256>.csv` and records that
//! location in the index options. The object is content addressed: it is immutable, safe to
//! cache for the lifetime of the process, and a different dictionary yields a different
//! location, hence different options and a new index version.

use std::collections::BTreeMap;
use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::LazyLock;

use databend_common_base::runtime::GlobalIORuntime;
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
use sha2::Digest;
use sha2::Sha256;

use crate::FuseTable;
use crate::io::TableMetaLocationGenerator;

/// Upper bound on a user dictionary CSV. Dictionaries are typically a few KiB; MeCab-style
/// domain dictionaries can reach a few MiB.
const MAX_INVERTED_INDEX_USER_DICTIONARY_SIZE: usize = 16 * 1024 * 1024;

const USER_DICTIONARY_CACHE_CAPACITY: usize = 64;

pub(crate) static JAPANESE_DICTIONARY: LazyLock<Dictionary> = LazyLock::new(|| {
    load_dictionary("embedded://ipadic").expect("the embedded IPADIC dictionary must be available")
});

/// Built dictionaries keyed by storage location. Locations are content addressed, so entries
/// never go stale; the LRU bound only limits memory for tenants with many dictionaries.
static USER_DICTIONARY_CACHE: LazyLock<Mutex<LruCache<String, Arc<UserDictionary>>>> =
    LazyLock::new(|| {
        Mutex::new(LruCache::new(
            NonZeroUsize::new(USER_DICTIONARY_CACHE_CAPACITY).unwrap(),
        ))
    });

/// A validated user dictionary read from the user's stage, ready to be uploaded.
pub struct InvertedIndexUserDictionary {
    content: Vec<u8>,
    digest: String,
}

impl InvertedIndexUserDictionary {
    /// Validates `content` (size, encoding and Lindera CSV format) and computes its digest.
    pub fn try_new(content: Vec<u8>) -> Result<Self> {
        build_inverted_index_user_dictionary(&content)?;
        let digest = inverted_index_user_dictionary_digest(&content);
        Ok(Self { content, digest })
    }

    /// Hex SHA-256 of the content; the object is named after it.
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Snapshots the dictionary into the storage of `table` and returns the location to record
    /// in the index options.
    ///
    /// Writing is idempotent: the object name is the content digest, so re-creating an index
    /// with the same dictionary, or two indexes sharing one dictionary, reuse a single object.
    /// An object left behind by a failed DDL is unreferenced and reclaimed by vacuum.
    pub async fn upload(&self, table: &FuseTable) -> Result<String> {
        self.upload_to(table.get_operator_ref(), table.meta_location_generator())
            .await
    }

    async fn upload_to(
        &self,
        operator: &Operator,
        location_generator: &TableMetaLocationGenerator,
    ) -> Result<String> {
        let location = location_generator.gen_inverted_index_dict_location(&self.digest);
        if operator.exists(&location).await? {
            info!("inverted index user dictionary already exists: {location}");
            return Ok(location);
        }
        operator.write(&location, self.content.clone()).await?;
        info!(
            "uploaded inverted index user dictionary: {location}, size={} bytes",
            self.content.len()
        );
        Ok(location)
    }
}

/// Hex SHA-256 of the dictionary content; used as the object name.
fn inverted_index_user_dictionary_digest(content: &[u8]) -> String {
    format!("{:x}", Sha256::digest(content))
}

/// Parses and builds a Lindera user dictionary from CSV bytes against the embedded IPADIC.
///
/// Lindera 5.3 only exposes a path-based CSV loader, so the content goes through a temporary
/// file. The result is what `Segmenter::new` consumes.
fn build_inverted_index_user_dictionary(content: &[u8]) -> Result<UserDictionary> {
    if content.len() > MAX_INVERTED_INDEX_USER_DICTIONARY_SIZE {
        return Err(ErrorCode::IndexOptionInvalid(format!(
            "user dictionary is {} bytes, exceeds the {} bytes limit",
            content.len(),
            MAX_INVERTED_INDEX_USER_DICTIONARY_SIZE
        )));
    }
    if std::str::from_utf8(content).is_err() {
        return Err(ErrorCode::IndexOptionInvalid(
            "user dictionary must be UTF-8 encoded CSV",
        ));
    }
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
    load_user_dictionary_from_csv(&JAPANESE_DICTIONARY.metadata, file.path())
        .map_err(|e| ErrorCode::IndexOptionInvalid(format!("invalid user dictionary: {e}")))
}

/// Loads the dictionary stored at `location`, building it on first use and caching the result.
///
/// The object name is its content digest, which is verified after reading so a corrupted or
/// tampered object is rejected instead of silently changing tokenization.
async fn load_inverted_index_user_dictionary(
    operator: &Operator,
    location: &str,
) -> Result<Arc<UserDictionary>> {
    if let Some(dictionary) = USER_DICTIONARY_CACHE.lock().get(location) {
        return Ok(dictionary.clone());
    }
    let content = operator.read(location).await?.to_vec();
    let expected = location
        .rsplit('/')
        .next()
        .and_then(|name| name.strip_suffix(".csv"))
        .unwrap_or_default();
    let actual = inverted_index_user_dictionary_digest(&content);
    if actual != expected {
        return Err(ErrorCode::StorageOther(format!(
            "inverted index user dictionary {location} is corrupted: content digest {actual} does not match its name"
        )));
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    const DICT: &[u8] = b"AI,\xe3\x82\xab\xe3\x82\xb9\xe3\x82\xbf\xe3\x83\xa0\xe5\x90\x8d\xe8\xa9\x9e,\xe3\x82\xa8\xe3\x83\xbc\xe3\x82\xa2\xe3\x82\xa4\nDatabend Cloud,\xe3\x82\xab\xe3\x82\xb9\xe3\x82\xbf\xe3\x83\xa0\xe5\x90\x8d\xe8\xa9\x9e,\xe3\x83\x86\xe3\x82\xa3\xe3\x83\xbc\n";

    #[test]
    fn test_build_user_dictionary_from_csv() {
        let dictionary = build_inverted_index_user_dictionary(DICT).unwrap();
        // Two entries: word ids 0 and 1 carry the custom part of speech from the CSV.
        for word_id in 0..2 {
            assert!(
                dictionary.word_details(word_id).contains(&"カスタム名詞"),
                "word {word_id} details: {:?}",
                dictionary.word_details(word_id)
            );
        }
    }

    #[tokio::test]
    async fn test_upload_is_idempotent_and_load_is_cached() -> Result<()> {
        let operator = Operator::new(opendal::services::Memory::default())?.finish();
        let location_generator = TableMetaLocationGenerator::new("1/2".to_string());
        let dictionary = InvertedIndexUserDictionary::try_new(DICT.to_vec())?;
        assert_eq!(dictionary.digest().len(), 64);

        let location = dictionary.upload_to(&operator, &location_generator).await?;
        assert_eq!(location, format!("1/2/_i_i_d/{}.csv", dictionary.digest()));
        let again = dictionary.upload_to(&operator, &location_generator).await?;
        assert_eq!(location, again);

        let first = load_inverted_index_user_dictionary(&operator, &location).await?;
        let second = load_inverted_index_user_dictionary(&operator, &location).await?;
        assert!(Arc::ptr_eq(&first, &second));

        // An object whose content does not match its name is rejected.
        let bad_location = location_generator.gen_inverted_index_dict_location(&"0".repeat(64));
        operator.write(&bad_location, DICT.to_vec()).await?;
        assert!(
            load_inverted_index_user_dictionary(&operator, &bad_location)
                .await
                .is_err()
        );

        let mut options = BTreeMap::new();
        assert!(
            resolve_inverted_index_user_dictionary(&operator, &options)
                .await?
                .is_none()
        );
        options.insert(
            INVERTED_INDEX_OPT_USER_DICTIONARY_LOCATION.to_string(),
            location.clone(),
        );
        assert!(
            resolve_inverted_index_user_dictionary(&operator, &options)
                .await?
                .is_some()
        );
        Ok(())
    }
}
