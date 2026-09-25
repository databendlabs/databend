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

//! Keys of `TableIndex::options`, shared by the binder that validates them and the storage
//! layer that consumes them.

// Inverted index.

/// Text analyzer, one of `english`, `chinese`, `japanese`, `whitespace`.
pub const INVERTED_INDEX_OPT_TOKENIZER: &str = "tokenizer";
/// Comma separated token filters applied after the tokenizer.
pub const INVERTED_INDEX_OPT_FILTERS: &str = "filters";
/// Tantivy `IndexRecordOption`, serialized as JSON.
pub const INVERTED_INDEX_OPT_INDEX_RECORD: &str = "index_record";
/// Japanese segmentation mode, `normal` (default) or `decompose`. `decompose` splits long
/// compound words the dictionary knows as one entry into their components, like the `search`
/// mode of kuromoji.
pub const INVERTED_INDEX_OPT_MODE: &str = "mode";
/// User-facing stage location of a Japanese user dictionary, e.g. `@ja_dict/userdict.csv`.
/// Kept for `SHOW CREATE TABLE`; the analyzer never reads it.
pub const INVERTED_INDEX_OPT_USER_DICTIONARY: &str = "user_dictionary";
/// Internal: storage location of the dictionary snapshot taken when the index was created. The
/// object lives under the table's own storage prefix and is named after the content digest, so a
/// dictionary change produces a new location, hence new index options and a new index version.
pub const INVERTED_INDEX_OPT_USER_DICTIONARY_LOCATION: &str = "user_dictionary_location";

// Ngram index.

/// Number of characters per gram.
pub const NGRAM_INDEX_OPT_GRAM_SIZE: &str = "gram_size";
/// Bloom filter size in bytes.
pub const NGRAM_INDEX_OPT_BLOOM_SIZE: &str = "bloom_size";
/// Target bloom filter false positive rate, in `(0, 1)`.
pub const NGRAM_INDEX_OPT_FALSE_POSITIVE_RATE: &str = "false_positive_rate";
/// Hash algorithm, one of `city64_v0`, `rolling_v1`.
pub const NGRAM_INDEX_OPT_HASH_ALGORITHM: &str = "hash_algorithm";

// Vector index.

/// HNSW `m`, the number of bi-directional links per node.
pub const VECTOR_INDEX_OPT_M: &str = "m";
/// HNSW `ef_construct`, the candidate list size during construction.
pub const VECTOR_INDEX_OPT_EF_CONSTRUCT: &str = "ef_construct";
/// Comma separated distance functions the index is built for.
pub const VECTOR_INDEX_OPT_DISTANCE: &str = "distance";

/// Options that are derived by the system and must not be shown to or set by users.
pub fn is_internal_table_index_option(key: &str) -> bool {
    key == INVERTED_INDEX_OPT_USER_DICTIONARY_LOCATION
}
