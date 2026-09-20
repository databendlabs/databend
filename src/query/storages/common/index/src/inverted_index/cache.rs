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

use bytes::Bytes;
use databend_common_exception::ErrorCode;

use super::bundle::INVERTED_INDEX_BUNDLE_MAX_FOOTER_SIZE;
use super::bundle::InvertedIndexBundleFooter;

/// Maximum persisted footer bytes accepted from the metadata cache.
const FOOTER_CACHE_DECODE_LIMIT: usize = INVERTED_INDEX_BUNDLE_MAX_FOOTER_SIZE;
/// Maximum bytes in one cached lookup component or payload range.
const PAGE_CACHE_DECODE_LIMIT: usize = 1024 * 1024;
/// Bincode allocation limit for the metadata envelope containing a persisted footer.
const META_CACHE_DECODE_LIMIT: usize = 8 * 1024 * 1024;

pub fn inverted_index_meta_cache_key(location: &str) -> String {
    location.to_string()
}

/// Complete persisted footer for a raw Tantivy bundle stored in outer object format V1.
///
/// Historical container locations use format version 0 and are never inserted into this cache.
/// The footer bytes include the fixed trailer and can reconstruct all file ranges, open slices,
/// `.managed.json`, and `meta.json` without another object-storage read.
#[derive(Clone, Debug)]
pub struct InvertedIndexMeta {
    pub object_size: u64,
    pub footer_bytes: Bytes,
}

impl InvertedIndexMeta {
    pub fn new(object_size: u64, footer_bytes: Bytes) -> Self {
        Self {
            object_size,
            footer_bytes,
        }
    }

    fn validate(&self) -> Result<(), ErrorCode> {
        if self.footer_bytes.len() > FOOTER_CACHE_DECODE_LIMIT {
            return Err(ErrorCode::StorageOther(
                "cached inverted-index footer exceeds the maximum size",
            ));
        }
        InvertedIndexBundleFooter::open_footer_for_object(
            self.footer_bytes.as_ref(),
            self.object_size,
            None,
        )
        .map(|_| ())
        .map_err(|error| {
            ErrorCode::StorageOther(format!("cached inverted-index footer is invalid: {error}"))
        })
    }

    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.footer_bytes.len()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SerializableInvertedIndexMeta {
    object_size: u64,
    footer_bytes: Vec<u8>,
}

impl TryFrom<&InvertedIndexMeta> for Vec<u8> {
    type Error = ErrorCode;

    fn try_from(value: &InvertedIndexMeta) -> std::result::Result<Self, Self::Error> {
        value.validate()?;
        let serializable = SerializableInvertedIndexMeta {
            object_size: value.object_size,
            footer_bytes: value.footer_bytes.to_vec(),
        };
        bincode::serde::encode_to_vec(&serializable, bincode::config::standard()).map_err(|error| {
            ErrorCode::StorageOther(format!(
                "failed to encode inverted index metadata: {error:?}"
            ))
        })
    }
}

impl TryFrom<Bytes> for InvertedIndexMeta {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        let config = bincode::config::standard().with_limit::<{ META_CACHE_DECODE_LIMIT }>();
        let (metadata, len): (SerializableInvertedIndexMeta, usize) =
            bincode::serde::decode_from_slice(value.as_ref(), config).map_err(|error| {
                ErrorCode::StorageOther(format!(
                    "failed to decode inverted index metadata: {error:?}"
                ))
            })?;
        if len != value.len() {
            return Err(ErrorCode::StorageOther(
                "inverted index metadata cache value has trailing bytes".to_string(),
            ));
        }
        let metadata = Self::new(metadata.object_size, Bytes::from(metadata.footer_bytes));
        metadata.validate()?;
        Ok(metadata)
    }
}

macro_rules! cached_index_bytes {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Debug)]
        pub struct $name {
            pub data: Bytes,
        }

        impl $name {
            pub fn new(data: Bytes) -> Self {
                Self { data }
            }
        }

        impl TryFrom<&$name> for Vec<u8> {
            type Error = ErrorCode;

            fn try_from(value: &$name) -> std::result::Result<Self, Self::Error> {
                if value.data.len() > PAGE_CACHE_DECODE_LIMIT {
                    return Err(ErrorCode::StorageOther(
                        "inverted index range cache value exceeds the maximum size",
                    ));
                }
                Ok(value.data.to_vec())
            }
        }

        impl TryFrom<Bytes> for $name {
            type Error = ErrorCode;

            fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
                if value.len() > PAGE_CACHE_DECODE_LIMIT {
                    return Err(ErrorCode::StorageOther(
                        "inverted index range cache value exceeds the maximum size",
                    ));
                }
                Ok(Self::new(value))
            }
        }
    };
}

cached_index_bytes!(
    InvertedIndexLookupBytes,
    "A complete small lookup component or one page of a large term dictionary."
);
cached_index_bytes!(
    InvertedIndexPayloadBytes,
    "One cached range page from a Tantivy payload component."
);

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::*;
    use crate::inverted_index::BundleOpenSlice;

    fn metadata_fixture() -> InvertedIndexMeta {
        let object = InvertedIndexBundleFooter::build(
            [("segment.idx", b"postings".as_slice())],
            BTreeMap::new(),
            BTreeMap::from([(PathBuf::from("segment.idx"), vec![BundleOpenSlice {
                range: 0..4,
                bytes: Arc::from(b"post".as_slice()),
            }])]),
            b"managed".to_vec(),
            b"meta".to_vec(),
        )
        .unwrap();
        let object_size = u64::try_from(object.len()).unwrap();
        let footer = InvertedIndexBundleFooter::open(&object).unwrap();
        let footer_start = usize::try_from(footer.footer_start).unwrap();
        InvertedIndexMeta::new(object_size, Bytes::copy_from_slice(&object[footer_start..]))
    }

    #[test]
    fn test_cache_key_namespace() {
        assert_eq!(inverted_index_meta_cache_key("index"), "index");
    }

    #[test]
    fn test_metadata_round_trip_contains_complete_footer() {
        let metadata = metadata_fixture();
        let encoded = Vec::<u8>::try_from(&metadata).unwrap();
        let decoded = InvertedIndexMeta::try_from(Bytes::from(encoded)).unwrap();

        assert_eq!(decoded.object_size, metadata.object_size);
        assert_eq!(decoded.footer_bytes, metadata.footer_bytes);
        assert!(
            InvertedIndexBundleFooter::open_footer_for_object(
                decoded.footer_bytes.as_ref(),
                decoded.object_size,
                None,
            )
            .is_ok()
        );
    }

    #[test]
    fn test_metadata_rejects_corrupt_footer() {
        let metadata = InvertedIndexMeta::new(7, Bytes::from_static(b"corrupt"));
        assert!(Vec::<u8>::try_from(&metadata).is_err());
    }

    #[test]
    fn test_range_cache_decode_is_bounded() {
        let oversized = Bytes::from(vec![0; PAGE_CACHE_DECODE_LIMIT + 1]);
        assert!(InvertedIndexLookupBytes::try_from(oversized.clone()).is_err());
        assert!(InvertedIndexPayloadBytes::try_from(oversized).is_err());
    }
}
