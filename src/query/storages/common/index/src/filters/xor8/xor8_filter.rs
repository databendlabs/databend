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

use std::fmt::Display;
use std::hash::Hash;

use anyerror::AnyError;
use cbordata::Cbor;
use cbordata::FromCbor;
use cbordata::IntoCbor;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::DataType;
use xorfilter::Xor8;

use crate::filters::Filter;
use crate::filters::FilterBuilder;
use crate::Index;

/// A builder that builds a xor8 filter.
///
/// When a filter is built, the source key set should be updated(by calling `add_keys()`), although Xor8 itself allows.
pub struct Xor8Builder {
    builder: xorfilter::Xor8Builder,
}

pub struct Xor8Filter {
    pub filter: Xor8,
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[error("{msg}; cause: {cause}")]
pub struct Xor8CodecError {
    msg: String,
    #[source]
    cause: AnyError,
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[error("fail to build xor8 filter; cause: {cause}")]
pub struct Xor8BuildingError {
    #[source]
    cause: AnyError,
}

impl FilterBuilder for Xor8Builder {
    type Filter = Xor8Filter;
    type Error = Xor8BuildingError;

    fn add_key<K: Hash>(&mut self, key: &K) {
        self.builder.insert(key)
    }

    fn add_keys<K: Hash>(&mut self, keys: &[K]) {
        self.builder.populate(keys)
    }

    fn add_digests<'i, I: IntoIterator<Item = &'i u64>>(&mut self, digests: I) {
        self.builder.populate_digests(digests)
    }

    fn build(&mut self) -> Result<Self::Filter, Self::Error> {
        let f = self
            .builder
            .build()
            .map_err(|e| Xor8BuildingError::new(&e))?;

        Ok(Xor8Filter { filter: f })
    }
}

impl Xor8Builder {
    pub fn create() -> Self {
        Xor8Builder {
            builder: xorfilter::Xor8Builder::default(),
        }
    }
}

impl Filter for Xor8Filter {
    type CodecError = Xor8CodecError;

    fn len(&self) -> Option<usize> {
        self.filter.len()
    }

    fn contains<K: ?Sized + Hash>(&self, key: &K) -> bool {
        self.filter.contains(key)
    }

    fn contains_digest(&self, digest: u64) -> bool {
        self.filter.contains_digest(digest)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Xor8CodecError> {
        let mut buf: Vec<u8> = vec![];
        let cbor_val = self
            .filter
            .clone()
            .into_cbor()
            .map_err(|e| Xor8CodecError::new("fail to build cbor", &e))?;
        cbor_val
            .encode(&mut buf)
            .map_err(|e| Xor8CodecError::new("fail to encode cbor", &e))?;

        Ok(buf)
    }

    fn from_bytes(mut buf: &[u8]) -> Result<(Self, usize), Xor8CodecError> {
        let (cbor_val, n) =
            Cbor::decode(&mut buf).map_err(|e| Xor8CodecError::new("fail to decode cbor", &e))?;

        let xor_value = Xor8::from_cbor(cbor_val)
            .map_err(|e| Xor8CodecError::new("fail to build filter from cbor", &e))?;
        Ok((Self { filter: xor_value }, n))
    }
}

impl Index for Xor8Filter {
    fn supported_type(data_type: &DataType) -> bool {
        let inner_type = data_type.remove_nullable();
        if let DataType::Map(box inner_ty) = inner_type {
            match inner_ty {
                DataType::Tuple(kv_tys) => {
                    return matches!(
                        kv_tys[1].remove_nullable(),
                        DataType::Number(_)
                            | DataType::String
                            | DataType::Variant
                            | DataType::Timestamp
                            | DataType::Date
                    );
                }
                _ => unreachable!(),
            };
        }
        matches!(
            inner_type,
            DataType::Number(_) | DataType::String | DataType::Timestamp | DataType::Date
        )
    }
}

impl Xor8CodecError {
    pub fn new(msg: impl Display, cause: &(impl std::error::Error + 'static)) -> Self {
        Self {
            msg: msg.to_string(),
            cause: AnyError::new(cause),
        }
    }
}

impl From<Xor8CodecError> for ErrorCode {
    fn from(e: Xor8CodecError) -> Self {
        ErrorCode::Internal(e.to_string())
    }
}

impl Xor8BuildingError {
    pub fn new(cause: &(impl std::error::Error + 'static)) -> Self {
        Self {
            cause: AnyError::new(cause),
        }
    }
}

impl From<Xor8BuildingError> for ErrorCode {
    fn from(e: Xor8BuildingError) -> Self {
        ErrorCode::Internal(e.to_string())
    }
}
