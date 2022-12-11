//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::fmt::Display;
use std::hash::Hash;

use anyerror::AnyError;
use cbordata::Cbor;
use cbordata::FromCbor;
use cbordata::IntoCbor;
use common_datavalues::prelude::TypeID;
use common_datavalues::remove_nullable;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_exception::ErrorCode;
use xorfilter::Xor8;

use crate::filters::Filter;
use crate::filters::FilterBuilder;
use crate::SupportedType;

/// A builder that builds a xor8 filter.
///
/// When a filter is built, the source key set should be updated(by calling `add_keys()`), although Xor8 itself allows.
pub struct Xor8Builder {
    builder: xorfilter::Xor8Builder,
}

pub struct Xor8Filter {
    filter: Xor8,
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

    fn build(mut self) -> Result<Self::Filter, Self::Error> {
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

impl SupportedType for Xor8Filter {
    fn is_supported_type(data_type: &DataTypeImpl) -> bool {
        // Bloom index only enabled for String and Integral types for now
        let inner_type = remove_nullable(data_type);
        let data_type_id = inner_type.data_type_id();
        matches!(
            data_type_id,
            TypeID::String
                | TypeID::UInt8
                | TypeID::UInt16
                | TypeID::UInt32
                | TypeID::UInt64
                | TypeID::Int8
                | TypeID::Int16
                | TypeID::Int32
                | TypeID::Int64
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
