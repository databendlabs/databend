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

use std::collections::HashSet;
use std::fmt::Display;
use std::hash::Hash;
use std::hash::Hasher;

use anyerror::AnyError;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::DataType;
use xorf::BinaryFuse32;
use xorf::BinaryFuse32Ref;
use xorf::DmaSerializable;
use xorf::Filter as XorFilter;
use xorf::FilterRef as XorFilterRef;

use crate::Index;
use crate::filters::Filter;
use crate::filters::FilterBuilder;

#[derive(Default)]
pub struct BinaryFuse32Builder {
    digests: HashSet<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BinaryFuse32Filter {
    pub descriptor: [u8; BinaryFuse32::DESCRIPTOR_LEN],
    pub fingerprints: Vec<u32>,
    pub len: usize,
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[error("{msg}; cause: {cause}")]
pub struct BinaryFuse32CodecError {
    msg: String,
    #[source]
    cause: AnyError,
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[error("fail to build binary fuse 32 filter; cause: {cause}")]
pub struct BinaryFuse32BuildingError {
    #[source]
    cause: AnyError,
}

impl BinaryFuse32Builder {
    pub fn create() -> Self {
        Self::default()
    }
}

impl TryFrom<&BinaryFuse32Filter> for Vec<u8> {
    type Error = ErrorCode;

    fn try_from(value: &BinaryFuse32Filter) -> std::result::Result<Self, Self::Error> {
        value.to_bytes().map_err(ErrorCode::from)
    }
}

impl TryFrom<Bytes> for BinaryFuse32Filter {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        BinaryFuse32Filter::from_bytes(value.as_ref())
            .map(|(v, len)| {
                assert_eq!(len, value.len());
                v
            })
            .map_err(ErrorCode::from)
    }
}

impl FilterBuilder for BinaryFuse32Builder {
    type Filter = BinaryFuse32Filter;
    type Error = BinaryFuse32BuildingError;

    fn add_key<K: Hash>(&mut self, key: &K) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        self.digests.insert(hasher.finish());
    }

    fn add_keys<K: Hash>(&mut self, keys: &[K]) {
        for key in keys {
            self.add_key(key);
        }
    }

    fn add_digests<'i, I: IntoIterator<Item = &'i u64>>(&mut self, digests: I) {
        self.digests.extend(digests.into_iter().copied());
    }

    fn build(&mut self) -> Result<Self::Filter, Self::Error> {
        let digests = std::mem::take(&mut self.digests);
        let len = digests.len();
        let digests = digests.into_iter().collect::<Vec<_>>();
        let filter =
            BinaryFuse32::try_from(digests.as_slice()).map_err(BinaryFuse32BuildingError::new)?;

        let mut descriptor = [0; BinaryFuse32::DESCRIPTOR_LEN];
        filter.dma_copy_descriptor_to(&mut descriptor);

        Ok(BinaryFuse32Filter {
            descriptor,
            fingerprints: filter.fingerprints.into_vec(),
            len,
        })
    }
}

impl BinaryFuse32Filter {
    pub fn mem_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.fingerprints.len() * std::mem::size_of::<u32>()
    }
}

impl Filter for BinaryFuse32Filter {
    type CodecError = BinaryFuse32CodecError;

    fn len(&self) -> Option<usize> {
        Some(self.len)
    }

    fn contains<K: ?Sized + Hash>(&self, key: &K) -> bool {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        self.contains_digest(hasher.finish())
    }

    fn contains_digest(&self, digest: u64) -> bool {
        BinaryFuse32Ref::from_dma(&self.descriptor, bytemuck::cast_slice(&self.fingerprints))
            .contains(&digest)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Self::CodecError> {
        let mut bytes = Vec::with_capacity(
            std::mem::size_of::<u64>()
                + self.descriptor.len()
                + std::mem::size_of::<u64>()
                + self.fingerprints.len() * std::mem::size_of::<u32>(),
        );
        bytes.extend_from_slice(&(self.len as u64).to_le_bytes());
        bytes.extend_from_slice(&self.descriptor);
        bytes.extend_from_slice(&(self.fingerprints.len() as u64).to_le_bytes());
        bytes.extend_from_slice(bytemuck::cast_slice(&self.fingerprints));
        Ok(bytes)
    }

    fn from_bytes(buf: &[u8]) -> Result<(Self, usize), Self::CodecError> {
        let mut offset = 0;

        fn eof() -> BinaryFuse32CodecError {
            BinaryFuse32CodecError::new(
                "unexpected end of data",
                &std::io::Error::other("unexpected end of data"),
            )
        }

        fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64, BinaryFuse32CodecError> {
            if *offset + 8 > data.len() {
                return Err(eof());
            }
            let value = u64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            Ok(value)
        }

        let len = read_u64(buf, &mut offset)? as usize;

        let mut descriptor = [0; BinaryFuse32::DESCRIPTOR_LEN];
        if offset + BinaryFuse32::DESCRIPTOR_LEN > buf.len() {
            return Err(eof());
        }
        descriptor.copy_from_slice(&buf[offset..offset + BinaryFuse32::DESCRIPTOR_LEN]);
        offset += BinaryFuse32::DESCRIPTOR_LEN;

        let fingerprint_len = read_u64(buf, &mut offset)? as usize;
        let byte_len = fingerprint_len * std::mem::size_of::<u32>();
        if offset + byte_len > buf.len() {
            return Err(eof());
        }
        let fingerprints = buf[offset..offset + byte_len]
            .chunks_exact(std::mem::size_of::<u32>())
            .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        offset += byte_len;

        Ok((
            Self {
                descriptor,
                fingerprints,
                len,
            },
            offset,
        ))
    }
}

impl Index for BinaryFuse32Filter {
    fn supported_type(data_type: &DataType) -> bool {
        crate::filters::Xor8Filter::supported_type(data_type)
    }
}

impl BinaryFuse32CodecError {
    pub fn new(msg: impl Display, cause: &(impl std::error::Error + 'static)) -> Self {
        Self {
            msg: msg.to_string(),
            cause: AnyError::new(cause),
        }
    }
}

impl BinaryFuse32BuildingError {
    pub fn new(cause: impl Display) -> Self {
        Self {
            cause: AnyError::error(cause.to_string()),
        }
    }
}

impl From<BinaryFuse32CodecError> for ErrorCode {
    fn from(value: BinaryFuse32CodecError) -> Self {
        ErrorCode::Internal(value.to_string())
    }
}

impl From<BinaryFuse32BuildingError> for ErrorCode {
    fn from(value: BinaryFuse32BuildingError) -> Self {
        ErrorCode::Internal(value.to_string())
    }
}
