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

use std::fmt::Debug;
use std::iter::TrustedLen;
use std::ptr::NonNull;

use databend_common_column::buffer::Buffer;
use databend_common_exception::Result;
use databend_common_hashtable::DictionaryKeys;
use databend_common_hashtable::FastHash;
use either::Either;
use ethnum::u256;

use crate::types::binary::BinaryColumn;
use crate::types::decimal::Decimal;
use crate::types::i256;
use crate::types::DataType;
use crate::types::NumberDataType;
use crate::types::StringColumn;
use crate::Column;
use crate::HashMethodKeysU128;
use crate::HashMethodKeysU16;
use crate::HashMethodKeysU256;
use crate::HashMethodKeysU32;
use crate::HashMethodKeysU64;
use crate::HashMethodKeysU8;
use crate::HashMethodSerializer;
use crate::HashMethodSingleBinary;
use crate::InputColumns;

#[derive(Debug, Clone)]
pub enum KeysState {
    Column(Column),
    U128(Buffer<u128>),
    U256(Buffer<u256>),
    Dictionary {
        columns: Vec<Either<StringColumn, BinaryColumn>>,
        keys_point: Vec<NonNull<[u8]>>,
        dictionaries: Vec<DictionaryKeys>,
    },
}

unsafe impl Send for KeysState {}

unsafe impl Sync for KeysState {}

pub trait KeyAccessor {
    type Key: ?Sized;

    /// # Safety
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    unsafe fn key_unchecked(&self, index: usize) -> &Self::Key;
}

pub trait HashMethod: Clone + Sync + Send + 'static {
    type HashKey: ?Sized + Eq + FastHash + Debug;

    type HashKeyIter<'a>: Iterator<Item = &'a Self::HashKey> + TrustedLen
    where Self: 'a;

    fn name(&self) -> String;

    fn build_keys_state(&self, group_columns: InputColumns, rows: usize) -> Result<KeysState>;

    fn build_keys_iter<'a>(&self, keys_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>>;

    fn build_keys_accessor(
        &self,
        keys_state: KeysState,
    ) -> Result<Box<dyn KeyAccessor<Key = Self::HashKey>>>;

    fn build_keys_hashes(&self, keys_state: &KeysState, hashes: &mut Vec<u64>);
}

/// These methods are `generic` method to generate hash key,
/// that is the 'numeric' or 'binary` representation of each column value as hash key.
#[derive(Clone, Debug)]
pub enum HashMethodKind {
    Serializer(HashMethodSerializer),
    SingleBinary(HashMethodSingleBinary),
    KeysU8(HashMethodKeysU8),
    KeysU16(HashMethodKeysU16),
    KeysU32(HashMethodKeysU32),
    KeysU64(HashMethodKeysU64),
    KeysU128(HashMethodKeysU128),
    KeysU256(HashMethodKeysU256),
}

#[macro_export]
macro_rules! with_hash_method {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [Serializer, SingleBinary, KeysU8, KeysU16,
            KeysU32, KeysU64, KeysU128, KeysU256],
            $($tail)*
        }
    }
}

#[macro_export]
macro_rules! with_join_hash_method {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [Serializer, SingleBinary, KeysU8, KeysU16,
            KeysU32, KeysU64, KeysU128, KeysU256],
            $($tail)*
        }
    }
}

impl HashMethodKind {
    pub fn name(&self) -> String {
        with_hash_method!(|T| match self {
            HashMethodKind::T(v) => v.name(),
        })
    }

    pub fn data_type(&self) -> DataType {
        match self {
            HashMethodKind::Serializer(_) => DataType::Binary,
            HashMethodKind::SingleBinary(_) => DataType::Binary,
            HashMethodKind::KeysU8(_) => DataType::Number(NumberDataType::UInt8),
            HashMethodKind::KeysU16(_) => DataType::Number(NumberDataType::UInt16),
            HashMethodKind::KeysU32(_) => DataType::Number(NumberDataType::UInt32),
            HashMethodKind::KeysU64(_) => DataType::Number(NumberDataType::UInt64),
            HashMethodKind::KeysU128(_) => DataType::Decimal(i128::default_decimal_size()),
            HashMethodKind::KeysU256(_) => DataType::Decimal(i256::default_decimal_size()),
        }
    }
}
