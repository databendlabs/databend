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

use std::hash::BuildHasher;
use std::hash::Hash;

use cbordata::Cbor;
use cbordata::FromCbor;
use cbordata::IntoCbor;
use common_exception::ErrorCode;
use common_exception::Result;
use xorfilter::Xor8;

use crate::bloom::Bloom;

impl<H> Bloom for Xor8<H>
where H: Clone + BuildHasher + Into<Vec<u8>> + From<Vec<u8>>
{
    fn len(&self) -> Result<usize> {
        match self.len() {
            Some(n) => Ok(n),
            None => Err(ErrorCode::UnImplement("Xor8 does not implement len()")),
        }
    }

    fn is_empty(&self) -> bool {
        match self.len() {
            Some(n) => n == 0,
            None => true,
        }
    }

    #[inline]
    fn add_key<K: ?Sized + Hash>(&mut self, key: &K) {
        self.insert(key)
    }

    #[inline]
    fn add_keys<K: Hash>(&mut self, keys: &[K]) {
        self.populate(keys)
    }

    #[inline]
    fn build(&mut self) -> Result<()> {
        self.build()
            .map_err(|e| ErrorCode::UnexpectedError(format!("Xor8.build error:{:?}", e)))
    }

    #[inline]
    fn contains<K: ?Sized + Hash>(&self, key: &K) -> bool {
        self.contains(key)
    }

    fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf: Vec<u8> = vec![];

        let cbor_val = self
            .clone()
            .into_cbor()
            .map_err(|e| ErrorCode::UnexpectedError(format!("Xor8.into_cbor error:{:}", e)))?;
        cbor_val
            .encode(&mut buf)
            .map_err(|e| ErrorCode::UnexpectedError(format!("Xor8.encode error:{:}", e)))?;

        Ok(buf)
    }

    fn from_bytes(mut buf: &[u8]) -> Result<(Self, usize)> {
        let (cbor_val, n) = Cbor::decode(&mut buf)
            .map_err(|e| ErrorCode::UnexpectedError(format!("Xor8.cbor.decode error:{:}", e)))?;

        let xor_value = Xor8::<H>::from_cbor(cbor_val)
            .map_err(|e| ErrorCode::UnexpectedError(format!("Xor8.from_cborerror:{:}", e)))?;
        Ok((xor_value, n))
    }
}
