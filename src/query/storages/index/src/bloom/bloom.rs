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

use std::hash::Hash;

use common_exception::Result;

pub trait Bloom: Sized {
    // Length of the bitmap index.
    fn len(&self) -> Result<usize>;

    /// Add key into the bitmap index.
    fn add_key<K: ?Sized + Hash>(&mut self, key: &K);

    /// Add several keys into the bitmap index.
    fn add_keys<K: Hash>(&mut self, keys: &[K]);

    /// Build the bitmap index.
    fn build(&mut self) -> Result<()>;

    /// Check the key is in the bitmap index.
    fn contains<K: ?Sized + Hash>(&self, key: &K) -> bool;

    /// Serialize the bitmap index to binary array.
    fn to_bytes(&self) -> Result<Vec<u8>>;

    /// Deserialize the binary array to bitmap index.
    fn from_bytes(buf: &[u8]) -> Result<(Self, usize)>;
}
