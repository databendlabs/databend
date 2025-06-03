// Copyright Qdrant
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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

pub trait EncodedStorage {
    fn get_vector_data(&self, index: usize, vector_size: usize) -> &[u8];

    fn from_slice(slice: &[u8], quantized_vector_size: usize, vectors_count: usize) -> Result<Self>
    where Self: Sized;

    fn to_vec(&self) -> Result<Vec<u8>>;
}

pub trait EncodedStorageBuilder<TStorage: EncodedStorage> {
    fn build(self) -> TStorage;

    fn push_vector_data(&mut self, other: &[u8]);
}

impl EncodedStorage for Vec<u8> {
    fn get_vector_data(&self, index: usize, vector_size: usize) -> &[u8] {
        &self[vector_size * index..vector_size * (index + 1)]
    }

    fn from_slice(
        slice: &[u8],
        quantized_vector_size: usize,
        vectors_count: usize,
    ) -> Result<Self> {
        let expected_size = quantized_vector_size * vectors_count;
        if slice.len() == expected_size {
            Ok(slice.to_vec())
        } else {
            Err(ErrorCode::Internal(format!(
                "Loaded storage size {} is not equal to expected size {expected_size}",
                slice.len()
            )))
        }
    }

    fn to_vec(&self) -> Result<Vec<u8>> {
        Ok(self.clone())
    }
}

impl EncodedStorageBuilder<Vec<u8>> for Vec<u8> {
    fn build(self) -> Vec<u8> {
        self
    }

    fn push_vector_data(&mut self, other: &[u8]) {
        self.extend_from_slice(other);
    }
}
