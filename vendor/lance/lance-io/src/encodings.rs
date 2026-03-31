// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Data encodings
//!

use arrow_array::{Array, ArrayRef, UInt32Array};
use async_trait::async_trait;

pub mod binary;
pub mod dictionary;
pub mod plain;

use crate::ReadBatchParams;
use lance_core::Result;

/// Encoder - Write an arrow array to the file.
#[async_trait]
pub trait Encoder {
    /// Write an slice of Arrays, and returns the file offset of the beginning of the batch.
    async fn encode(&mut self, array: &[&dyn Array]) -> Result<usize>;
}

/// Decoder - Read Arrow Data.
#[async_trait]
pub trait Decoder: Send + AsyncIndex<ReadBatchParams> {
    async fn decode(&self) -> Result<ArrayRef>;

    /// Take by indices.
    async fn take(&self, indices: &UInt32Array) -> Result<ArrayRef>;
}

#[async_trait]
pub trait AsyncIndex<IndexType> {
    type Output: Send + Sync;

    async fn get(&self, index: IndexType) -> Self::Output;
}
