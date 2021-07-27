// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

//! This crate defines data types used in meta data storage service.

pub use errors::SeqError;
pub use match_seq::MatchSeq;
pub use match_seq::MatchSeqExt;

mod errors;
mod match_seq;

#[cfg(test)]
mod match_seq_test;

/// Value with a corresponding sequence number
pub type SeqValue<T = Vec<u8>> = (u64, T);

pub type MetaVersion = u64;
pub type MetaId = u64;
