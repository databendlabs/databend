// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod siphash_test;

mod hash;
mod siphash;

pub use hash::HashesFunction;
