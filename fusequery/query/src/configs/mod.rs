// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod config_test;

pub mod config;
mod extractor_config;

pub use config::Config;
pub use extractor_config::ConfigExtractor;
