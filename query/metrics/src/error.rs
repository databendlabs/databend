// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;

use snafu::{Backtrace, Snafu};

pub type MetricResult<T, E = MetricError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum MetricError {
    #[snafu(display("Metric Internal Error: {}", message))]
    Internal {
        message: String,
        backtrace: Backtrace,
    },
}

impl MetricError {
    pub fn build_internal_error(message: String) -> MetricError {
        Internal { message }.build()
    }
}

impl From<std::net::AddrParseError> for MetricError {
    fn from(err: std::net::AddrParseError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}
