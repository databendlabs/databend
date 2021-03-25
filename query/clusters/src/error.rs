// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;

use snafu::{Backtrace, Snafu};

pub type ClusterResult<T, E = ClusterError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum ClusterError {
    #[snafu(display("DataBlock Internal Error: {}", message))]
    Internal {
        message: String,
        backtrace: Backtrace,
    },
}

impl ClusterError {
    pub fn build_internal_error(message: String) -> ClusterError {
        Internal { message }.build()
    }
}

impl<T> From<std::sync::PoisonError<T>> for ClusterError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}
