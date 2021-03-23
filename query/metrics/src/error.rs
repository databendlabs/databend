// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;

use snafu::{Backtrace, Snafu};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("Internal Error: {}", message))]
    Internal {
        message: String,
        backtrace: Backtrace,
    },
}

impl Error {
    pub fn build_internal_error(message: String) -> Error {
        Internal { message }.build()
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(err: std::net::AddrParseError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}
