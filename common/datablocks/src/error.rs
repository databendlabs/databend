// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;

use snafu::{Backtrace, IntoError, Snafu};

pub type DataBlockResult<T, E = DataBlockError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum DataBlockError {
    #[snafu(display("DataBlock Internal Error: {}", message))]
    Internal {
        message: String,
        backtrace: Backtrace,
    },

    #[snafu(display("DataBlock Arrow Error"))]
    Arrow {
        source: arrow::error::ArrowError,
        backtrace: Backtrace,
    },
}

impl DataBlockError {
    pub fn build_internal_error(message: String) -> DataBlockError {
        Internal { message }.build()
    }
}

impl From<arrow::error::ArrowError> for DataBlockError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Arrow.into_error(e)
    }
}
