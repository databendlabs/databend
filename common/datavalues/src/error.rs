// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;

use snafu::{Backtrace, IntoError, Snafu};

pub type DataValueResult<T, E = DataValueError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum DataValueError {
    #[snafu(display("DataValue Internal Error: {}", message))]
    Internal {
        message: String,
        backtrace: Backtrace,
    },

    #[snafu(display("DataValue Arrow Error"))]
    Arrow {
        source: arrow::error::ArrowError,
        backtrace: Backtrace,
    },
}

impl DataValueError {
    pub fn build_internal_error(message: String) -> DataValueError {
        Internal { message }.build()
    }
}

impl From<std::num::ParseFloatError> for DataValueError {
    fn from(err: std::num::ParseFloatError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<arrow::error::ArrowError> for DataValueError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Arrow.into_error(e)
    }
}
