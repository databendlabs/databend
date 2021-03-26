// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;

use snafu::{Backtrace, IntoError, Snafu};

pub type FunctionResult<T, E = FunctionError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum FunctionError {
    #[snafu(display("Function Internal Error: {}", message))]
    Internal {
        message: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Arrow Error"))]
    Arrow {
        source: arrow::error::ArrowError,
        backtrace: Backtrace,
    },
}

impl FunctionError {
    pub fn build_internal_error(message: String) -> FunctionError {
        Internal { message }.build()
    }
}

// Internal convert.
impl From<common_datavalues::DataValueError> for FunctionError {
    fn from(err: common_datavalues::DataValueError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<common_datablocks::DataBlockError> for FunctionError {
    fn from(err: common_datablocks::DataBlockError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<arrow::error::ArrowError> for FunctionError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Arrow.into_error(e)
    }
}

impl<T> From<std::sync::PoisonError<T>> for FunctionError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}
