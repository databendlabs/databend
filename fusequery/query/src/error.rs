// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::result;

use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use snafu::{Backtrace, Snafu};
use snafu::{ErrorCompat, IntoError};
use sqlparser::parser::ParserError;

pub type FuseQueryResult<T> = result::Result<T, FuseQueryError>;

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum FuseQueryError {
    #[snafu(display("SqlParser Error"))]
    SqlParse {
        source: ParserError,
        backtrace: Backtrace,
    },

    #[snafu(display("Error during plan: {}", message))]
    Plan {
        message: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Internal Error: {}", message))]
    Internal {
        message: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Arrow Error"))]
    Arrow {
        source: ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Parquet Error"))]
    Parquet {
        source: ParquetError,
        backtrace: Backtrace,
    },

    #[snafu(display("Flight Error: {}", status))]
    Flight {
        status: tonic::Status,
        backtrace: Backtrace,
    },
}

impl FuseQueryError {
    pub fn build_internal_error(message: String) -> FuseQueryError {
        Internal { message }.build()
    }

    pub fn build_plan_error(message: String) -> FuseQueryError {
        Plan { message }.build()
    }

    pub fn build_flight_error(status: tonic::Status) -> FuseQueryError {
        Flight { status }.build()
    }
}

// Internal convert.
impl From<common_datavalues::DataValueError> for FuseQueryError {
    fn from(err: common_datavalues::DataValueError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<common_datablocks::DataBlockError> for FuseQueryError {
    fn from(err: common_datablocks::DataBlockError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<common_functions::FunctionError> for FuseQueryError {
    fn from(err: common_functions::FunctionError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<common_planners::PlannerError> for FuseQueryError {
    fn from(err: common_planners::PlannerError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<ArrowError> for FuseQueryError {
    fn from(e: ArrowError) -> Self {
        Arrow.into_error(e)
    }
}

impl From<ParquetError> for FuseQueryError {
    fn from(e: ParquetError) -> Self {
        Parquet.into_error(e)
    }
}

impl From<ParserError> for FuseQueryError {
    fn from(e: ParserError) -> Self {
        SqlParse.into_error(e)
    }
}

impl From<String> for FuseQueryError {
    fn from(message: String) -> Self {
        Internal { message }.build()
    }
}

impl From<std::num::ParseFloatError> for FuseQueryError {
    fn from(err: std::num::ParseFloatError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<std::num::ParseIntError> for FuseQueryError {
    fn from(err: std::num::ParseIntError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<std::io::Error> for FuseQueryError {
    fn from(err: std::io::Error) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<std::fmt::Error> for FuseQueryError {
    fn from(err: std::fmt::Error) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl<T> From<std::sync::PoisonError<T>> for FuseQueryError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<tokio::task::JoinError> for FuseQueryError {
    fn from(err: tokio::task::JoinError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<std::net::AddrParseError> for FuseQueryError {
    fn from(err: std::net::AddrParseError) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

impl From<Box<dyn snafu::Error>> for FuseQueryError {
    fn from(err: Box<dyn snafu::Error>) -> Self {
        Internal {
            message: err.to_string(),
        }
        .build()
    }
}

// a better eprintln function for error
pub fn report<E: 'static>(err: &E)
where
    E: std::error::Error,
    E: ErrorCompat,
{
    eprintln!("[ERROR] {}", err);
    if let Some(source) = err.source() {
        eprintln!();
        eprintln!("Caused by:");
        for (i, e) in std::iter::successors(Some(source), |e| e.source()).enumerate() {
            eprintln!("   {}: {}", i, e);
        }
    }

    let env_backtrace = std::env::var("RUST_BACKTRACE").unwrap_or_default();
    let env_lib_backtrace = std::env::var("RUST_LIB_BACKTRACE").unwrap_or_default();
    if env_lib_backtrace == "1" || (env_backtrace == "1" && env_lib_backtrace != "0") {
        if let Some(backtrace) = ErrorCompat::backtrace(&err) {
            eprintln!();
            eprintln!("Backtrace:");
            eprintln!("{}", backtrace);
        }
    }
}
