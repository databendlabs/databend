// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::result;

use arrow::error::ArrowError;
use sqlparser::parser::ParserError;

pub type FuseQueryResult<T> = result::Result<T, FuseQueryError>;

#[derive(thiserror::Error, Debug)]
pub enum FuseQueryError {
    #[error("SQLParser Error: {0}")]
    SQLParse(#[from] ParserError),

    #[error("Error during plan: {0}")]
    Plan(String),

    #[error("Internal Error: {0}")]
    Internal(String),
}

impl From<ArrowError> for FuseQueryError {
    fn from(e: ArrowError) -> Self {
        FuseQueryError::Internal(e.to_string())
    }
}

impl From<std::num::ParseFloatError> for FuseQueryError {
    fn from(err: std::num::ParseFloatError) -> Self {
        FuseQueryError::Internal(err.to_string())
    }
}

impl From<std::num::ParseIntError> for FuseQueryError {
    fn from(err: std::num::ParseIntError) -> Self {
        FuseQueryError::Internal(err.to_string())
    }
}

impl From<std::io::Error> for FuseQueryError {
    fn from(err: std::io::Error) -> Self {
        FuseQueryError::Internal(err.to_string())
    }
}

impl From<std::fmt::Error> for FuseQueryError {
    fn from(err: std::fmt::Error) -> Self {
        FuseQueryError::Internal(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for FuseQueryError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        FuseQueryError::Internal(err.to_string())
    }
}

impl From<tokio::task::JoinError> for FuseQueryError {
    fn from(err: tokio::task::JoinError) -> Self {
        FuseQueryError::Internal(err.to_string())
    }
}

impl From<serde_json::Error> for FuseQueryError {
    fn from(err: serde_json::Error) -> Self {
        FuseQueryError::Internal(err.to_string())
    }
}

impl From<std::net::AddrParseError> for FuseQueryError {
    fn from(err: std::net::AddrParseError) -> Self {
        FuseQueryError::Internal(err.to_string())
    }
}

impl From<prost::EncodeError> for FuseQueryError {
    fn from(err: prost::EncodeError) -> Self {
        FuseQueryError::Internal(err.to_string())
    }
}

impl From<tonic::transport::Error> for FuseQueryError {
    fn from(err: tonic::transport::Error) -> Self {
        FuseQueryError::Internal(err.to_string())
    }
}
