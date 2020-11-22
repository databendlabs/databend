// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::result;

use arrow::error::ArrowError;
use sqlparser::parser::ParserError;

pub type FuseQueryResult<T> = result::Result<T, FuseQueryError>;

#[derive(thiserror::Error, Debug)]
pub enum FuseQueryError {
    #[error("SQLParser Error: {0}")]
    SQLParse(#[from] ParserError),

    #[error("Internal Error: {0}")]
    Internal(String),

    #[error("Unsupported Error: {0}")]
    Unsupported(String),
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
