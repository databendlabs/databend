// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::result;
use thiserror::Error;

use arrow::error::ArrowError;
use sqlparser::parser::ParserError;

pub type Result<T> = result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("SQLParser Error: {0}")]
    SQLParse(#[from] ParserError),

    #[error("Internal Error: {0}")]
    Internal(String),

    #[error("Unsupported Error: {0}")]
    Unsupported(String),
}

impl From<ArrowError> for Error {
    fn from(e: ArrowError) -> Self {
        Error::Internal(e.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(err: std::num::ParseFloatError) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::Internal(err.to_string())
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for Error {
    fn from(err: std::sync::mpsc::SendError<T>) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<std::sync::mpsc::RecvError> for Error {
    fn from(err: std::sync::mpsc::RecvError) -> Self {
        Error::Internal(err.to_string())
    }
}
