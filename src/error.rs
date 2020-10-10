// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::fmt;
use std::result;

use arrow::error::ArrowError;
use sqlparser::parser::ParserError;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    SQLParse(ParserError),
    Internal(String),
    Unsupported(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Internal(ref msg) => write!(f, "Internal Error: {:?}", msg),
            Error::SQLParse(ref msg) => write!(f, "SQLParser Error: {:?}", msg),
            Error::Unsupported(ref msg) => write!(f, "Unsupported Error: {:?}", msg),
        }
    }
}

impl From<ParserError> for Error {
    fn from(e: ParserError) -> Self {
        Error::SQLParse(e)
    }
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
