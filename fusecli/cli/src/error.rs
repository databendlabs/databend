// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io;

pub type Result<T> = std::result::Result<T, CliError>;

#[derive(thiserror::Error, Debug)]
pub enum CliError {
    #[error("Unknown error: {0}")]
    Unknown(String),

    #[error("IO error: {0}")]
    Io(io::Error),

    #[error("Script error: {0}")]
    Script(run_script::ScriptError),

    #[error("Http error: {0}")]
    Http(Box<ureq::Error>),
}

impl From<io::Error> for CliError {
    fn from(err: io::Error) -> CliError {
        CliError::Io(err)
    }
}

impl From<run_script::ScriptError> for CliError {
    fn from(err: run_script::ScriptError) -> CliError {
        CliError::Script(err)
    }
}

impl From<ureq::Error> for CliError {
    fn from(err: ureq::Error) -> CliError {
        CliError::Http(Box::new(err))
    }
}
