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
