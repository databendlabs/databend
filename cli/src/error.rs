// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;

pub type Result<T> = std::result::Result<T, CliError>;

#[derive(thiserror::Error, Debug)]
pub enum CliError {
    #[error("Unknown error: {0}")]
    Unknown(String),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Script error: {0}")]
    Script(#[from] run_script::ScriptError),

    #[error("Http error: {0}")]
    Http(Box<ureq::Error>),

    #[error("Serde error: {0}")]
    Serde(Box<serde_json::Error>),

    #[error("Nix error: {0}")]
    Nix(Box<nix::Error>),

    #[error("Exited")]
    Exited,
}

impl From<ureq::Error> for CliError {
    fn from(err: ureq::Error) -> CliError {
        CliError::Http(Box::new(err))
    }
}

impl From<serde_json::Error> for CliError {
    fn from(err: serde_json::Error) -> CliError {
        CliError::Serde(Box::new(err))
    }
}

impl From<nix::Error> for CliError {
    fn from(err: nix::Error) -> CliError {
        CliError::Nix(Box::new(err))
    }
}
