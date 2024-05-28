// Copyright 2021 Datafuse Labs
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

use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Duration;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::quote::AtString;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum PresignAction {
    Download,
    Upload,
}

impl Default for PresignAction {
    fn default() -> Self {
        Self::Download
    }
}

impl Display for PresignAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            PresignAction::Download => write!(f, "DOWNLOAD"),
            PresignAction::Upload => write!(f, "UPLOAD"),
        }
    }
}

/// TODO: we can support uri location in the future.
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum PresignLocation {
    StageLocation(String),
}

impl Display for PresignLocation {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            PresignLocation::StageLocation(v) => write!(f, "{}", AtString(v)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct PresignStmt {
    pub action: PresignAction,
    pub location: PresignLocation,
    #[drive(skip)]
    pub expire: Duration,
    pub content_type: Option<String>,
}

impl Display for PresignStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "PRESIGN {} {} EXPIRE = {}",
            self.action,
            self.location,
            self.expire.as_secs()
        )?;
        if let Some(content_type) = &self.content_type {
            write!(f, " CONTENT_TYPE = '{}'", content_type)?;
        }
        Ok(())
    }
}

impl PresignStmt {
    pub fn apply_option(&mut self, opt: PresignOption) {
        match opt {
            PresignOption::Expire(v) => self.expire = Duration::from_secs(v),
            PresignOption::ContentType(v) => self.content_type = Some(v),
        }
    }
}

pub enum PresignOption {
    ContentType(String),
    Expire(u64),
}
