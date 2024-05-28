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

use std::fmt;

use crate::background::BackgroundJobIdent;
use crate::KeyWithTenant;

/// Same as [`BackgroundJobIdent`] but provide serde support for use in a record value.
/// [`BackgroundJobIdent`] is a kvapi::Key that does not need to be `serde`
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct BackgroundTaskCreator {
    /// The user this job belongs to
    pub tenant: String,

    pub name: String,
}

impl BackgroundTaskCreator {
    pub fn new(tenant: impl ToString, name: impl ToString) -> Self {
        Self {
            tenant: tenant.to_string(),
            name: name.to_string(),
        }
    }
}

impl fmt::Display for BackgroundTaskCreator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.tenant, self.name)
    }
}

impl From<BackgroundJobIdent> for BackgroundTaskCreator {
    fn from(ident: BackgroundJobIdent) -> Self {
        Self::new(ident.tenant_name(), ident.name())
    }
}
