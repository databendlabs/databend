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

use crate::principal::UserIdentity;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrincipalIdentity {
    User(UserIdentity),
    Role(String),
}

impl PrincipalIdentity {
    pub fn user(name: String, host: String) -> Self {
        PrincipalIdentity::User(UserIdentity::new(name, host))
    }

    pub fn role(name: String) -> Self {
        PrincipalIdentity::Role(name)
    }
}

impl fmt::Display for PrincipalIdentity {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        match self {
            PrincipalIdentity::User(u) => write!(f, " USER {}", u.display()),
            PrincipalIdentity::Role(r) => write!(f, " ROLE '{r}'"),
        }
    }
}
