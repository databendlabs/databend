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

use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct RepoMetaSimple {
    pub name: String,
    full_name: String,
}

#[derive(Deserialize, Debug)]
pub struct License {
    pub key: String,
    name: String,
}

#[derive(Deserialize, Debug)]
pub struct RepoDetail {
    pub name: String,
    pub language: Option<String>,
    pub license: Option<License>,
    pub stargazers_count: u32,
    pub forks_count: u32,
    pub watchers_count: u32,
    pub open_issues_count: u32,
    pub subscribers_count: u32,
    pub created_at: String,
    pub updated_at: String,
}
