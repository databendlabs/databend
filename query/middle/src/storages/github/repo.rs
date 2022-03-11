//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::HashMap;
use std::convert::TryFrom;

use common_exception::ErrorCode;
use common_exception::Result;

pub const OWNER: &str = "owner";
pub const REPO: &str = "repo";
pub const TOKEN: &str = "token";
pub const TABLE_TYPE: &str = "table_type";

#[derive(Clone)]
pub struct RepoTableOptions {
    pub repo: String,
    pub owner: String,
    pub token: String,
    pub table_type: String,
}

impl From<RepoTableOptions> for HashMap<String, String> {
    fn from(options: RepoTableOptions) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert(OWNER.to_string(), options.owner);
        map.insert(REPO.to_string(), options.repo);
        map.insert(TOKEN.to_string(), options.token);
        map.insert(TABLE_TYPE.to_string(), options.table_type);
        map
    }
}

impl TryFrom<&HashMap<String, String>> for RepoTableOptions {
    type Error = ErrorCode;
    fn try_from(options: &HashMap<String, String>) -> Result<RepoTableOptions> {
        let owner = options
            .get(OWNER)
            .ok_or_else(|| ErrorCode::UnexpectedError("Github engine table missing owner key"))?
            .clone();
        let repo = options
            .get(REPO)
            .ok_or_else(|| ErrorCode::UnexpectedError("Github engine table missing repo key"))?
            .clone();
        let token = options
            .get(TOKEN)
            .ok_or_else(|| ErrorCode::UnexpectedError("Github engine table missing token key"))?
            .clone();
        let table_type = options
            .get(TABLE_TYPE)
            .ok_or_else(|| {
                ErrorCode::UnexpectedError("Github engine table missing table_type key")
            })?
            .clone();
        let options = RepoTableOptions {
            repo,
            owner,
            token,
            table_type,
        };
        Ok(options)
    }
}
