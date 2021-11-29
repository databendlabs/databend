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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableInfo;
use octocrab::Octocrab;
use reqwest::header::AUTHORIZATION;

use crate::storages::github::OWNER;
use crate::storages::github::REPO;

pub fn create_github_client() -> Result<Octocrab> {
    let github_token_res = std::env::var("GITHUB_TOKEN");
    if let Err(e) = github_token_res {
        return Err(ErrorCode::SecretKeyNotSet(format!(
            "Github Access Token is not set, see how to set at https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token, {}",
            e.to_string()
        )));
    }
    let github_token = github_token_res.unwrap();
    octocrab::OctocrabBuilder::new()
        .add_header(AUTHORIZATION, format!("token {}", github_token))
        .build()
        .map_err(|err| {
            ErrorCode::UnexpectedError(format!(
                "Error Occured when creating octorab client, err: {}",
                err.to_string()
            ))
        })
}

pub fn get_own_repo_from_table_info(table_info: &TableInfo) -> Result<(String, String)> {
    let owner = table_info
        .meta
        .options
        .get(OWNER)
        .ok_or_else(|| ErrorCode::UnexpectedError("Github table need owner in its mata"))?
        .clone();
    let repo = table_info
        .meta
        .options
        .get(REPO)
        .ok_or_else(|| ErrorCode::UnexpectedError("Github table need repo in its mata"))?
        .clone();
    Ok((owner, repo))
}
