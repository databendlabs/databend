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

use common_exception::Result;
use reqwest::header::USER_AGENT;
use reqwest::Client;

use super::items::RepoDetail;
use super::items::RepoMetaSimple;

const USER_AGENT_VAL: &str = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36";

pub async fn get_owner_repositories_details(owner: &str) -> Result<Vec<RepoDetail>> {
    let repo_metas = get_owner_repositories(owner).await?;

    let mut iter = repo_metas.iter();
    let mut resp = Vec::new();
    for repo in &mut iter {
        let detail = get_repository_detail(owner, &repo.name).await?;
        resp.push(detail);
    }
    Ok(resp)
}

pub async fn get_owner_repositories(owner: &str) -> Result<Vec<RepoMetaSimple>> {
    let url = format!("http://api.github.com/users/{}/repos", owner);
    let client = Client::new();
    let resp = client
        .get(&url)
        .header(USER_AGENT, USER_AGENT_VAL)
        .send()
        .await?;
    let resp: Vec<RepoMetaSimple> = resp.json().await?;
    Ok(resp)
}

pub async fn get_repository_detail(owner: &str, repo: &str) -> Result<RepoDetail> {
    let url = format!("http://api.github.com/repos/{}/{}", owner, repo);
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header(USER_AGENT, USER_AGENT_VAL)
        .send()
        .await?;
    let detail: RepoDetail = resp.json().await?;
    Ok(detail)
}
