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

use common_base::tokio;
#[allow(unused)]
use common_exception::Result;

#[allow(unused)]
use super::data_accessor;

#[allow(unused)]
const OWNER: &str = "datafuselabs";
#[allow(unused)]
const REPO: &str = "databend";

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_owner_repositories() -> Result<()> {
    let repo_metas = data_accessor::get_owner_repositories(OWNER).await?;
    assert!(repo_metas.len() > 5);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_repository_detail() -> Result<()> {
    let repo_detail = data_accessor::get_repository_detail(OWNER, REPO).await?;
    assert!(repo_detail.stargazers_count > 2000);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_owner_repositories_details() -> Result<()> {
    let repo_details = data_accessor::get_owner_repositories_details(OWNER).await?;
    assert!(repo_details.len() > 5);
    Ok(())
}
