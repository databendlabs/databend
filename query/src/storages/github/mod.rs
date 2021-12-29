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

mod github_client;
mod github_table;
mod repo;
mod repo_comments_table;
mod repo_info_table;
mod repo_issues_table;
mod repo_prs_table;

pub use github_client::create_github_client;
pub use github_table::GithubDataGetter;
pub use github_table::GithubTable;
pub use github_table::GithubTableType;
pub use repo::RepoTableOptions;
pub use repo_comments_table::RepoCommentsTable;
pub use repo_info_table::RepoInfoTable;
pub use repo_issues_table::RepoIssuesTable;
pub use repo_prs_table::RepoPRsTable;
