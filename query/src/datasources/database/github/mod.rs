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

pub mod database;
mod repo_comments_table;
mod repo_info_table;
mod repo_issues_table;
mod repo_prs_table;
mod util;

pub use database::GithubDatabase;
pub use database::GITHUB_REPO_COMMENTS_ENGINE;
pub use database::GITHUB_REPO_INFO_ENGINE;
pub use database::GITHUB_REPO_ISSUES_ENGINE;
pub use database::GITHUB_REPO_PRS_ENGINE;
pub use repo_comments_table::RepoCommentsTable;
pub use repo_info_table::RepoInfoTable;
pub use repo_issues_table::RepoIssuesTable;
pub use repo_prs_table::RepoPrsTable;
