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

use databend_meta_types::MatchSeq;

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateOption {
    Create,
    CreateIfNotExists,
    CreateOrReplace,
}

impl From<databend_common_ast::ast::CreateOption> for CreateOption {
    fn from(create_option: databend_common_ast::ast::CreateOption) -> Self {
        match create_option {
            databend_common_ast::ast::CreateOption::Create => CreateOption::Create,
            databend_common_ast::ast::CreateOption::CreateIfNotExists => {
                CreateOption::CreateIfNotExists
            }
            databend_common_ast::ast::CreateOption::CreateOrReplace => {
                CreateOption::CreateOrReplace
            }
        }
    }
}

impl From<CreateOption> for MatchSeq {
    /// Convert `CreateOption` to `MatchSeq`.
    ///
    /// - If `CreateOption` is `CreateIfNotExists`, then to add a record only when it does not exist, i.e., `MatchSeq` is `Exact(0)`.
    /// - If `CreateOption` is `CreateOrReplace`, then always to add a record, i.e., `MatchSeq` matches any value: `GE(0)`.
    fn from(create_option: CreateOption) -> Self {
        match create_option {
            CreateOption::Create => MatchSeq::Exact(0),
            CreateOption::CreateIfNotExists => MatchSeq::Exact(0),
            CreateOption::CreateOrReplace => MatchSeq::GE(0),
        }
    }
}

impl CreateOption {
    pub fn is_overriding(&self) -> bool {
        matches!(self, CreateOption::CreateOrReplace)
    }

    pub fn if_not_exist(&self) -> bool {
        matches!(self, CreateOption::CreateIfNotExists)
    }

    pub fn if_return_error(&self) -> bool {
        matches!(self, CreateOption::Create)
    }
}
