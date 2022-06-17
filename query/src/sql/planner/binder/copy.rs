// Copyright 2022 Datafuse Labs.
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

use std::str::FromStr;

use common_ast::ast::CopyStmt;
use common_ast::ast::CreateStageStmt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::OnErrorMode;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_planners::CreateUserStagePlan;
use common_planners::ListPlan;
use common_planners::RemoveUserStagePlan;

use crate::sql::binder::Binder;
use crate::sql::plans::Plan;
use crate::sql::statements::parse_copy_file_format_options;
use crate::sql::statements::parse_stage_location;
use crate::sql::statements::parse_uri_location;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_copy(
        &mut self,
        stmt: &CopyStmt<'a>,
    ) -> Result<Plan> {
        todo!()
    }
}
