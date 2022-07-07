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
use std::collections::BTreeMap;
use std::str::FromStr;

use common_ast::ast::CopyStmt;
use common_ast::ast::CopyUnit;
use common_ast::ast::PresignAction as AstPresignAction;
use common_ast::ast::PresignLocation;
use common_ast::ast::PresignStmt;
use common_ast::ast::Query;
use common_ast::ast::Statement;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserStageInfo;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;
use common_planners::StageTableInfo;
use time::Duration;

use crate::sql::binder::Binder;
use crate::sql::plans::CopyPlanV2;
use crate::sql::plans::Plan;
use crate::sql::plans::PresignAction;
use crate::sql::plans::PresignPlan;
use crate::sql::plans::ValidationMode;
use crate::sql::statements::parse_copy_file_format_options;
use crate::sql::statements::parse_stage_location_v2;
use crate::sql::statements::parse_uri_location_v2;
use crate::sql::BindContext;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_presign(
        &mut self,
        _: &BindContext,
        stmt: &PresignStmt,
    ) -> Result<Plan> {
        match &stmt.location {
            PresignLocation::StageLocation { name, path } => {
                let (stage_info, path) = parse_stage_location_v2(&self.ctx, name, path).await?;

                Ok(Plan::Presign(Box::new(PresignPlan {
                    stage: Box::new(stage_info),
                    path,
                    action: match stmt.action {
                        AstPresignAction::Download => PresignAction::Download,
                        AstPresignAction::Upload => PresignAction::Upload,
                    },
                    expire: Duration::seconds(stmt.expire.as_secs() as i64),
                })))
            }
        }
    }
}
