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

use databend_common_ast::ast::PresignAction as AstPresignAction;
use databend_common_ast::ast::PresignLocation;
use databend_common_ast::ast::PresignStmt;
use databend_common_exception::Result;

use crate::BindContext;
use crate::binder::Binder;
use crate::binder::StagePathAccess;
use crate::binder::StageResolver;
use crate::plans::Plan;
use crate::plans::PresignAction;
use crate::plans::PresignPlan;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_presign(
        &mut self,
        _: &BindContext,
        stmt: &PresignStmt,
    ) -> Result<Plan> {
        match &stmt.location {
            PresignLocation::StageLocation(stage_location) => {
                let (stage_info, path) = StageResolver::from_table_context(
                    self.ctx.clone(),
                    databend_common_users::UserApiProvider::instance(),
                    databend_common_config::GlobalConfig::instance()
                        .storage
                        .allow_insecure,
                )?
                .resolve_stage_location(stage_location, match stmt.action {
                    AstPresignAction::Upload => StagePathAccess::Write,
                    AstPresignAction::Download => StagePathAccess::Read,
                })
                .await?;

                Ok(Plan::Presign(Box::new(PresignPlan {
                    stage: Box::new(stage_info),
                    path,
                    action: match stmt.action {
                        AstPresignAction::Download => PresignAction::Download,
                        AstPresignAction::Upload => PresignAction::Upload,
                    },
                    expire: stmt.expire,
                    content_type: stmt.content_type.clone(),
                })))
            }
        }
    }
}
