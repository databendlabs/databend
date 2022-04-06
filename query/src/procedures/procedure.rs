// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::validate_function_arg;

use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;

#[async_trait::async_trait]
pub trait Procedure: Sync + Send {
    fn name(&self) -> &str;

    fn features(&self) -> ProcedureFeatures;

    fn validate(&self, ctx: Arc<QueryContext>, args: &[String]) -> Result<()> {
        let features = self.features();
        validate_function_arg(
            self.name(),
            args.len(),
            features.variadic_arguments,
            features.num_arguments,
        )?;
        if features.management_mode_required && !ctx.get_config().query.management_mode {
            return Err(ErrorCode::ManagementModePermissionDenied(format!(
                "Access denied: '{}' only used in management-mode",
                self.name()
            )));
        }
        if let Some(user_option_flag) = features.user_option_flag {
            let user_info = ctx.get_current_user()?;
            if !user_info.has_option_flag(user_option_flag) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Access denied: '{}' requires user {} option flag",
                    self.name(),
                    user_option_flag
                )));
            }
        }
        Ok(())
    }

    async fn eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        self.validate(ctx.clone(), &args)?;
        self.inner_eval(ctx, args).await
    }

    async fn inner_eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock>;

    fn schema(&self) -> Arc<DataSchema>;
}
