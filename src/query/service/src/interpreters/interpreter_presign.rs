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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VariantType;
use databend_common_meta_app::storage::StorageAzblobConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storage::AzblobPresignOp;
use databend_common_storage::azblob_user_delegation_presign;
use databend_common_storage::azblob_user_delegation_presign_supported;
use databend_common_storage::init_stage_operator;
use databend_common_storage::internal_stage_storage_params;
use jsonb::Value as JsonbValue;
use log::debug;
use log::info;
use opendal::Operator;
use opendal::raw::PresignedRequest;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::PresignAction;
use crate::sql::plans::PresignPlan;

pub struct PresignInterpreter {
    ctx: Arc<dyn TableContext>,
    plan: PresignPlan,
}

impl PresignInterpreter {
    /// Create a PresignInterpreter with context and [`PresignPlan`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: PresignPlan) -> Result<Self> {
        Ok(PresignInterpreter { ctx, plan })
    }

    async fn presign_with_operator(&self, op: &Operator) -> Result<PresignedRequest> {
        Ok(match self.plan.action {
            PresignAction::Download => op.presign_read(&self.plan.path, self.plan.expire).await?,
            PresignAction::Upload => {
                let mut fut = op.presign_write_with(&self.plan.path, self.plan.expire);
                if let Some(content_type) = &self.plan.content_type {
                    fut = fut.content_type(content_type);
                }
                fut.await?
            }
        })
    }

    /// Azure internal/user stage without a static credential, where presign
    /// falls back to a Workload Identity user delegation SAS.
    fn user_delegation_config(&self) -> Option<StorageAzblobConfig> {
        match internal_stage_storage_params(&self.plan.stage)? {
            StorageParams::Azblob(cfg) if azblob_user_delegation_presign_supported(&cfg) => {
                Some(cfg)
            }
            _ => None,
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for PresignInterpreter {
    fn name(&self) -> &str {
        "PresignInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            debug!("ctx.id" = self.ctx.get_id().as_str(); "presign_interpreter_execute");

            let op = init_stage_operator(&self.plan.stage)?;
            let start_time = std::time::Instant::now();
            let presigned_req = if op.info().full_capability().presign {
                self.presign_with_operator(&op).await?
            } else if let Some(cfg) = self.user_delegation_config() {
                // Workload Identity deployments have no static credential to
                // presign with; mint a short-lived user delegation SAS.
                let op = match self.plan.action {
                    PresignAction::Download => AzblobPresignOp::Read,
                    PresignAction::Upload => AzblobPresignOp::Write {
                        content_type: self.plan.content_type.as_deref(),
                    },
                };
                azblob_user_delegation_presign(&cfg, &self.plan.path, op, self.plan.expire).await?
            } else {
                return Err(ErrorCode::StorageUnsupported(
                    "storage doesn't support presign operation",
                ));
            };
            info!(
                "query_id" = self.ctx.get_id();
                "presign {:?} {} success in {}ms", self.plan.action, self.plan.path, start_time.elapsed().as_millis()
            );

            let header = JsonbValue::Object(
                presigned_req
                    .header()
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k.to_string(),
                            JsonbValue::String(
                                v.to_str()
                                    .expect("header value generated by opendal must be valid")
                                    .to_string()
                                    .into(),
                            ),
                        )
                    })
                    .collect(),
            );

            let block = DataBlock::new(
                vec![
                    BlockEntry::new_const_column_arg::<StringType>(
                        presigned_req.method().as_str().to_string(),
                        1,
                    ),
                    BlockEntry::new_const_column_arg::<VariantType>(header.to_vec(), 1),
                    BlockEntry::new_const_column_arg::<StringType>(
                        presigned_req.uri().to_string(),
                        1,
                    ),
                ],
                1,
            );

            PipelineBuildResult::from_blocks(vec![block])
        })
    }
}
