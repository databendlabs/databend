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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::DataSchemaRef;
use common_datavalues::prelude::DataSchemaRefExt;
use common_datavalues::prelude::Series;
use common_datavalues::SeriesFrom;
use common_exception::Result;
use common_meta_api::ShareApi;
use common_meta_app::share::GetShareGrantObjectReq;
use common_meta_app::share::ShareGrantObjectName;
use common_meta_app::share::ShareNameIdent;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::share::DescSharePlan;

pub struct DescShareInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescSharePlan,
}

impl DescShareInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescSharePlan) -> Result<Self> {
        Ok(DescShareInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescShareInterpreter {
    fn name(&self) -> &str {
        "DescShareInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let user_mgr = self.ctx.get_user_manager();
        let meta_api = user_mgr.get_meta_store_client();
        let req = GetShareGrantObjectReq {
            share_name: ShareNameIdent {
                tenant: self.ctx.get_tenant(),
                share_name: self.plan.share.clone(),
            },
        };
        let resp = meta_api.get_share_grant_objects(req).await?;
        if resp.objects.is_empty() {
            return Ok(PipelineBuildResult::create());
        }

        let desc_schema = self.plan.schema();

        let mut names: Vec<String> = vec![];
        let mut kinds: Vec<String> = vec![];
        let mut shared_ons: Vec<String> = vec![];
        for entry in resp.objects.iter() {
            match &entry.object {
                ShareGrantObjectName::Database(db) => {
                    kinds.push("DATABASE".to_string());
                    names.push(db.clone());
                }
                ShareGrantObjectName::Table(db, table_name) => {
                    kinds.push("TABLE".to_string());
                    names.push(format!("{}.{}", db, table_name));
                }
            }
            shared_ons.push(entry.grant_on.to_string());
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::create(desc_schema.clone(), vec![
            Series::from_data(kinds),
            Series::from_data(names),
            Series::from_data(shared_ons),
        ])])
    }
}
