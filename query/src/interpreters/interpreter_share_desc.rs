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
use common_datavalues::prelude::Series;
use common_datavalues::SeriesFrom;
use common_exception::Result;
use common_meta_api::ShareApi;
use common_meta_app::share::GetShareGrantObjectReq;
use common_meta_app::share::ShareNameIdent;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
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

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let user_mgr = self.ctx.get_user_manager();
        let meta_api = user_mgr.get_meta_store_client();
        let req = GetShareGrantObjectReq {
            share_name: ShareNameIdent {
                tenant: match &self.plan.tenant {
                    Some(tenant) => tenant.clone(),
                    None => self.ctx.get_tenant(),
                },
                share_name: self.plan.share.clone(),
            },
            object: None,
        };
        let resp = meta_api.get_share_grant_objects(req).await?;
        let desc_schema = self.plan.schema();
        println!("{:?}", resp);

        let mut names: Vec<String> = vec![];
        let mut kinds: Vec<String> = vec![];
        let mut shared_ons: Vec<String> = vec![];
        for entry in resp.objects.iter() {
            kinds.push("kind".to_string());
            names.push(entry.object.to_string());
            shared_ons.push(entry.grant_on.to_string());
        }

        println!("{:?}, {:?}", names.clone(), shared_ons.clone());
        let block = DataBlock::create(desc_schema.clone(), vec![
            Series::from_data(kinds),
            Series::from_data(names),
            Series::from_data(shared_ons),
        ]);
        println!("block: {:?}", block);

        Ok(Box::pin(DataBlockStream::create(desc_schema, None, vec![
            block,
        ])))
    }
}
