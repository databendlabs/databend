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
use common_meta_app::share::ShowSharesReq;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::share::ShowSharesPlan;

pub struct ShowSharesInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowSharesPlan,
}

impl ShowSharesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowSharesPlan) -> Result<Self> {
        Ok(ShowSharesInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowSharesInterpreter {
    fn name(&self) -> &str {
        "ShowSharesInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let user_mgr = self.ctx.get_user_manager();
        let meta_api = user_mgr.get_meta_store_client();
        let tenant = self.ctx.get_tenant();
        let req = ShowSharesReq {
            tenant: tenant.clone(),
        };
        let resp = meta_api.show_shares(req).await?;
        if resp.inbound_accounts.is_empty() && resp.outbound_accounts.is_empty() {
            return Ok(Box::pin(DataBlockStream::create(
                DataSchemaRefExt::create(vec![]),
                None,
                vec![],
            )));
        }

        let desc_schema = self.plan.schema();

        let mut names: Vec<String> = vec![];
        let mut kinds: Vec<String> = vec![];
        let mut created_ons: Vec<String> = vec![];
        let mut database_names: Vec<String> = vec![];
        let mut from: Vec<String> = vec![];
        let mut to: Vec<String> = vec![];
        let mut comments: Vec<String> = vec![];
        for entry in resp.inbound_accounts {
            names.push(entry.share_name.share_name.clone());
            kinds.push("INBOUND".to_string());
            created_ons.push(entry.create_on.to_string());
            database_names.push(
                entry
                    .database_name
                    .map_or("".to_string(), |database_name| database_name),
            );
            from.push(entry.share_name.tenant.clone());
            to.push(tenant.clone());
            comments.push(entry.comment.map_or("".to_string(), |comment| comment));
        }
        for entry in resp.outbound_accounts {
            names.push(entry.share_name.share_name.clone());
            kinds.push("OUTBOUND".to_string());
            created_ons.push(entry.create_on.to_string());
            database_names.push(
                entry
                    .database_name
                    .map_or("".to_string(), |database_name| database_name),
            );
            from.push(entry.share_name.tenant.clone());
            to.push(
                entry
                    .accounts
                    .map_or("".to_string(), |accounts| accounts.join(",")),
            );
            comments.push(entry.comment.map_or("".to_string(), |comment| comment));
        }

        let block = DataBlock::create(desc_schema.clone(), vec![
            Series::from_data(created_ons),
            Series::from_data(kinds),
            Series::from_data(names),
            Series::from_data(database_names),
            Series::from_data(from),
            Series::from_data(to),
            Series::from_data(comments),
        ]);
        Ok(Box::pin(DataBlockStream::create(desc_schema, None, vec![
            block,
        ])))
    }
}
