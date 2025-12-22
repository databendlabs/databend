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
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::RenameDictionaryReq;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_sql::plans::RenameDictionaryPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct RenameDictionaryInterpreter {
    ctx: Arc<QueryContext>,
    plan: RenameDictionaryPlan,
}

impl RenameDictionaryInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RenameDictionaryPlan) -> Result<Self> {
        Ok(RenameDictionaryInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RenameDictionaryInterpreter {
    fn name(&self) -> &str {
        "RenameDictionaryInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = &self.plan.tenant;
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;

        let dict_ident =
            DictionaryIdentity::new(self.plan.database_id, self.plan.dictionary.clone());
        let name_ident = DictionaryNameIdent::new(tenant, dict_ident);

        let new_dict_ident =
            DictionaryIdentity::new(self.plan.new_database_id, self.plan.new_dictionary.clone());

        let req = RenameDictionaryReq {
            name_ident,
            new_dict_ident,
        };

        let reply = catalog.rename_dictionary(req).await;
        if let Err(err) = reply {
            if !(self.plan.if_exists && err.code() == ErrorCode::UNKNOWN_DICTIONARY) {
                return Err(err);
            }
        }
        Ok(PipelineBuildResult::create())
    }
}
