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

use databend_common_ast::ast::CreateOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::tenant_dictionary_ident::TenantDictionaryIdent;
use databend_common_meta_app::schema::CreateDictionaryReq;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::UpdateDictionaryReq;
use databend_common_sql::plans::CreateDictionaryPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Clone, Debug)]
pub struct CreateDictionaryInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateDictionaryPlan,
}

impl CreateDictionaryInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateDictionaryPlan) -> Result<Self> {
        Ok(CreateDictionaryInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateDictionaryInterpreter {
    fn name(&self) -> &str {
        "CreateDictionaryInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = &self.plan.tenant;
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;

        let dictionary_meta = self.plan.meta.clone();
        let dict_ident =
            DictionaryIdentity::new(self.plan.database_id, self.plan.dictionary.clone());
        let dictionary_ident = TenantDictionaryIdent::new(tenant, dict_ident);
        let req = CreateDictionaryReq {
            dictionary_ident: dictionary_ident.clone(),
            dictionary_meta: dictionary_meta.clone(),
        };
        let reply = catalog.create_dictionary(req).await;
        if reply.is_ok() {
            return Ok(PipelineBuildResult::create());
        } else {
            match self.plan.create_option {
                CreateOption::Create => {
                    return Err(ErrorCode::DictionaryAlreadyExists(format!(
                        "Dictionary {} already exists.",
                        self.plan.dictionary,
                    )));
                }
                CreateOption::CreateIfNotExists => {
                    return Ok(PipelineBuildResult::create());
                }
                CreateOption::CreateOrReplace => {
                    let req = UpdateDictionaryReq {
                        dictionary_meta: dictionary_meta.clone(),
                        dictionary_ident: dictionary_ident.clone(),
                    };
                    let _reply = catalog.update_dictionary(req).await?;
                    return Ok(PipelineBuildResult::create());
                }
            }
        }
    }
}
