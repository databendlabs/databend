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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_sql::plans::UnsetOptionsPlan;
use databend_common_storages_fuse::TableContext;
use databend_meta_types::MatchSeq;

use crate::interpreters::Interpreter;
use crate::interpreters::common::table_option_validation::UNSET_TABLE_OPTIONS_WHITE_LIST;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct UnsetOptionsInterpreter {
    ctx: Arc<QueryContext>,
    plan: UnsetOptionsPlan,
}

impl UnsetOptionsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: UnsetOptionsPlan) -> Result<Self> {
        Ok(UnsetOptionsInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for UnsetOptionsInterpreter {
    fn name(&self) -> &str {
        "SetOptionsInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        let database = self.plan.database.as_str();
        let table_name = self.plan.table.as_str();
        let table = catalog
            .get_table(&self.ctx.get_tenant(), database, table_name)
            .await?;

        // check mutability
        table.check_mutable()?;

        let table_version = table.get_table_info().ident.seq;

        let opts_to_be_del: HashMap<String, Option<String>> = self
            .plan
            .options
            .iter()
            .map(|opt_name| (opt_name.to_owned(), None))
            .collect();

        let mut opts_not_allowed_to_unset: Vec<&String> = opts_to_be_del
            .keys()
            .filter(|k| !UNSET_TABLE_OPTIONS_WHITE_LIST.contains(k.as_str()))
            .collect();

        if !opts_not_allowed_to_unset.is_empty() {
            opts_not_allowed_to_unset.sort();
            return Err(ErrorCode::TableOptionInvalid(format!(
                "table option {:?} is not allowed to be unset",
                opts_not_allowed_to_unset,
            )));
        }

        let req = UpsertTableOptionReq {
            table_id: table.get_id(),
            seq: MatchSeq::Exact(table_version),
            options: opts_to_be_del,
        };

        catalog
            .upsert_table_option(&self.ctx.get_tenant(), database, req)
            .await?;
        Ok(PipelineBuildResult::create())
    }
}
