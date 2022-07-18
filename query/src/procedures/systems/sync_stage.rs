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

use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::StageType;

use crate::interpreters::list_files_from_dal;
use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;

pub struct SyncStageFileProcedure;

impl SyncStageFileProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(Box::new(SyncStageFileProcedure {}))
    }
}

#[async_trait::async_trait]
impl Procedure for SyncStageFileProcedure {
    fn name(&self) -> &str {
        "SYNC_STAGE_FILE"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default().num_arguments(2)
    }

    async fn inner_eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let stage_name = args[0].clone();
        let path = args[1].clone();
        let path = path.trim_start_matches('/');

        let tenant = ctx.get_tenant();
        let user_mgr = ctx.get_user_manager();

        let stage = user_mgr.get_stage(&tenant, &stage_name).await?;
        if stage.stage_type != StageType::Internal {
            return Err(ErrorCode::BadArguments("only support internal stage"));
        }

        let prefix = stage.get_prefix();
        let path = format!("{prefix}{path}");
        let files = list_files_from_dal(&ctx, &stage, &path, "").await?;
        for file in files.iter() {
            let mut file = file.clone();
            file.path = file
                .path
                .trim_start_matches('/')
                .trim_start_matches(prefix.trim_start_matches('/'))
                .to_string();
            let _ = user_mgr.add_file(&tenant, &stage.stage_name, file).await;
        }
        Ok(DataBlock::empty())
    }

    fn schema(&self) -> Arc<DataSchema> {
        Arc::new(DataSchema::empty())
    }
}
