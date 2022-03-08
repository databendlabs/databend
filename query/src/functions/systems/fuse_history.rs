//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;

use super::Function;
use super::FunctionDescription;
use super::FunctionFeatures;
use crate::catalogs::Catalog;
use crate::sessions::QueryContext;
use crate::storages::fuse::FuseHistory;
use crate::storages::fuse::FuseTable;

pub struct FuseHistoryFunction {}

impl FuseHistoryFunction {
    pub fn try_create(_: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(FuseHistoryFunction {}))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().num_arguments(2))
    }
}

#[async_trait::async_trait]
impl Function for FuseHistoryFunction {
    fn name(&self) -> &str {
        "system$fuse_history"
    }

    async fn eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let database_name = args[0].clone();
        let table_name = args[1].clone();
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog()
            .get_table(
                tenant_id.as_str(),
                database_name.as_str(),
                table_name.as_str(),
            )
            .await?;

        let tbl = tbl.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "expecting fuse table, but got table of engine type: {}",
                tbl.get_table_info().meta.engine
            ))
        })?;

        Ok(FuseHistory::new(ctx, tbl).get_history().await?)
    }

    fn schema(&self) -> Arc<DataSchema> {
        FuseHistory::schema()
    }
}
