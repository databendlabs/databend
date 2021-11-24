// Copyright 2020 Datafuse Labs.
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

use std::any::Any;
use std::collections::HashMap;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::CreateTableReq;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;
use crate::datasources::context::DataSourceContext;
use crate::datasources::database::github::database::RepoPrsEngine;
use crate::sessions::DatabendQueryContextRef;

pub struct RepoPrsTable {
    table_info: TableInfo,
}

impl RepoPrsTable {
    pub async fn create(ctx: DataSourceContext, owner: String, repo: String) -> Result<()> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", DataType::String, false),
            DataField::new("host", DataType::String, false),
            DataField::new("port", DataType::UInt16, false),
        ]);

        let mut options = HashMap::new();
        options.insert("owner".to_string(), owner.clone());
        options.insert("repo".to_string(), repo.clone());

        let req = CreateTableReq {
            if_not_exists: false,
            db: owner.clone(),
            table: repo.clone() + "_prs",
            table_meta: TableMeta {
                schema: schema,
                engine: RepoPrsEngine.into(),
                options: options,
            },
        };
        ctx.meta.create_table(req).await?;
        Ok(())
    }

    pub fn build_table(table_info: &TableInfo) -> Box<dyn Table> {
        Box::new(Self {
            table_info: table_info.clone(),
        })
    }
}

#[async_trait::async_trait]
impl Table for RepoPrsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        unimplemented!()
    }
}
