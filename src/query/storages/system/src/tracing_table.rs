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

use std::any::Any;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::Pipeline;

/// Outdated. will be removed after 2024-05-01
pub struct TracingTable {
    table_info: TableInfo,
}

impl TracingTable {
    pub fn create(table_id: u64) -> Self {
        let schema =
            TableSchemaRefExt::create(vec![TableField::new("entry", TableDataType::String)]);

        let table_info = TableInfo {
            desc: "'system'.'tracing'".to_string(),
            name: "tracing".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemTracing".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        TracingTable { table_info }
    }
}

#[async_trait::async_trait]
impl Table for TracingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Err(ErrorCode::Outdated(
            "Outdated. please let us know if you have used it. Will be removed after 2024-05-01",
        ))
    }

    fn read_data(
        &self,
        _: Arc<dyn TableContext>,
        _: &DataSourcePlan,
        _: &mut Pipeline,
        _: bool,
    ) -> Result<()> {
        Err(ErrorCode::Outdated(
            "Outdated. please let us know if you have used it. Will be removed after 2024-05-01",
        ))
    }
}
