// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::scalars::FunctionFactory;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UserDefinedFunction;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::sessions::QueryContext;
use crate::storages::Table;

pub struct FunctionsTable {
    table_info: TableInfo,
}

impl FunctionsTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("is_builtin", bool::to_data_type()),
            DataField::new("is_aggregate", bool::to_data_type()),
            DataField::new("definition", Vu8::to_data_type()),
            DataField::new("description", Vu8::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'functions'".to_string(),
            name: "functions".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemFunctions".to_string(),

                ..Default::default()
            },
        };
        FunctionsTable { table_info }
    }

    async fn get_udfs(ctx: Arc<QueryContext>) -> Result<Vec<UserDefinedFunction>> {
        let tenant = ctx.get_tenant();
        let user_mgr = ctx.get_user_manager();
        user_mgr.get_udfs(&tenant).await
    }
}

#[async_trait::async_trait]
impl Table for FunctionsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let function_factory = FunctionFactory::instance();
        let aggregate_function_factory = AggregateFunctionFactory::instance();
        let func_names = function_factory.registered_names();
        let aggr_func_names = aggregate_function_factory.registered_names();
        let udfs = FunctionsTable::get_udfs(ctx).await?;

        let names: Vec<&[u8]> = func_names
            .iter()
            .chain(aggr_func_names.iter())
            .chain(udfs.iter().map(|udf| &udf.name))
            .map(|x| x.as_bytes())
            .collect();
        let builtin_func_len = func_names.len() + aggr_func_names.len();

        let is_builtin = (0..names.len())
            .map(|i| i < builtin_func_len)
            .collect::<Vec<bool>>();

        let is_aggregate = (0..names.len())
            .map(|i| i >= func_names.len() && i < builtin_func_len)
            .collect::<Vec<bool>>();

        let definitions = (0..names.len())
            .map(|i| {
                if i < builtin_func_len {
                    ""
                } else {
                    udfs.get(i - builtin_func_len)
                        .map_or("", |udf| udf.definition.as_str())
                }
            })
            .collect::<Vec<&str>>();

        let descriptions = (0..names.len())
            .map(|i| {
                if i < builtin_func_len {
                    ""
                } else {
                    udfs.get(i - builtin_func_len)
                        .map_or("", |udf| udf.description.as_str())
                }
            })
            .collect::<Vec<&str>>();

        let block = DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(names),
            Series::from_data(is_builtin),
            Series::from_data(is_aggregate),
            Series::from_data(definitions),
            Series::from_data(descriptions),
        ]);

        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }
}
