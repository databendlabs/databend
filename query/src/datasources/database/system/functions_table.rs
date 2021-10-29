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
use std::sync::Arc;

use common_context::TableIOContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::scalars::FunctionFactory;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;

pub struct FunctionsTable {
    table_info: TableInfo,
}

impl FunctionsTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", DataType::String, false),
            DataField::new("is_aggregate", DataType::Boolean, false),
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
        _io_ctx: Arc<TableIOContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let function_factory = FunctionFactory::instance();
        let aggregate_function_factory = AggregateFunctionFactory::instance();
        let func_names = function_factory.registered_names();
        let aggr_func_names = aggregate_function_factory.registered_names();

        let names: Vec<&[u8]> = func_names
            .iter()
            .chain(aggr_func_names.iter())
            .map(|x| x.as_bytes())
            .collect();

        let is_aggregate = (0..names.len())
            .map(|i| i >= func_names.len())
            .collect::<Vec<bool>>();

        let block = DataBlock::create_by_array(self.table_info.schema(), vec![
            Series::new(names),
            Series::new(is_aggregate),
        ]);

        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }
}
