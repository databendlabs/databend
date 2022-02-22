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
use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::{MetaId, TableIdent};
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::{Expression, Extras, Partitions, ReadDataSourcePlan, Statistics, TruncateTablePlan};
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::pipelines::new::{NewPipe, NewPipeline};
use crate::pipelines::new::processors::{AsyncSource, AsyncSourcer, SyncSourcer};
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::sessions::QueryContext;
use crate::storages::Table;

pub struct ColumnsTable {
    table_info: TableInfo,
}

impl ColumnsTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("database", Vu8::to_data_type()),
            DataField::new("table", Vu8::to_data_type()),
            DataField::new("data_type", Vu8::to_data_type()),
            DataField::new("is_nullable", bool::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'columns'".to_string(),
            name: "columns".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemColumns".to_string(),
                ..Default::default()
            },
        };

        Self { table_info }
    }

    pub async fn dump_table_columns(
        &self,
        ctx: Arc<QueryContext>,
    ) -> Result<Vec<(String, String, DataField)>> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog();
        let databases = catalog.list_databases(tenant.as_str()).await?;

        let mut rows: Vec<(String, String, DataField)> = vec![];
        for database in databases {
            for table in catalog
                .list_tables(tenant.as_str(), database.name())
                .await?
            {
                for field in table.schema().fields() {
                    rows.push((database.name().into(), table.name().into(), field.clone()))
                }
            }
        }

        Ok(rows)
    }
}

#[async_trait::async_trait]
impl Table for ColumnsTable {
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
        let rows = self.dump_table_columns(ctx).await?;
        let mut names: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut tables: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut databases: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut data_types: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut is_nullables: Vec<bool> = Vec::with_capacity(rows.len());
        for (database_name, table_name, field) in rows.into_iter() {
            names.push(field.name().clone().into_bytes());
            tables.push(table_name.into_bytes());
            databases.push(database_name.into_bytes());
            let type_str = format!("{:?}", field.data_type());
            data_types.push(type_str.into_bytes());
            is_nullables.push(field.is_nullable());
        }

        let block = DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(names),
            Series::from_data(databases),
            Series::from_data(tables),
            Series::from_data(data_types),
            Series::from_data(is_nullables),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }

    fn read2(&self, ctx: Arc<QueryContext>, _: &ReadDataSourcePlan, pipeline: &mut NewPipeline) -> Result<()> {
        let schema = self.table_info.schema();
        let output = OutputPort::create();
        pipeline.add_pipe(NewPipe::SimplePipe {
            processors: vec![ColumnsTableSource::create(output.clone(), ctx, schema)?],
            inputs_port: vec![],
            outputs_port: vec![output],
        });

        Ok(())
    }
}

struct ColumnsTableSource {
    finished: bool,
    schema: DataSchemaRef,
    context: Arc<QueryContext>,
}

impl ColumnsTableSource {
    pub fn create(output: Arc<OutputPort>, context: Arc<QueryContext>, schema: DataSchemaRef) -> Result<ProcessorPtr> {
        AsyncSourcer::create(output, ColumnsTableSource { finished: false, schema, context })
    }

    async fn dump_table_columns(&self) -> Result<Vec<(String, String, DataField)>> {
        let tenant = self.context.get_tenant();
        let catalog = self.context.get_catalog();
        let databases = catalog.list_databases(tenant.as_str()).await?;

        let mut rows: Vec<(String, String, DataField)> = vec![];
        for database in databases {
            for table in catalog
                .list_tables(tenant.as_str(), database.name())
                .await?
            {
                for field in table.schema().fields() {
                    rows.push((database.name().into(), table.name().into(), field.clone()))
                }
            }
        }

        Ok(rows)
    }
}

#[async_trait::async_trait]
impl AsyncSource for ColumnsTableSource {
    const NAME: &'static str = "ColumnsTableSource";

    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }

        self.finished = true;
        let rows = self.dump_table_columns().await?;
        let mut names: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut tables: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut databases: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut data_types: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut is_nullables: Vec<bool> = Vec::with_capacity(rows.len());
        for (database_name, table_name, field) in rows.into_iter() {
            names.push(field.name().clone().into_bytes());
            tables.push(table_name.into_bytes());
            databases.push(database_name.into_bytes());
            let type_str = format!("{:?}", field.data_type());
            data_types.push(type_str.into_bytes());
            is_nullables.push(field.is_nullable());
        }

        Ok(Some(DataBlock::create(self.schema.clone(), vec![
            Series::from_data(names),
            Series::from_data(databases),
            Series::from_data(tables),
            Series::from_data(data_types),
            Series::from_data(is_nullables),
        ])))
    }
}

