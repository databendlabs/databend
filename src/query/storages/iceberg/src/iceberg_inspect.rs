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

use chrono::DateTime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::parse_db_tb_args;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use futures::StreamExt;

use crate::IcebergTable;

pub struct IcebergInspectTable {
    table_info: TableInfo,
    args_parsed: IcebergInspectArgsParsed,
    table_args: TableArgs,
    inspect_type: InspectType,
}

impl IcebergInspectTable {
    pub fn create(
        _database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> databend_common_exception::Result<Arc<dyn TableFunction>> {
        let args_parsed = IcebergInspectArgsParsed::parse(&table_args, table_func_name)?;
        let inspect_type = InspectType::from_name(table_func_name);
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", args_parsed.database, args_parsed.table),
            name: String::from(table_func_name),
            meta: TableMeta {
                schema: inspect_type.schema(),
                engine: String::from(table_func_name),
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };
        Ok(Arc::new(IcebergInspectTable {
            table_info,
            args_parsed,
            table_args,
            inspect_type,
        }))
    }

    pub fn create_source(
        &self,
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<ProcessorPtr> {
        IcebergInspectSource::create(
            ctx,
            output,
            self.args_parsed.database.clone(),
            self.args_parsed.table.clone(),
            self.inspect_type,
        )
    }
}

#[async_trait::async_trait]
impl Table for IcebergInspectTable {
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
    ) -> databend_common_exception::Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::new_exact(1, 1, 1, 1), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(self.table_args.clone())
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> databend_common_exception::Result<()> {
        pipeline.add_source(|output| self.create_source(ctx.clone(), output, None), 1)?;
        Ok(())
    }
}

struct IcebergInspectSource {
    is_finished: bool,
    database: String,
    table: String,
    iceberg_table: Option<IcebergTable>,
    inspect_type: InspectType,
    ctx: Arc<dyn TableContext>,
}

impl IcebergInspectSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        database: String,
        table: String,
        inspect_type: InspectType,
    ) -> databend_common_exception::Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, IcebergInspectSource {
            ctx,
            database,
            table,
            iceberg_table: None,
            is_finished: false,
            inspect_type,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for IcebergInspectSource {
    const NAME: &'static str = "iceberg_inspect";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> databend_common_exception::Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        if self.iceberg_table.is_none() {
            let table = self
                .ctx
                .get_table(
                    &self.ctx.get_current_catalog(),
                    self.database.as_str(),
                    self.table.as_str(),
                )
                .await?;
            let table = IcebergTable::try_from_table(table.as_ref())?;
            self.iceberg_table = Some(table.clone());
        }

        let table = self.iceberg_table.as_ref().unwrap();
        let is = table.table.inspect();
        let mut blocks = vec![];

        let sc = match self.inspect_type {
            InspectType::Snapshot => {
                let sns = is.snapshots();
                sns.scan().await
            }
            InspectType::Manifest => {
                let mns = is.manifests();
                mns.scan().await
            }
        };

        let mut stream = sc.map_err(|err| {
            ErrorCode::Internal(format!("iceberg table inspect scan build: {err:?}"))
        })?;
        let schema = self.inspect_type.schema();
        let schema = DataSchema::from(schema);
        while let Some(Ok(d)) = stream.next().await {
            let block = DataBlock::from_record_batch(&schema, &d)?;
            blocks.push(block);
        }

        self.is_finished = true;
        if !blocks.is_empty() {
            Ok(Some(DataBlock::concat(&blocks)?))
        } else {
            Ok(None)
        }
    }
}

impl TableFunction for IcebergInspectTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[derive(Clone)]
pub(crate) struct IcebergInspectArgsParsed {
    pub(crate) database: String,
    pub(crate) table: String,
}

impl IcebergInspectArgsParsed {
    fn parse(table_args: &TableArgs, name: &str) -> databend_common_exception::Result<Self> {
        let (database, table) = parse_db_tb_args(table_args, name)?;
        Ok(IcebergInspectArgsParsed { database, table })
    }
}

#[derive(Debug, Copy, Clone)]
enum InspectType {
    Snapshot,
    Manifest,
}

impl InspectType {
    pub fn schema(&self) -> TableSchemaRef {
        match self {
            InspectType::Snapshot => TableSchemaRefExt::create(vec![
                TableField::new("committed_at", TableDataType::Timestamp),
                TableField::new("snapshot_id", TableDataType::Number(NumberDataType::Int64)),
                TableField::new("parent_id", TableDataType::Number(NumberDataType::Int64)),
                TableField::new("operation", TableDataType::String),
                TableField::new("manifest_list", TableDataType::String),
                TableField::new(
                    "summary",
                    TableDataType::Map(Box::new(TableDataType::Tuple {
                        fields_name: vec!["keys".to_string(), "values".to_string()],
                        fields_type: vec![
                            TableDataType::String,
                            TableDataType::String.wrap_nullable(),
                        ],
                    })),
                ),
            ]),
            InspectType::Manifest => {
                let fields = vec![
                    TableField::new("content", TableDataType::Number(NumberDataType::Int32)),
                    TableField::new("path", TableDataType::String),
                    TableField::new("length", TableDataType::Number(NumberDataType::Int64)),
                    TableField::new(
                        "partition_spec_id",
                        TableDataType::Number(NumberDataType::Int32),
                    ),
                    TableField::new(
                        "added_snapshot_id",
                        TableDataType::Number(NumberDataType::Int64).wrap_nullable(),
                    ),
                    TableField::new(
                        "added_data_files_count",
                        TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
                    ),
                    TableField::new(
                        "existing_data_files_count",
                        TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
                    ),
                    TableField::new(
                        "deleted_data_files_count",
                        TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
                    ),
                    TableField::new(
                        "added_delete_files_count",
                        TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
                    ),
                    TableField::new(
                        "existing_delete_files_count",
                        TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
                    ),
                    TableField::new(
                        "deleted_delete_files_count",
                        TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
                    ),
                    TableField::new(
                        "partition_summaries",
                        TableDataType::Array(Box::new(
                            TableDataType::Tuple {
                                fields_name: vec![
                                    "contains_null".to_string(),
                                    "contains_nan".to_string(),
                                    "lower_bound".to_string(),
                                    "upper_bound".to_string(),
                                ],
                                fields_type: vec![
                                    TableDataType::Boolean.wrap_nullable(),
                                    TableDataType::Boolean.wrap_nullable(),
                                    TableDataType::String,
                                    TableDataType::String,
                                ],
                            }
                            .wrap_nullable(),
                        )),
                    ),
                ];

                Arc::new(TableSchema::new(fields))
            }
        }
    }

    pub fn from_name(name: &str) -> InspectType {
        match name {
            "iceberg_snapshot" => InspectType::Snapshot,
            "iceberg_manifest" => InspectType::Manifest,
            _ => unimplemented!(),
        }
    }
}
