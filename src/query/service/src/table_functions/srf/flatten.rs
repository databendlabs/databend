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

use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Utc;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_args::TableArgs;
use common_catalog::table_function::TableFunction;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::*;
use common_expression::DataBlock;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_functions::srfs::FlattenGenerator;
use common_functions::srfs::FlattenMode;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::OutputPort;
use common_pipeline_core::processors::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::SyncSource;
use common_pipeline_sources::SyncSourcer;
use common_storages_factory::Table;
use common_storages_fuse::table_functions::string_value;
use common_storages_fuse::TableContext;
use jsonb::jsonpath::parse_json_path;
use jsonb::jsonpath::Mode as SelectorMode;
use jsonb::jsonpath::Selector;

pub struct FlattenTable {
    table_info: TableInfo,
    input: Vec<u8>,
    path: Option<Vec<u8>>,
    generator: FlattenGenerator,
    table_args: TableArgs,
}

impl FlattenTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let mut input = None;
        let mut path = None;
        let mut outer = false;
        let mut recursive = false;
        let mut mode = FlattenMode::Both;

        for (key, val) in &table_args.named {
            match key.to_lowercase().as_str() {
                "input" => match val.as_variant() {
                    Some(val) => {
                        input = Some(val);
                    }
                    None => {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "Expected input variant type, but got is {:?}",
                            val
                        )));
                    }
                },
                "path" => match val.as_string() {
                    Some(val) => {
                        path = Some(val);
                    }
                    None => {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "Expected path string type, but got is {:?}",
                            val
                        )));
                    }
                },
                "outer" => match val.as_boolean() {
                    Some(val) => {
                        outer = *val;
                    }
                    None => {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "Expected outer boolean type, but got is {:?}",
                            val
                        )));
                    }
                },
                "recursive" => match val.as_boolean() {
                    Some(val) => {
                        recursive = *val;
                    }
                    None => {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "Expected recursive boolean type, but got is {:?}",
                            val
                        )));
                    }
                },
                "mode" => {
                    let val = string_value(val)?;
                    match val.to_lowercase().as_str() {
                        "object" => {
                            mode = FlattenMode::Object;
                        }
                        "array" => {
                            mode = FlattenMode::Array;
                        }
                        "both" => {
                            mode = FlattenMode::Both;
                        }
                        _ => {
                            return Err(ErrorCode::BadArguments(format!("Invalid mode {:?}", val)));
                        }
                    }
                }
                _ => {
                    return Err(ErrorCode::BadArguments(format!(
                        "Invalid param name {:?}",
                        key
                    )));
                }
            }
        }

        if input.is_none() {
            return Err(ErrorCode::BadArguments(
                "Flatten function must specify input",
            ));
        }
        let input = input.unwrap();

        let schema = TableSchema::new(vec![
            TableField::new("seq", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "key",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "path",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "index",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "value",
                TableDataType::Nullable(Box::new(TableDataType::Variant)),
            ),
            TableField::new(
                "this",
                TableDataType::Nullable(Box::new(TableDataType::Variant)),
            ),
        ]);

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from(table_func_name),
            meta: TableMeta {
                schema: Arc::new(schema),
                engine: String::from(table_func_name),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: Utc
                    .from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
                updated_on: Utc
                    .from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
                ..Default::default()
            },
            ..Default::default()
        };

        let generator = FlattenGenerator::create(outer, recursive, mode);

        Ok(Arc::new(FlattenTable {
            table_info,
            input: input.clone(),
            path: path.cloned(),
            generator,
            table_args,
        }))
    }
}

impl TableFunction for FlattenTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[async_trait::async_trait]
impl Table for FlattenTable {
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
        // dummy statistics
        Ok((PartStatistics::default_exact(), Partitions::default()))
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
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                FlattenSource::create(
                    ctx.clone(),
                    output,
                    self.input.clone(),
                    self.path.clone(),
                    self.generator,
                )
            },
            1,
        )?;

        Ok(())
    }
}

struct FlattenSource {
    finished: bool,

    input: Vec<u8>,
    path: Option<Vec<u8>>,
    generator: FlattenGenerator,
}

impl FlattenSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        input: Vec<u8>,
        path: Option<Vec<u8>>,
        generator: FlattenGenerator,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx.clone(), output, Self {
            finished: false,
            input,
            path,
            generator,
        })
    }
}

impl SyncSource for FlattenSource {
    const NAME: &'static str = "FlattenSourceTransform";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }
        self.finished = true;

        let columns = match &self.path {
            Some(path) => {
                match parse_json_path(path) {
                    Ok(json_path) => {
                        // get inner input values by path
                        let mut builder = StringColumnBuilder::with_capacity(0, 0);
                        let selector = Selector::new(json_path, SelectorMode::First);
                        selector.select(&self.input, &mut builder.data, &mut builder.offsets);
                        let path = unsafe { std::str::from_utf8_unchecked(path) };
                        let inner_val = builder.pop().unwrap_or_default();
                        self.generator.generate(1, &inner_val, path)
                    }
                    Err(_) => {
                        return Err(ErrorCode::BadArguments(format!(
                            "Invalid JSON Path {:?}",
                            String::from_utf8_lossy(path)
                        )));
                    }
                }
            }
            None => self.generator.generate(1, &self.input, ""),
        };
        let block = DataBlock::new_from_columns(columns);
        Ok(Some(block))
    }
}
