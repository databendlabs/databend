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
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::SyncSource;
use common_pipeline_sources::SyncSourcer;
use common_storages_factory::Table;
use common_storages_fuse::table_functions::string_value;
use common_storages_fuse::TableContext;
use jsonb::array_length;
use jsonb::as_str;
use jsonb::get_by_index;
use jsonb::get_by_name;
use jsonb::jsonpath::parse_json_path;
use jsonb::jsonpath::Mode as SelectorMode;
use jsonb::jsonpath::Selector;
use jsonb::object_keys;

pub struct FlattenTable {
    table_info: TableInfo,
    input: Vec<u8>,
    path: Option<Vec<u8>>,
    outer: bool,
    recursive: bool,
    mode: FlattenMode,
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
        Ok(Arc::new(FlattenTable {
            table_info,
            input: input.clone(),
            path: path.cloned(),
            outer,
            recursive,
            mode,
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
                    self.outer,
                    self.recursive,
                    self.mode,
                )
            },
            1,
        )?;

        Ok(())
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum FlattenMode {
    Both,
    Object,
    Array,
}

struct FlattenSource {
    finished: bool,

    input: Vec<u8>,
    path: Option<Vec<u8>>,
    outer: bool,
    recursive: bool,
    mode: FlattenMode,
}

impl FlattenSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        input: Vec<u8>,
        path: Option<Vec<u8>>,
        outer: bool,
        recursive: bool,
        mode: FlattenMode,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx.clone(), output, Self {
            finished: false,
            input,
            path,
            outer,
            recursive,
            mode,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn flatten(
        &mut self,
        input: &[u8],
        path: &str,
        keys: &mut Vec<Option<Vec<u8>>>,
        paths: &mut Vec<Option<Vec<u8>>>,
        indices: &mut Vec<Option<u64>>,
        values: &mut Vec<Option<Vec<u8>>>,
        thises: &mut Vec<Option<Vec<u8>>>,
    ) {
        match self.mode {
            FlattenMode::Object => {
                self.flatten_object(input, path, keys, paths, indices, values, thises);
            }
            FlattenMode::Array => {
                self.flatten_array(input, path, keys, paths, indices, values, thises);
            }
            FlattenMode::Both => {
                self.flatten_array(input, path, keys, paths, indices, values, thises);
                self.flatten_object(input, path, keys, paths, indices, values, thises);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn flatten_array(
        &mut self,
        input: &[u8],
        path: &str,
        keys: &mut Vec<Option<Vec<u8>>>,
        paths: &mut Vec<Option<Vec<u8>>>,
        indices: &mut Vec<Option<u64>>,
        values: &mut Vec<Option<Vec<u8>>>,
        thises: &mut Vec<Option<Vec<u8>>>,
    ) {
        if let Some(len) = array_length(input) {
            for i in 0..len {
                let val = get_by_index(input, i).unwrap();
                keys.push(None);
                let inner_path = format!("{}[{}]", path, i);
                paths.push(Some(inner_path.as_bytes().to_vec()));
                indices.push(Some(i.try_into().unwrap()));
                values.push(Some(val.clone()));
                thises.push(Some(input.to_vec().clone()));

                if self.recursive {
                    self.flatten(&val, &inner_path, keys, paths, indices, values, thises);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn flatten_object(
        &mut self,
        input: &[u8],
        path: &str,
        keys: &mut Vec<Option<Vec<u8>>>,
        paths: &mut Vec<Option<Vec<u8>>>,
        indices: &mut Vec<Option<u64>>,
        values: &mut Vec<Option<Vec<u8>>>,
        thises: &mut Vec<Option<Vec<u8>>>,
    ) {
        if let Some(obj_keys) = object_keys(input) {
            if let Some(len) = array_length(&obj_keys) {
                for i in 0..len {
                    let key = get_by_index(&obj_keys, i).unwrap();
                    let name = as_str(&key).unwrap();
                    let val = get_by_name(input, &name, false).unwrap();

                    keys.push(Some(name.as_bytes().to_vec()));
                    let inner_path = if !path.is_empty() {
                        format!("{}.{}", path, name)
                    } else {
                        name.to_string()
                    };
                    paths.push(Some(inner_path.as_bytes().to_vec()));
                    indices.push(None);
                    values.push(Some(val.clone()));
                    thises.push(Some(input.to_vec().clone()));

                    if self.recursive {
                        self.flatten(&val, &inner_path, keys, paths, indices, values, thises);
                    }
                }
            }
        }
    }
}

impl SyncSource for FlattenSource {
    const NAME: &'static str = "FlattenSourceTransform";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }
        self.finished = true;

        // get inner input values by path
        let mut builder = StringColumnBuilder::with_capacity(0, 0);
        let path = if let Some(path) = &self.path {
            match parse_json_path(path) {
                Ok(json_path) => {
                    let selector = Selector::new(json_path, SelectorMode::All);
                    selector.select(&self.input, &mut builder.data, &mut builder.offsets);
                    unsafe { String::from_utf8_unchecked(path.to_vec()) }
                }
                Err(_) => {
                    return Err(ErrorCode::BadArguments(format!(
                        "Invalid JSON Path {:?}",
                        String::from_utf8_lossy(path)
                    )));
                }
            }
        } else {
            builder.put_slice(&self.input);
            builder.commit_row();
            "".to_string()
        };
        let inputs = builder.build();

        let mut keys: Vec<Option<Vec<u8>>> = vec![];
        let mut paths: Vec<Option<Vec<u8>>> = vec![];
        let mut indices: Vec<Option<u64>> = vec![];
        let mut values: Vec<Option<Vec<u8>>> = vec![];
        let mut thises: Vec<Option<Vec<u8>>> = vec![];

        for input in inputs.iter() {
            self.flatten(
                input,
                &path,
                &mut keys,
                &mut paths,
                &mut indices,
                &mut values,
                &mut thises,
            );
        }

        if self.outer && values.is_empty() {
            // add an empty row
            keys.push(None);
            paths.push(None);
            indices.push(None);
            values.push(None);
            thises.push(None);
        }

        // TODO: generate sequence number associated with the input record
        let seqs: Vec<u64> = [1].repeat(values.len());

        let block = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(seqs),
            StringType::from_opt_data(keys),
            StringType::from_opt_data(paths),
            UInt64Type::from_opt_data(indices),
            VariantType::from_opt_data(values),
            VariantType::from_opt_data(thises),
        ]);
        Ok(Some(block))
    }
}
