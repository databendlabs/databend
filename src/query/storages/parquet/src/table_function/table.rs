//  Copyright 2022 Datafuse Labs.
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

use std::any::Any;
use std::sync::Arc;

use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Utc;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read as pread;
use common_base::runtime::GlobalIORuntime;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_function::TableFunction;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Scalar;
use common_expression::TableSchema;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::Pipeline;
use futures::TryStreamExt;
use opendal::raw::get_basename;
use opendal::raw::get_parent;
use opendal::ObjectMode;
use opendal::Operator;
use regex::Regex;

use super::TableContext;
use crate::ReadOptions;

pub struct ParquetTable {
    table_args: Vec<Scalar>,

    pub(super) file_locations: Vec<String>,
    pub(super) table_info: TableInfo,
    pub(super) arrow_schema: ArrowSchema,
    pub(super) operator: Operator,
    pub(super) read_options: ReadOptions,
}

impl ParquetTable {
    /// Create the table function `read_parquet`.
    ///
    /// Syntax:
    ///
    /// ```sql
    /// select * from read_parquet('path1', 'path2', ..., prune_pages=>true, refresh_meta_cache=>true, ...);
    /// ```
    pub fn create_table_function(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        if !GlobalConfig::instance().storage.allow_insecure {
            return Err(ErrorCode::StorageInsecure(
                "Should enable `allow_insecure` to use table function `read_parquet`",
            ));
        }
        let mut builder = opendal::services::fs::Builder::default();
        builder.root("/");
        let operator = Operator::new(builder.build()?);

        Self::create(
            database_name,
            table_func_name,
            table_id,
            table_args,
            operator,
        )
    }

    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
        operator: Operator,
    ) -> Result<Arc<dyn TableFunction>> {
        if !GlobalConfig::instance().storage.allow_insecure {
            return Err(ErrorCode::StorageInsecure(
                "Should enable `allow_insecure` to use table function `read_parquet`",
            ));
        }

        if table_args.is_none() {
            return Err(ErrorCode::BadArguments(
                "read_parquet needs at least one argument",
            ));
        }

        let args = table_args.unwrap();
        let path_num = args
            .iter()
            .position(|arg| matches!(arg, Scalar::Tuple(_)))
            .unwrap_or(args.len());
        if path_num == 0 {
            return Err(ErrorCode::BadArguments(
                "read_parquet needs at least one file path",
            ));
        }

        let mut paths = Vec::with_capacity(path_num);
        for arg in args.iter().take(path_num) {
            match arg {
                Scalar::String(path) => {
                    let path = std::str::from_utf8(path).unwrap();
                    paths.push(path.to_string());
                }
                _ => {
                    return Err(ErrorCode::BadArguments(
                        "read_parquet only accepts string arguments",
                    ));
                }
            }
        }

        let (file_locations, arrow_schema) = Self::prepare_metas(paths, operator.clone())?;

        // Now, `read_options` is hard-coded.
        let read_options = ReadOptions::try_from(&args[path_num..])?;

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: arrow_to_table_schema(arrow_schema.clone()).into(),
                engine: "SystemReadParquet".to_string(),
                created_on: Utc
                    .from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
                updated_on: Utc
                    .from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(ParquetTable {
            table_args: args,
            file_locations,
            table_info,
            arrow_schema,
            operator,
            read_options,
        }))
    }

    fn prepare_metas(paths: Vec<String>, operator: Operator) -> Result<(Vec<String>, ArrowSchema)> {
        let (file_locations, meta) = GlobalIORuntime::instance().block_on(async move {
            let mut file_locations = Vec::with_capacity(paths.len());
            for maybe_glob_path in paths.iter() {
                let files = Self::list_files(maybe_glob_path, &operator).await?;
                file_locations.extend(files);
            }

            if file_locations.is_empty() {
                return Err(ErrorCode::BadArguments(
                    "No matched files found for read_parquet",
                ));
            }

            // Infer schema from the first parquet file.
            // Assume all parquet files have the same schema.
            // If not, throw error during reading.
            let mut reader = operator.object(&file_locations[0]).reader().await?;
            let first_meta = pread::read_metadata_async(&mut reader).await.map_err(|e| {
                ErrorCode::Internal(format!(
                    "Read parquet file '{}''s meta error: {}",
                    &file_locations[0], e
                ))
            })?;

            Ok((file_locations, first_meta))
        })?;

        let arrow_schema = pread::infer_schema(&meta)?;

        Ok((file_locations, arrow_schema))
    }

    /// List files from the given path with pattern.
    ///
    /// Only support simple patterns (one level): `path/to/dir/*.parquet`.
    async fn list_files(maybe_glob_path: &str, operator: &Operator) -> Result<Vec<String>> {
        let basename = get_basename(maybe_glob_path);
        let regex = match Regex::new(basename) {
            Ok(regex) => regex,
            Err(_) => {
                // not a regex format, push the path directly.
                return Ok(vec![maybe_glob_path.to_string()]);
            }
        };

        let obj = operator.object(get_parent(maybe_glob_path));
        let mut files = Vec::new();
        let mut list = obj.list().await?;
        while let Some(de) = list.try_next().await? {
            match de.mode().await? {
                ObjectMode::FILE => {
                    if regex.is_match(de.name()) {
                        files.push(de.path().to_string());
                    }
                }
                ObjectMode::DIR => {
                    return Err(ErrorCode::BadArguments(
                        "read_parquet only support simple patterns (example: `path/to/dir/*.parquet`)",
                    ));
                }
                ObjectMode::Unknown => continue,
            }
        }

        Ok(files)
    }
}

#[async_trait::async_trait]
impl Table for ParquetTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn benefit_column_prune(&self) -> bool {
        true
    }

    fn support_prewhere(&self) -> bool {
        self.read_options.do_prewhere()
    }

    fn has_exact_total_row_count(&self) -> bool {
        true
    }

    fn table_args(&self) -> Option<Vec<Scalar>> {
        Some(self.table_args.clone())
    }

    /// The returned partitions only record the locations of files to read.
    /// So they don't have any real statistics.
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(ctx, push_down).await
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.do_read_data(ctx, plan, pipeline)
    }
}

impl TableFunction for ParquetTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

fn lower_field_name(field: &mut ArrowField) {
    field.name = field.name.to_lowercase();
    match &mut field.data_type {
        ArrowDataType::List(f)
        | ArrowDataType::LargeList(f)
        | ArrowDataType::FixedSizeList(f, _) => {
            lower_field_name(f.as_mut());
        }
        ArrowDataType::Struct(ref mut fields) => {
            for f in fields {
                lower_field_name(f);
            }
        }
        _ => {}
    }
}

pub fn arrow_to_table_schema(mut schema: ArrowSchema) -> TableSchema {
    schema.fields.iter_mut().for_each(|f| {
        lower_field_name(f);
    });
    TableSchema::from(&schema)
}
