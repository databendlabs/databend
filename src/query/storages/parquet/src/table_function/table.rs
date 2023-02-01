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

use std::sync::Arc;

use common_catalog::plan::ParquetReadOptions;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_function::TableFunction;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Scalar;
use opendal::Operator;

use crate::parquet_table::ParquetTable;

impl ParquetTable {
    /// Create the table function `read_parquet`.
    ///
    /// Syntax:
    ///
    /// ```sql
    /// select * from read_parquet('path1', 'path2', ..., prune_pages=>true, refresh_meta_cache=>true, ...);
    /// ```
    ///
    /// The table function `read_parquet` can only be used on local filesystem.
    pub fn create_table_function(
        _database_name: &str,
        _table_func_name: &str,
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

        let (file_locations, read_options) = parse_parquet_table_args(&table_args)?;

        let table = Self::blocking_create(table_id, operator, file_locations, read_options, None)?;

        Ok(Arc::new(table))
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

/// Parse [`TableArgs`] to get the file locations and [`ParquetReadOptions`].
///
/// Options should always be behind the file locations, or an error will be returned.
///
/// The file locations may be glob patterns.
pub fn parse_parquet_table_args(
    table_args: &TableArgs,
) -> Result<(Vec<String>, ParquetReadOptions)> {
    if table_args.is_none() {
        return Err(ErrorCode::BadArguments(
            "read_parquet needs at least one argument",
        ));
    }

    let args = table_args.as_ref().unwrap();

    // Options should always be behind the file locations.
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
                return Err(ErrorCode::BadArguments("file locations should be string"));
            }
        }
    }

    let read_options = ParquetReadOptions::try_from(&args[path_num..])?;

    Ok((paths, read_options))
}
