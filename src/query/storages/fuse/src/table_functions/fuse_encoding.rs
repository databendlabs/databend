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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::string_value;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::expr::*;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_native::read::reader::NativeReader;
use databend_common_native::stat::ColumnInfo;
use databend_common_native::stat::PageBody;
use databend_common_native::stat::stat_simple;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use parquet::basic::Compression as ParquetCompression;
use parquet::basic::Encoding as ParquetEncoding;
use parquet::format::Type as ParquetPhysicalType;

use crate::BlockReadResult;
use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::io::read::meta::read_thrift_file_metadata;
use crate::sessions::TableContext;
use crate::table_functions::SimpleArgFunc;
use crate::table_functions::SimpleArgFuncTemplate;
use crate::table_functions::string_literal;

pub struct FuseEncodingArgs {
    database_name: String,
    table_name: Option<String>,
    column_name: Option<String>,
}

impl TryFrom<(&str, TableArgs)> for FuseEncodingArgs {
    type Error = ErrorCode;
    fn try_from(
        (func_name, table_args): (&str, TableArgs),
    ) -> std::result::Result<Self, Self::Error> {
        let args = table_args.expect_all_positioned(func_name, None)?;
        if args.is_empty() || args.len() > 3 {
            return Err(ErrorCode::BadArguments(format!(
                "{} must accept between 1 and 3 string literals",
                func_name
            )));
        }
        let database_name = string_value(&args[0])?;
        let table_name = if args.len() > 1 {
            Some(string_value(&args[1])?)
        } else {
            None
        };
        let column_name = if args.len() > 2 {
            Some(string_value(&args[2])?)
        } else {
            None
        };
        Ok(Self {
            database_name,
            table_name,
            column_name,
        })
    }
}

impl From<&FuseEncodingArgs> for TableArgs {
    fn from(args: &FuseEncodingArgs) -> Self {
        let mut positional = vec![string_literal(args.database_name.as_str())];
        if let Some(table_name) = &args.table_name {
            positional.push(string_literal(table_name.as_str()));
        }
        if let Some(column_name) = &args.column_name {
            positional.push(string_literal(column_name.as_str()));
        }
        TableArgs::new_positioned(positional)
    }
}

pub type FuseEncodingFunc = SimpleArgFuncTemplate<FuseEncoding>;
pub struct FuseEncoding;

#[async_trait::async_trait]
impl SimpleArgFunc for FuseEncoding {
    type Args = FuseEncodingArgs;

    fn schema() -> TableSchemaRef {
        FuseEncodingImpl::schema()
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        args: &Self::Args,
        plan: &DataSourcePlan,
    ) -> Result<DataBlock> {
        let tenant_id = ctx.get_tenant();
        let tbls = ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_database(&tenant_id, args.database_name.as_str())
            .await?
            .list_tables()
            .await?;

        let fuse_tables = tbls
            .iter()
            .map(|tbl| {
                let tbl = FuseTable::try_from_table(tbl.as_ref()).unwrap();
                tbl
            })
            .collect::<Vec<_>>();

        let filters = plan.push_downs.as_ref().and_then(|x| x.filters.clone());
        FuseEncodingImpl::new(
            ctx.clone(),
            fuse_tables,
            filters,
            args.table_name.clone(),
            args.column_name.clone(),
        )
        .get_blocks()
        .await
    }
}

pub struct FuseEncodingImpl<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub tables: Vec<&'a FuseTable>,
    pub filters: Option<Filters>,
    pub table_name_filter: Option<String>,
    pub column_name_filter: Option<String>,
}

struct EncodingRow {
    table_name: String,
    storage_format: FuseStorageFormat,
    block_location: String,
    column_name: String,
    column_type: String,
    validity_size: Option<u32>,
    compressed_size: u64,
    uncompressed_size: u64,
    level_one: String,
    level_two: Option<String>,
}

impl<'a> FuseEncodingImpl<'a> {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        tables: Vec<&'a FuseTable>,
        filters: Option<Filters>,
        table_name_filter: Option<String>,
        column_name_filter: Option<String>,
    ) -> Self {
        Self {
            ctx,
            tables,
            filters,
            table_name_filter,
            column_name_filter,
        }
    }

    #[async_backtrace::framed]
    pub async fn get_blocks(&self) -> Result<DataBlock> {
        let mut rows = Vec::new();
        for table in self.tables.clone() {
            if !self.table_matches(table.name()) {
                continue;
            }
            let Some(snapshot) = table.read_table_snapshot().await? else {
                continue;
            };

            let segments_io =
                SegmentsIO::create(self.ctx.clone(), table.operator.clone(), table.schema());
            let chunk_size = self.ctx.get_settings().get_max_threads()? as usize * 4;

            match table.storage_format {
                FuseStorageFormat::Native => {
                    self.collect_native_rows(
                        table,
                        snapshot.as_ref(),
                        &segments_io,
                        chunk_size,
                        &mut rows,
                    )
                    .await?;
                }
                FuseStorageFormat::Parquet => {
                    self.collect_parquet_rows(
                        table,
                        snapshot.as_ref(),
                        &segments_io,
                        chunk_size,
                        &mut rows,
                    )
                    .await?;
                }
            }
        }

        let data_block = self.to_block(&rows).await?;
        let result = if let Some(filter) = self.filters.as_ref().map(|f| &f.filter) {
            let func_ctx = FunctionContext::default();
            let evaluator = Evaluator::new(&data_block, &func_ctx, &BUILTIN_FUNCTIONS);
            let filter = evaluator
                .run(&as_expr(
                    filter,
                    &BUILTIN_FUNCTIONS,
                    &FuseEncodingImpl::schema(),
                ))?
                .try_downcast::<BooleanType>()
                .unwrap();
            data_block.filter_boolean_value(&filter)?
        } else {
            data_block
        };

        Ok(result)
    }

    #[async_backtrace::framed]
    async fn to_block(&self, rows: &[EncodingRow]) -> Result<DataBlock> {
        let num_rows = rows.len();
        let mut validity_size: Vec<Option<u32>> = Vec::with_capacity(num_rows);
        let mut compressed_size: Vec<u64> = Vec::with_capacity(num_rows);
        let mut uncompressed_size: Vec<u64> = Vec::with_capacity(num_rows);
        let mut l1 = StringColumnBuilder::with_capacity(num_rows);
        let mut l2 = NullableColumnBuilder::<StringType>::with_capacity(num_rows, &[]);
        let mut table_name = StringColumnBuilder::with_capacity(num_rows);
        let mut storage_format = StringColumnBuilder::with_capacity(num_rows);
        let mut block_location = StringColumnBuilder::with_capacity(num_rows);
        let mut column_name = StringColumnBuilder::with_capacity(num_rows);
        let mut column_type = StringColumnBuilder::with_capacity(num_rows);
        for row in rows {
            table_name.put_and_commit(&row.table_name);
            storage_format.put_and_commit(row.storage_format.to_string());
            block_location.put_and_commit(&row.block_location);
            column_name.put_and_commit(&row.column_name);
            column_type.put_and_commit(&row.column_type);
            validity_size.push(row.validity_size);
            compressed_size.push(row.compressed_size);
            uncompressed_size.push(row.uncompressed_size);
            l1.put_and_commit(&row.level_one);
            if let Some(level_two) = &row.level_two {
                l2.push(level_two);
            } else {
                l2.push_null();
            }
        }

        Ok(DataBlock::new(
            vec![
                Column::String(table_name.build()).into(),
                Column::String(storage_format.build()).into(),
                Column::String(block_location.build()).into(),
                Column::String(column_name.build()).into(),
                Column::String(column_type.build()).into(),
                UInt32Type::from_opt_data(validity_size).into(),
                UInt64Type::from_data(compressed_size).into(),
                UInt64Type::from_data(uncompressed_size).into(),
                Column::String(l1.build()).into(),
                Column::Nullable(Box::new(l2.build().upcast())).into(),
            ],
            num_rows,
        ))
    }

    #[async_backtrace::framed]
    async fn collect_native_rows(
        &self,
        table: &'a FuseTable,
        snapshot: &TableSnapshot,
        segments_io: &SegmentsIO,
        chunk_size: usize,
        rows: &mut Vec<EncodingRow>,
    ) -> Result<()> {
        let schema = table.schema();
        let fields = schema.fields();
        for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, false)
                .await?;
            for segment in segments {
                let segment = segment?;
                for block in segment.blocks.iter() {
                    for field in fields {
                        if field.is_nested() {
                            continue;
                        }
                        if !self.column_matches(field.name()) {
                            continue;
                        }
                        let column_id = field.column_id;
                        let Some(column_meta) = block.col_metas.get(&column_id) else {
                            continue;
                        };
                        let (offset, len) = column_meta.offset_length();
                        let ranges = vec![(column_id, offset..(offset + len))];
                        let read_settings = ReadSettings::from_ctx(&self.ctx)?;
                        let merge_io_result = MergeIOReader::merge_io_read(
                            &read_settings,
                            table.operator.clone(),
                            &block.location.0,
                            &ranges,
                        )
                        .await?;

                        let block_read_res =
                            BlockReadResult::create(merge_io_result, vec![], vec![]);
                        let column_chunks = block_read_res.columns_chunks()?;
                        let pages = column_chunks
                            .get(&column_id)
                            .unwrap()
                            .as_raw_data()
                            .unwrap()
                            .to_bytes();
                        let pages = std::io::Cursor::new(pages);
                        let page_metas = column_meta.as_native().unwrap().pages.clone();
                        let reader = NativeReader::new(pages, page_metas, vec![]);
                        let column_info = stat_simple(reader, field.clone())?;
                        self.push_native_column_rows(
                            table.name(),
                            table.storage_format,
                            &block.location.0,
                            field,
                            column_info,
                            rows,
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn collect_parquet_rows(
        &self,
        table: &'a FuseTable,
        snapshot: &TableSnapshot,
        segments_io: &SegmentsIO,
        chunk_size: usize,
        rows: &mut Vec<EncodingRow>,
    ) -> Result<()> {
        let schema = table.schema();
        let fields = schema.fields();
        let column_id_to_index: HashMap<ColumnId, usize> = schema
            .to_leaf_column_ids()
            .into_iter()
            .enumerate()
            .map(|(idx, id)| (id, idx))
            .collect();

        for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, false)
                .await?;
            for segment in segments {
                let segment = segment?;
                for block in segment.blocks.iter() {
                    let file_meta = read_thrift_file_metadata(
                        table.operator.clone(),
                        &block.location.0,
                        Some(block.file_size),
                    )
                    .await?;
                    if file_meta.row_groups.len() != 1 {
                        return Err(ErrorCode::ParquetFileInvalid(format!(
                            "invalid parquet file {}, expects one row group but got {}",
                            block.location.0,
                            file_meta.row_groups.len()
                        )));
                    }
                    let row_group = &file_meta.row_groups[0];
                    let columns = &row_group.columns;

                    for field in fields {
                        if field.is_nested() {
                            continue;
                        }
                        if !self.column_matches(field.name()) {
                            continue;
                        }
                        let column_id = field.column_id;
                        let Some(column_idx) = column_id_to_index.get(&column_id) else {
                            continue;
                        };
                        let Some(column_chunk) = columns.get(*column_idx) else {
                            return Err(ErrorCode::ParquetFileInvalid(format!(
                                "invalid parquet file {}, column index {} is missing",
                                block.location.0, column_idx
                            )));
                        };
                        let chunk_meta = column_chunk.meta_data.as_ref().ok_or_else(|| {
                            ErrorCode::ParquetFileInvalid(format!(
                                "invalid parquet file {}, meta data of column {} is empty",
                                block.location.0, column_id
                            ))
                        })?;

                        let compressed_size = u64::try_from(chunk_meta.total_compressed_size)
                            .map_err(|_| {
                                ErrorCode::ParquetFileInvalid(format!(
                                "invalid parquet file {}, compressed size overflow for column {}",
                                block.location.0,
                                field.name()
                            ))
                            })?;
                        let uncompressed_size = u64::try_from(chunk_meta.total_uncompressed_size)
                            .map_err(|_| {
                            ErrorCode::ParquetFileInvalid(format!(
                                "invalid parquet file {}, uncompressed size overflow for column {}",
                                block.location.0,
                                field.name()
                            ))
                        })?;

                        let physical_type = parquet_physical_type_to_string(chunk_meta.type_);
                        rows.push(EncodingRow {
                            table_name: table.name().to_string(),
                            storage_format: table.storage_format,
                            block_location: block.location.0.clone(),
                            column_name: field.name().to_string(),
                            column_type: format!(
                                "{} ({})",
                                field.data_type().sql_name(),
                                physical_type
                            ),
                            validity_size: None,
                            compressed_size,
                            uncompressed_size,
                            level_one: parquet_encodings_to_string(&chunk_meta.encodings),
                            level_two: Some(parquet_codec_to_string(chunk_meta.codec)),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn push_native_column_rows(
        &self,
        table_name: &str,
        storage_format: FuseStorageFormat,
        block_location: &str,
        field: &TableField,
        column_info: ColumnInfo,
        rows: &mut Vec<EncodingRow>,
    ) {
        let column_name = field.name().clone();
        let column_type = field.data_type().sql_name();
        let block_location = block_location.to_string();
        for page in column_info.pages {
            rows.push(EncodingRow {
                table_name: table_name.to_string(),
                storage_format,
                block_location: block_location.clone(),
                column_name: column_name.clone(),
                column_type: column_type.clone(),
                validity_size: page.validity_size,
                compressed_size: page.compressed_size as u64,
                uncompressed_size: page.uncompressed_size as u64,
                level_one: encoding_to_string(&page.body),
                level_two: native_level_two_encoding(&page.body),
            });
        }
    }

    fn table_matches(&self, table_name: &str) -> bool {
        match &self.table_name_filter {
            Some(filter) => filter == table_name,
            None => true,
        }
    }

    fn column_matches(&self, column_name: &str) -> bool {
        match &self.column_name_filter {
            Some(filter) => filter == column_name,
            None => true,
        }
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("table_name", TableDataType::String),
            TableField::new("storage_format", TableDataType::String),
            TableField::new("block_location", TableDataType::String),
            TableField::new("column_name", TableDataType::String),
            TableField::new("column_type", TableDataType::String),
            TableField::new(
                "validity_size",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt32))),
            ),
            TableField::new(
                "compressed_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "uncompressed_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("level_one", TableDataType::String),
            TableField::new(
                "level_two",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ])
    }
}

fn encoding_to_string(page_body: &PageBody) -> String {
    match page_body {
        PageBody::Dict(_) => "Dict".to_string(),
        PageBody::Freq(_) => "Freq".to_string(),
        PageBody::OneValue => "OneValue".to_string(),
        PageBody::Rle => "Rle".to_string(),
        PageBody::Patas => "Patas".to_string(),
        PageBody::Bitpack => "Bitpack".to_string(),
        PageBody::DeltaBitpack => "DeltaBitpack".to_string(),
        PageBody::Common(c) => format!("Common({:?})", c),
    }
}

fn native_level_two_encoding(page_body: &PageBody) -> Option<String> {
    match page_body {
        PageBody::Dict(dict) => Some(encoding_to_string(&dict.indices.body)),
        PageBody::Freq(freq) => freq
            .exceptions
            .as_ref()
            .map(|e| encoding_to_string(&e.body)),
        _ => None,
    }
}

fn parquet_encodings_to_string(encodings: &[parquet::format::Encoding]) -> String {
    if encodings.is_empty() {
        "unknown".to_string()
    } else {
        encodings
            .iter()
            .map(|encoding| parquet_encoding_to_string(*encoding))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn parquet_encoding_to_string(encoding: parquet::format::Encoding) -> &'static str {
    match ParquetEncoding::try_from(encoding) {
        Ok(ParquetEncoding::PLAIN) => "plain",
        Ok(ParquetEncoding::PLAIN_DICTIONARY) => "plain_dictionary",
        Ok(ParquetEncoding::RLE) => "rle",
        #[allow(deprecated)]
        Ok(ParquetEncoding::BIT_PACKED) => "bit_packed",
        Ok(ParquetEncoding::DELTA_BINARY_PACKED) => "delta_binary_packed",
        Ok(ParquetEncoding::DELTA_LENGTH_BYTE_ARRAY) => "delta_length_byte_array",
        Ok(ParquetEncoding::DELTA_BYTE_ARRAY) => "delta_byte_array",
        Ok(ParquetEncoding::RLE_DICTIONARY) => "rle_dictionary",
        Ok(ParquetEncoding::BYTE_STREAM_SPLIT) => "byte_stream_split",
        Err(_) => "unknown",
    }
}

fn parquet_codec_to_string(codec: parquet::format::CompressionCodec) -> String {
    match ParquetCompression::try_from(codec) {
        Ok(ParquetCompression::UNCOMPRESSED) => "uncompressed".to_string(),
        Ok(ParquetCompression::SNAPPY) => "snappy".to_string(),
        Ok(ParquetCompression::GZIP(_)) => "gzip".to_string(),
        Ok(ParquetCompression::LZO) => "lzo".to_string(),
        Ok(ParquetCompression::BROTLI(_)) => "brotli".to_string(),
        Ok(ParquetCompression::LZ4) => "lz4".to_string(),
        Ok(ParquetCompression::ZSTD(_)) => "zstd".to_string(),
        Ok(ParquetCompression::LZ4_RAW) => "lz4_raw".to_string(),
        Err(_) => format!("compression_codec({})", codec.0),
    }
}

fn parquet_physical_type_to_string(ty: ParquetPhysicalType) -> &'static str {
    match ty {
        ParquetPhysicalType::BOOLEAN => "BOOLEAN",
        ParquetPhysicalType::INT32 => "INT32",
        ParquetPhysicalType::INT64 => "INT64",
        ParquetPhysicalType::INT96 => "INT96",
        ParquetPhysicalType::FLOAT => "FLOAT",
        ParquetPhysicalType::DOUBLE => "DOUBLE",
        ParquetPhysicalType::BYTE_ARRAY => "BYTE_ARRAY",
        ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY => "FIXED_LEN_BYTE_ARRAY",
        parquet::format::Type(_) => "UNKNOWN",
    }
}

pub fn as_expr(
    remote_expr: &RemoteExpr<String>,
    fn_registry: &FunctionRegistry,
    schema: &Arc<TableSchema>,
) -> Expr {
    match remote_expr {
        RemoteExpr::Constant {
            span,
            scalar,
            data_type,
        } => Expr::Constant(Constant {
            span: *span,
            scalar: scalar.clone(),
            data_type: data_type.clone(),
        }),
        RemoteExpr::ColumnRef {
            span,
            id,
            data_type,
            display_name,
        } => {
            let id = schema.index_of(id).unwrap();
            ColumnRef {
                span: *span,
                id,
                data_type: data_type.clone(),
                display_name: display_name.clone(),
            }
            .into()
        }
        RemoteExpr::Cast {
            span,
            is_try,
            expr,
            dest_type,
        } => Cast {
            span: *span,
            is_try: *is_try,
            expr: Box::new(as_expr(expr, fn_registry, schema)),
            dest_type: dest_type.clone(),
        }
        .into(),
        RemoteExpr::FunctionCall {
            span,
            id,
            generics,
            args,
            return_type,
        } => {
            let function = fn_registry.get(id).expect("function id not found");
            FunctionCall {
                span: *span,
                id: id.clone(),
                function,
                generics: generics.clone(),
                args: args
                    .iter()
                    .map(|arg| as_expr(arg, fn_registry, schema))
                    .collect(),
                return_type: return_type.clone(),
            }
            .into()
        }
        RemoteExpr::LambdaFunctionCall {
            span,
            name,
            args,
            lambda_expr,
            lambda_display,
            return_type,
        } => {
            let args = args
                .iter()
                .map(|arg| as_expr(arg, fn_registry, schema))
                .collect();
            LambdaFunctionCall {
                span: *span,
                name: name.clone(),
                args,
                lambda_expr: lambda_expr.clone(),
                lambda_display: lambda_display.clone(),
                return_type: return_type.clone(),
            }
            .into()
        }
    }
}
