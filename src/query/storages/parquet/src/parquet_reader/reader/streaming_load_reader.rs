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

use bytes::Bytes;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteDefaultExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::expr::*;
use databend_common_meta_app::principal::NullAs;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_storage::parquet::infer_schema_with_extension;
use databend_storages_common_stage::project_columnar;
use opendal::Operator;
use opendal::services::Memory;
use parquet::file::metadata::ParquetMetaDataReader;

use crate::ParquetSourceType;
use crate::copy_into_table::CopyProjectionEvaluator;
use crate::parquet_reader::DataBlockIterator;
use crate::parquet_reader::ParquetReaderBuilder;
use crate::schema::arrow_to_table_schema;

pub struct InmMemoryFile {
    file_data: Bytes,
    location: String,
}

impl InmMemoryFile {
    pub fn new(location: String, file_data: Bytes) -> Self {
        Self {
            location,
            file_data,
        }
    }
}

impl InmMemoryFile {
    pub fn read(
        &self,
        ctx: Arc<dyn TableContext>,
        output_schema: TableSchemaRef,
        default_exprs: Option<Vec<RemoteDefaultExpr>>,
        missing_as: &NullAs,
        case_sensitive: bool,
        func_ctx: Arc<FunctionContext>,
        data_schema: DataSchemaRef,
        use_logic_type: bool,
    ) -> Result<DataBlockIterator> {
        let parquet_meta_data = ParquetMetaDataReader::new().parse_and_finish(&self.file_data)?;
        parquet_meta_data.file_metadata();
        let arrow_schema = infer_schema_with_extension(parquet_meta_data.file_metadata())?;
        let schema_descr = parquet_meta_data.file_metadata().schema_descr_ptr();
        let parquet_table_schema = Arc::new(arrow_to_table_schema(
            &arrow_schema,
            case_sensitive,
            use_logic_type,
        )?);

        let (mut output_projection, mut pushdown_columns) = project_columnar(
            &parquet_table_schema,
            &output_schema,
            missing_as,
            &default_exprs,
            &self.location,
            case_sensitive,
            StageFileFormatType::Parquet,
        )?;
        pushdown_columns.sort();
        let mapping = pushdown_columns
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, pos)| (pos, i))
            .collect::<HashMap<_, _>>();
        for expr in output_projection.iter_mut() {
            match expr {
                Expr::ColumnRef(ColumnRef { id, .. }) => *id = mapping[id],
                Expr::Cast(Cast {
                    expr: box Expr::ColumnRef(ColumnRef { id, .. }),
                    ..
                }) => *id = mapping[id],
                _ => {}
            }
        }
        let pushdowns = PushDownInfo {
            projection: Some(Projection::Columns(pushdown_columns)),
            ..Default::default()
        };

        let op: Operator = Operator::new(Memory::default())?.finish();
        let mut reader_builder = ParquetReaderBuilder::create_with_parquet_schema(
            ctx,
            Arc::new(op),
            parquet_table_schema,
            schema_descr,
            Some(arrow_schema),
            None,
        )
        .with_push_downs(Some(&pushdowns));
        reader_builder.build_output()?;
        let project_eval = CopyProjectionEvaluator::new(data_schema, func_ctx);

        let whole_file_reader =
            reader_builder.build_full_reader(ParquetSourceType::StreamingLoad, false)?;
        let it =
            whole_file_reader.read_blocks_from_binary(self.file_data.clone(), &self.location)?;
        Ok(Box::new(it.into_iter().map(move |result_block| {
            result_block.map(|block| project_eval.project(&block, &output_projection).unwrap())
        })))
    }
}
