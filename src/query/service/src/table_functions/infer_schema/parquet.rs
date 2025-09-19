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

use std::sync::Arc;

use arrow_schema::Schema;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableSchema;
use databend_common_meta_app::principal::StageInfo;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_storage::init_stage_operator;
use databend_common_storage::read_parquet_schema_async_rs;
use databend_common_storage::StageFileInfo;
use futures_util::future::try_join_all;
use itertools::Itertools;

use crate::table_functions::infer_schema::infer_schema_table::INFER_SCHEMA;

pub(crate) struct ParquetInferSchemaSource {
    is_finished: bool,

    stage_info: StageInfo,
    stage_file_infos: Vec<StageFileInfo>,
}

impl ParquetInferSchemaSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        stage_info: StageInfo,
        stage_file_infos: Vec<StageFileInfo>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, output, ParquetInferSchemaSource {
            is_finished: false,
            stage_info,
            stage_file_infos,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for ParquetInferSchemaSource {
    const NAME: &'static str = INFER_SCHEMA;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }
        self.is_finished = true;

        let operator = init_stage_operator(&self.stage_info)?;
        let infer_schema_futures = self.stage_file_infos.iter().map(|file| async {
            read_parquet_schema_async_rs(&operator, &file.path, Some(file.size)).await
        });
        // todo: unify_schemas(arrow-rs unsupported now)
        let arrow_schema = Schema::try_merge(try_join_all(infer_schema_futures).await?)?;
        let table_schema = TableSchema::try_from(&arrow_schema)?;

        let mut names: Vec<String> = vec![];
        let mut types: Vec<String> = vec![];
        let mut nulls: Vec<bool> = vec![];
        let mut filenames: Vec<String> = vec![];
        let filenames_str = self
            .stage_file_infos
            .iter()
            .map(|info| &info.path)
            .join(", ");

        for field in table_schema.fields().iter() {
            names.push(field.name().to_string());

            let non_null_type = field.data_type().remove_recursive_nullable();
            types.push(non_null_type.sql_name());
            nulls.push(field.is_nullable());
            filenames.push(filenames_str.clone());
        }

        let order_ids = (0..table_schema.fields().len() as u64).collect::<Vec<_>>();

        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(types),
            BooleanType::from_data(nulls),
            StringType::from_data(filenames),
            UInt64Type::from_data(order_ids),
        ]);
        Ok(Some(block))
    }
}
