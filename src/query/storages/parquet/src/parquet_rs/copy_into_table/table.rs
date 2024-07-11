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

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_pipeline_core::Pipeline;
use databend_common_storage::init_stage_operator;
use databend_common_storage::FileStatus;
use parquet::file::metadata::FileMetaData;

use crate::parquet_rs::copy_into_table::reader::RowGroupReaderForCopy;
use crate::parquet_rs::copy_into_table::source::ParquetCopySource;
use crate::parquet_rs::read_metas_in_parallel_for_copy;
use crate::ParquetPart;
use crate::ParquetRSRowGroupPart;

pub struct ParquetTableForCopy {}

impl ParquetTableForCopy {
    #[async_backtrace::framed]
    pub async fn do_read_partitions(
        stage_table_info: &StageTableInfo,
        ctx: Arc<dyn TableContext>,
        _push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let settings = ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;
        let max_memory_usage = settings.get_max_memory_usage()?;

        let operator = init_stage_operator(&stage_table_info.stage_info)?;
        // User set the files.
        let files = stage_table_info.files_to_copy.as_ref().expect(
            "ParquetTableForCopy::do_read_partitions must be called with files_to_copy set",
        );
        let file_infos = files
            .iter()
            .map(|f| (f.path.clone(), f.size))
            .collect::<Vec<_>>();
        let total_size = file_infos.iter().map(|(_, size)| *size as usize).sum();
        let metas =
            read_metas_in_parallel_for_copy(&operator, &file_infos, max_threads, max_memory_usage)
                .await?;
        let mut schemas = vec![];
        let mut parts = vec![];
        let copy_status = ctx.get_copy_status();
        let mut stats = PartStatistics::default();
        for meta in metas.iter() {
            let schema = meta.meta.file_metadata().schema_descr_ptr();
            let schema_index = match schemas.iter().position(|s| s == &schema) {
                Some(i) => i,
                None => {
                    schemas.push(schema);
                    schemas.len() - 1
                }
            };
            let num_rows = meta.meta.file_metadata().num_rows() as usize;
            stats.read_rows += num_rows;
            copy_status.add_chunk(meta.location.as_str(), FileStatus {
                num_rows_loaded: num_rows,
                error: None,
            });
            for rg in meta.meta.row_groups() {
                let part = ParquetRSRowGroupPart {
                    location: meta.location.clone(),
                    meta: rg.clone(),
                    schema_index,
                    uncompressed_size: rg.total_byte_size() as u64,
                    compressed_size: rg.compressed_size() as u64,
                    sort_min_max: None,
                    omit_filter: false,
                    page_locations: None,
                    selectors: None,
                };
                parts.push(part);
            }
        }
        let parts: Vec<_> = parts
            .into_iter()
            .map(|p| Arc::new(Box::new(ParquetPart::ParquetRSRowGroup(p)) as Box<dyn PartInfo>))
            .collect();

        stats.partitions_scanned = parts.len();
        stats.partitions_total = parts.len();
        stats.read_bytes = total_size;

        Ok((stats, Partitions::create(PartitionsShuffleKind::Mod, parts)))
    }

    pub fn do_read_data(
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        let settings = ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;
        let stage_table_info =
            if let DataSourceInfo::StageSource(stage_table_info) = &plan.source_info {
                stage_table_info
            } else {
                return Err(ErrorCode::Internal(
                    "bug: ParquetTableForCopy::read_data must be called with StageSource",
                ));
            };

        let fmt = match &stage_table_info.stage_info.file_format_params {
            FileFormatParams::Parquet(fmt) => fmt,
            _ => unreachable!("do_read_partitions expect parquet"),
        };

        let operator = init_stage_operator(&stage_table_info.stage_info)?;

        let mut readers = HashMap::new();

        for part in &plan.parts.partitions {
            let part = part.as_any().downcast_ref::<ParquetPart>().unwrap();
            match part {
                ParquetPart::ParquetRSRowGroup(part) => {
                    if readers.get(&part.schema_index).is_none() {
                        // TODO: temporary solution, need at least key_value_metadata.
                        let file_meta_data =
                            FileMetaData::new(0, 0, None, None, part.meta.schema_descr_ptr(), None);
                        readers.insert(
                            part.schema_index,
                            RowGroupReaderForCopy::try_create(
                                &part.location,
                                ctx.clone(),
                                operator.clone(),
                                &file_meta_data,
                                stage_table_info.schema.clone(),
                                stage_table_info.default_values.clone(),
                                &fmt.missing_field_as,
                            )?,
                        );
                    }
                }
                _ => unreachable!(),
            }
        }
        let readers = Arc::new(readers);
        ctx.set_partitions(plan.parts.clone())?;

        let data_schema = Arc::new(DataSchema::from(&stage_table_info.schema));
        pipeline.add_source(
            |output| {
                ParquetCopySource::try_create(
                    ctx.clone(),
                    output,
                    readers.clone(),
                    operator.clone(),
                    data_schema.clone(),
                )
            },
            max_threads,
        )?;
        Ok(())
    }
}
