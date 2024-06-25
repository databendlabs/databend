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

use std::io::Read;
use std::sync::Arc;

use dashmap::DashMap;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FieldDefaultExpr;
use databend_common_expression::TableSchemaRefExt;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::input_formats::InputContext;
use databend_common_pipeline_sources::input_formats::SplitInfo;
use databend_common_storage::STDIN_FD;
use log::debug;
use opendal::Scheme;

use crate::StageTable;

impl StageTable {
    pub async fn read_partition_old(
        &self,
        ctx: &Arc<dyn TableContext>,
    ) -> Result<(PartStatistics, Partitions)> {
        let thread_num = ctx.get_settings().get_max_threads()? as usize;

        let stage_info = &self.table_info;
        // User set the files.
        let files = if let Some(files) = &stage_info.files_to_copy {
            files.clone()
        } else {
            StageTable::list_files(stage_info, thread_num, None).await?
        };
        let format = InputContext::get_input_format(&stage_info.stage_info.file_format_params)?;
        let operator = StageTable::get_op(&stage_info.stage_info)?;
        let splits = format
            .get_splits(
                files,
                &stage_info.stage_info,
                &operator,
                &ctx.get_settings(),
            )
            .await?;

        let partitions = splits
            .into_iter()
            .map(|v| {
                let part_info: Box<dyn PartInfo> = Box::new((*v).clone());
                Arc::new(part_info)
            })
            .collect::<Vec<_>>();
        Ok((
            PartStatistics::default(),
            Partitions::create(PartitionsShuffleKind::Seq, partitions),
        ))
    }

    pub(crate) fn read_data_old(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        stage_table_info: &StageTableInfo,
    ) -> Result<()> {
        let projection = if let Some(PushDownInfo {
            projection: Some(Projection::Columns(columns)),
            ..
        }) = &plan.push_downs
        {
            Some(columns.clone())
        } else {
            None
        };

        let mut splits = vec![];
        for part in &plan.parts.partitions {
            if let Some(split) = part.as_any().downcast_ref::<SplitInfo>() {
                splits.push(Arc::new(split.clone()));
            }
        }

        //  Build copy pipeline.
        let settings = ctx.get_settings();
        let fields = stage_table_info
            .schema
            .fields()
            .iter()
            .filter(|f| f.computed_expr().is_none())
            .cloned()
            .collect::<Vec<_>>();
        let schema = TableSchemaRefExt::create(fields);
        let stage_info = stage_table_info.stage_info.clone();
        let operator = StageTable::get_op(&stage_table_info.stage_info)?;
        let compact_threshold = ctx.get_read_block_thresholds();
        let on_error_map = ctx.get_on_error_map().unwrap_or_else(|| {
            let m = Arc::new(DashMap::new());
            ctx.set_on_error_map(m.clone());
            m
        });

        // inject stdin to memory
        if operator.info().scheme() == Scheme::Memory {
            let mut buffer = vec![];
            std::io::stdin().lock().read_to_end(&mut buffer)?;

            let bop = operator.blocking();
            bop.write(STDIN_FD, buffer)?;
        }

        let default_values = match &self.table_info.default_values {
            Some(vs) => {
                let mut default_values = vec![];
                for v in vs {
                    match v {
                        FieldDefaultExpr::Const(s) => default_values.push(s.clone()),
                        FieldDefaultExpr::Expr(e) => {
                            // we will remove this impl soon
                            return Err(ErrorCode::Internal(format!(
                                "{} as default value not supported when copy into table",
                                e
                            )));
                        }
                    }
                }
                Some(default_values)
            }
            None => None,
        };

        let input_ctx = Arc::new(InputContext::try_create_from_copy(
            ctx.clone(),
            operator,
            settings,
            schema,
            stage_info,
            splits,
            ctx.get_scan_progress(),
            compact_threshold,
            on_error_map,
            self.table_info.is_select,
            projection,
            default_values,
        )?);
        debug!("start copy splits feeder in {}", ctx.get_cluster().local_id);
        input_ctx.format.exec_copy(input_ctx.clone(), pipeline)?;
        Ok(())
    }
}
