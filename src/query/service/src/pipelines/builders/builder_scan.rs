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
use std::sync::Mutex;
use std::time::Instant;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::OneBlockSource;
use databend_common_pipeline_transforms::processors::ProfileStub;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::ConstantTableScan;
use databend_common_sql::executor::physical_plans::CteScan;
use databend_common_sql::executor::physical_plans::TableScan;

use crate::pipelines::processors::transforms::MaterializedCteSource;
use crate::pipelines::processors::transforms::TransformAddInternalColumns;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_table_scan(&mut self, scan: &TableScan) -> Result<()> {
        let table = self.ctx.build_table_from_source_plan(&scan.source)?;
        self.ctx.set_partitions(scan.source.parts.clone())?;
        table.read_data(
            self.ctx.clone(),
            &scan.source,
            &mut self.main_pipeline,
            true,
        )?;

        // Fill internal columns if needed.
        if let Some(internal_columns) = &scan.internal_column {
            self.main_pipeline.add_transform(|input, output| {
                TransformAddInternalColumns::try_create(input, output, internal_columns.clone())
            })?;
        }

        let schema = scan.source.schema();
        let mut projection = scan
            .name_mapping
            .keys()
            .map(|name| schema.index_of(name.as_str()))
            .collect::<Result<Vec<usize>>>()?;
        projection.sort();

        // if projection is sequential, no need to add projection
        if projection != (0..schema.fields().len()).collect::<Vec<usize>>() {
            let ops = vec![BlockOperator::Project { projection }];
            let num_input_columns = schema.num_fields();
            self.main_pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                    input,
                    output,
                    num_input_columns,
                    self.func_ctx.clone(),
                    ops.clone(),
                )))
            })?;
        }

        Ok(())
    }

    pub(crate) fn build_cte_scan(&mut self, cte_scan: &CteScan) -> Result<()> {
        let max_threads = self.settings.get_max_threads()?;
        self.main_pipeline.add_source(
            |output| {
                MaterializedCteSource::create(
                    self.ctx.clone(),
                    output,
                    cte_scan.cte_idx,
                    self.cte_state.get(&cte_scan.cte_idx.0).unwrap().clone(),
                    cte_scan.offsets.clone(),
                )
            },
            max_threads as usize,
        )
    }

    pub(crate) fn build_constant_table_scan(&mut self, scan: &ConstantTableScan) -> Result<()> {
        self.main_pipeline.add_source(
            |output| {
                let block = if !scan.values.is_empty() {
                    DataBlock::new_from_columns(scan.values.clone())
                } else {
                    DataBlock::new(vec![], scan.num_rows)
                };
                OneBlockSource::create(output, block)
            },
            1,
        )
    }
}
