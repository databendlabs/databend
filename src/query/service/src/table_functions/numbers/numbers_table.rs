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
use std::mem::size_of;
use std::sync::Arc;

use chrono::DateTime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_core::SourcePipeBuilder;
use databend_common_pipeline_sources::EmptySource;
use databend_common_pipeline_sources::SyncSource;
use databend_common_pipeline_sources::SyncSourcer;
use databend_storages_common_table_meta::table::ChangeType;

use super::numbers_part::generate_numbers_parts;
use super::NumbersPartInfo;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::ProcessorPtr;
use crate::sessions::TableContext;
use crate::storages::Table;
use crate::table_functions::TableFunction;

pub struct NumbersTable {
    table_info: TableInfo,
    total: u64,
}

impl NumbersTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = table_args.expect_all_positioned(table_func_name, Some(1))?;
        let total = check_number::<_, u64>(
            None,
            &FunctionContext::default(),
            &Expr::<usize>::Constant {
                span: None,
                scalar: args[0].clone(),
                data_type: args[0].as_ref().infer_data_type(),
            },
            &BUILTIN_FUNCTIONS,
        )?;
        let engine = match table_func_name {
            "numbers" => "SystemNumbers",
            "numbers_mt" => "SystemNumbersMt",
            "numbers_local" => "SystemNumbersLocal",
            _ => unreachable!(),
        };

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: TableSchemaRefExt::create(vec![TableField::new(
                    "number",
                    TableDataType::Number(NumberDataType::UInt64),
                )]),
                engine: engine.to_string(),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(NumbersTable { table_info, total }))
    }
}

#[async_trait::async_trait]
impl Table for NumbersTable {
    fn is_local(&self) -> bool {
        self.name() == "numbers_local"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let max_block_size = ctx.get_settings().get_max_block_size()?;
        let mut limit = None;

        if let Some(extras) = &push_downs {
            if extras.limit.is_some() && extras.filters.is_none() && extras.order_by.is_empty() {
                // It is allowed to have an error when we can't get sort columns from the expression. For
                // example 'select number from numbers(10) order by number+4 limit 10', the column 'number+4'
                // doesn't exist in the numbers table.
                // For case like that, we ignore the error and don't apply any optimization.

                // No order by case
                limit = extras.limit;
            }
        }
        let total = match limit {
            Some(limit) => std::cmp::min(self.total, limit as u64),
            None => self.total,
        };

        let fake_partitions = (total / max_block_size) + 1;
        let statistics = PartStatistics::new_exact(
            total as usize,
            ((total) * size_of::<u64>() as u64) as usize,
            fake_partitions as usize,
            fake_partitions as usize,
        );

        let cluster = ctx.get_cluster();
        let mut worker_num = ctx.get_settings().get_max_threads()?;

        worker_num = match worker_num > fake_partitions {
            true => fake_partitions,
            false => worker_num * cluster.nodes.len() as u64,
        };

        let parts = generate_numbers_parts(0, worker_num, total);
        Ok((statistics, parts))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(TableArgs::new_positioned(vec![Scalar::Number(
            NumberScalar::UInt64(self.total),
        )]))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        if plan.parts.partitions.is_empty() {
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        }

        let mut source_builder = SourcePipeBuilder::create();

        for part_index in 0..plan.parts.len() {
            let source_ctx = ctx.clone();
            let source_output_port = OutputPort::create();

            source_builder.add_source(
                source_output_port.clone(),
                NumbersSource::create(
                    source_output_port,
                    source_ctx,
                    &plan.parts.partitions[part_index],
                )?,
            );
        }

        pipeline.add_pipe(source_builder.finalize());
        Ok(())
    }

    async fn table_statistics(
        &self,
        _ctx: Arc<dyn TableContext>,
        _change_type: Option<ChangeType>,
    ) -> Result<Option<TableStatistics>> {
        Ok(Some(TableStatistics {
            num_rows: Some(self.total),
            data_size: Some(self.total * 8),
            data_size_compressed: None,
            index_size: None,
            number_of_blocks: None,
            number_of_segments: None,
        }))
    }
}

struct NumbersSource {
    begin: u64,
    end: u64,
    step: u64,
}

impl NumbersSource {
    pub fn create(
        output: Arc<OutputPort>,
        ctx: Arc<dyn TableContext>,
        numbers_part: &PartInfoPtr,
    ) -> Result<ProcessorPtr> {
        let settings = ctx.get_settings();
        let numbers_part = NumbersPartInfo::from_part(numbers_part)?;

        SyncSourcer::create(ctx, output, NumbersSource {
            begin: numbers_part.part_start,
            end: numbers_part.part_end,
            step: settings.get_max_block_size()?,
        })
    }
}

impl SyncSource for NumbersSource {
    const NAME: &'static str = "NumbersSourceTransform";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        let source_remain_size = self.end - self.begin;

        match source_remain_size {
            0 => Ok(None),
            remain_size => {
                let step = std::cmp::min(remain_size, self.step);
                let column_data = (self.begin..self.begin + step).collect::<Vec<_>>();

                self.begin += step;
                Ok(Some(DataBlock::new_from_columns(vec![
                    UInt64Type::from_data(column_data),
                ])))
            }
        }
    }
}

impl TableFunction for NumbersTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
