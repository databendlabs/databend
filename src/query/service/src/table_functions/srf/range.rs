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
use std::mem::discriminant;
use std::sync::Arc;

use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Utc;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::*;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::SyncSource;
use databend_common_pipeline_sources::SyncSourcer;
use databend_common_sql::validate_function_arg;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::TableContext;
use itertools::Itertools;

pub struct RangeTable {
    table_info: TableInfo,
    start: Scalar,
    end: Scalar,
    step: Scalar,
    data_type: DataType,
}

impl RangeTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        validate_args(&table_args.positioned, table_func_name)?;

        let data_type = match &table_args.positioned[0] {
            Scalar::Number(_) => DataType::Number(NumberDataType::Int64),
            Scalar::Timestamp(_) => DataType::Timestamp,
            Scalar::Date(_) => DataType::Date,
            other => {
                return Err(ErrorCode::BadArguments(format!(
                    "Unsupported data type for generate_series: {:?}",
                    other
                )));
            }
        };

        let table_type = infer_schema_type(&data_type)?;

        // The data types of start and end have been checked for consistency, and the input types are returned
        let schema = TableSchema::new(vec![TableField::new(table_func_name, table_type)]);

        let start = table_args.positioned[0].clone();
        let end = table_args.positioned[1].clone();

        let mut step = if table_args.positioned.len() == 3 {
            table_args.positioned[2].clone()
        } else {
            Scalar::Number(NumberScalar::Int64(1))
        };
        if let Scalar::Timestamp(_) = start {
            // since `to_timestamp` return value in micro seconds, we need to to change step as the same unit
            let step_i64 = get_i64_number(&step)?;
            if step_i64 < 1000 {
                // treat step as seconds
                step = Scalar::Number(NumberScalar::Int64(step_i64 * 1000000));
            } else if step_i64 < 1000000 {
                // treat step as mills seconds
                step = Scalar::Number(NumberScalar::Int64(step_i64 * 1000));
            }
        }

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
        Ok(Arc::new(RangeTable {
            table_info,
            start,
            end,
            step,
            data_type,
        }))
    }
}

impl TableFunction for RangeTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[async_trait::async_trait]
impl Table for RangeTable {
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
        Some(TableArgs::new_positioned(vec![
            self.start.clone(),
            self.end.clone(),
            self.step.clone(),
        ]))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        match self.name() {
            "generate_series" => {
                pipeline.add_source(
                    |output| {
                        RangeSource::<true>::create(
                            ctx.clone(),
                            output,
                            self.data_type.clone(),
                            self.start.clone(),
                            self.end.clone(),
                            self.step.clone(),
                        )
                    },
                    1,
                )?;
            }

            "range" => {
                pipeline.add_source(
                    |output| {
                        RangeSource::<false>::create(
                            ctx.clone(),
                            output,
                            self.data_type.clone(),
                            self.start.clone(),
                            self.end.clone(),
                            self.step.clone(),
                        )
                    },
                    1,
                )?;
            }
            _ => {
                return Err(ErrorCode::BadDataValueType(format!(
                    "Unknown function: {}",
                    self.name(),
                )));
            }
        }
        Ok(())
    }
}

struct RangeSource<const INCLUSIVE: bool> {
    finished: bool,

    data_type: DataType,

    // TODO: make it atomic thus we can use it in multiple threads
    current_idx: i64,
    start: i64,
    end: i64,
    step: i64,
}

fn get_i64_number(scalar: &Scalar) -> Result<i64> {
    check_number::<_, i64>(
        None,
        &FunctionContext::default(),
        &Expr::<usize>::Constant {
            span: None,
            scalar: scalar.clone(),
            data_type: scalar.clone().as_ref().infer_data_type(),
        },
        &BUILTIN_FUNCTIONS,
    )
}

impl<const INCLUSIVE: bool> RangeSource<INCLUSIVE> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        data_type: DataType,
        start: Scalar,
        end: Scalar,
        step: Scalar,
    ) -> Result<ProcessorPtr> {
        let start = get_i64_number(&start)?;
        let mut end = get_i64_number(&end)?;
        let step = get_i64_number(&step)?;

        if INCLUSIVE {
            if step > 0 {
                end += 1;
            } else {
                end -= 1;
            }
        }

        if step == 0 {
            return Err(ErrorCode::BadArguments("step must not be zero".to_string()));
        } else if step > 0 && start > end {
            return Err(ErrorCode::BadArguments(
                "start must be less than or equal to end when step is positive".to_string(),
            ));
        } else if step < 0 && start < end {
            return Err(ErrorCode::BadArguments(
                "start must be greater than or equal to end when step is negative".to_string(),
            ));
        }

        SyncSourcer::create(ctx.clone(), output, Self {
            current_idx: 0,
            data_type,
            start,
            end,
            step,
            finished: false,
        })
    }
}

impl<const INCLUSIVE: bool> SyncSource for RangeSource<INCLUSIVE> {
    const NAME: &'static str = "RangeSourceTransform";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }

        let current_start = self.start + self.step * self.current_idx;
        let offset = if self.step < 0 { 1 } else { -1 };

        // check if we need to finish
        if (self.step > 0 && current_start >= self.end)
            || (self.step < 0 && current_start <= self.end)
        {
            self.finished = true;
            return Ok(None);
        }

        static MAX_BLOCK_SIZE: i64 = 1024 * 1024;

        let size =
            ((self.end - current_start + (self.step + offset)) / self.step).min(MAX_BLOCK_SIZE);

        let column = match self.data_type {
            DataType::Number(_) => Int64Type::from_data(
                (0..size)
                    .map(|idx| current_start + self.step * idx)
                    .collect_vec(),
            ),
            DataType::Timestamp => TimestampType::from_data(
                (0..size)
                    .map(|idx| current_start + self.step * idx)
                    .collect_vec(),
            ),
            DataType::Date => {
                let current_start = current_start as i32;
                let step = self.step as i32;
                DateType::from_data(
                    (0..size as i32)
                        .map(|idx| current_start + step * idx)
                        .collect_vec(),
                )
            }
            _ => unreachable!(),
        };

        self.current_idx += size;
        Ok(Some(DataBlock::new_from_columns(vec![column])))
    }
}

fn validate_args(args: &Vec<Scalar>, table_func_name: &str) -> Result<()> {
    // Check args len.
    validate_function_arg(table_func_name, args.len(), Some((2, 3)), 2)?;

    // Check whether the data types of start and end are consistent.
    if discriminant(&args[0]) != discriminant(&args[1]) {
        return Err(ErrorCode::BadDataValueType(format!(
            "Expected same scalar type, but got start is {:?} and end is {:?}",
            args[0], args[1]
        )));
    }

    if args.iter().all(|arg| {
        matches!(
            arg,
            Scalar::Number(_) | Scalar::Date(_) | Scalar::Timestamp(_)
        )
    }) {
        Ok(())
    } else {
        Err(ErrorCode::BadDataValueType(format!(
            "Expected Number, Date or Timestamp type, but got {:?}",
            args
        )))
    }
}
