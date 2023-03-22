//  Copyright 2023 Datafuse Labs.
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
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_args::TableArgs;
use common_catalog::table_function::TableFunction;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_number;
use common_expression::types::DataType;
use common_expression::types::Float64Type;
use common_expression::types::Int64Type;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::types::F64;
use common_expression::DataBlock;
use common_expression::Expr;
use common_expression::FromData;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;
use common_sql::validate_function_arg;
use common_storages_factory::Table;
use common_storages_fuse::TableContext;

pub struct GenerateSeriesTable {
    table_info: TableInfo,
    start: Scalar,
    end: Scalar,
    step: Scalar,
}

impl GenerateSeriesTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        // Check args.
        validate_function_arg(
            table_func_name,
            table_args.positioned.len(),
            Some((2, 3)),
            2,
        )?;

        // Check if the parameter type is a Number or a Decimal.
        let check_number = |arg: &Scalar| -> Result<Scalar> {
            match arg {
                Scalar::Number(_) | Scalar::Decimal(_) => Ok(arg.clone()),
                _ => Err(ErrorCode::BadArguments("Expected number argument.")),
            }
        };

        let start = check_number(&table_args.positioned[0])?;
        let end = check_number(&table_args.positioned[1])?;
        let mut step = Scalar::Number(NumberScalar::Int64(1));
        if table_args.positioned.len() == 3 {
            step = check_number(&table_args.positioned[2])?;
        }

        let schema = TableSchema::new(vec![TableField::new(
            "generate_series",
            if number_scalars_have_float(vec![start.clone(), end.clone(), step.clone()]) {
                TableDataType::Number(NumberDataType::Float64)
            } else {
                TableDataType::Number(NumberDataType::Int64)
            },
        )]);

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

        Ok(Arc::new(GenerateSeriesTable {
            table_info,
            start,
            end,
            step,
        }))
    }
}

impl TableFunction for GenerateSeriesTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[async_trait::async_trait]
impl Table for GenerateSeriesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        _: Arc<dyn TableContext>,
        _: Option<PushDownInfo>,
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
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                GenerateSeriesSource::create(
                    ctx.clone(),
                    output,
                    self.start.clone(),
                    self.end.clone(),
                    self.step.clone(),
                )
            },
            1,
        )?;
        Ok(())
    }
}

struct GenerateSeriesSource {
    finished: bool,
    start: Scalar,
    end: Scalar,
    step: Scalar,
}

impl GenerateSeriesSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        start: Scalar,
        end: Scalar,
        step: Scalar,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, GenerateSeriesSource {
            start,
            end,
            step,
            finished: false,
        })
    }
}

fn range<T>(start: T, end: T, step: T) -> Vec<T>
where T: std::ops::Add<Output = T> + std::ops::Sub<Output = T> + PartialOrd + Copy + Default {
    let mut result = Vec::new();
    if step == T::default() {
        return result;
    }
    let increase_sign = end >= start;
    let step_sign = step > T::default();
    if increase_sign != step_sign {
        return result;
    }
    let mut current = start;
    if increase_sign {
        while current <= end {
            result.push(current);
            current = current + step;
        }
    } else {
        while current >= end {
            result.push(current);
            current = current + step;
        }
    }
    result
}

#[async_trait::async_trait]
impl AsyncSource for GenerateSeriesSource {
    const NAME: &'static str = "GenerateSeriesSourceTransform";

    #[async_trait::unboxed_simple]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }
        let have_float = number_scalars_have_float(vec![
            self.start.clone(),
            self.end.clone(),
            self.step.clone(),
        ]);
        if have_float {
            let dest_type = DataType::Number(NumberDataType::Float64);
            let start: F64 = check_number(
                None,
                FunctionContext::default(),
                &Expr::<usize>::Cast {
                    span: None,
                    is_try: false,
                    expr: Box::new(Expr::Constant {
                        span: None,
                        scalar: self.start.clone(),
                        data_type: self.start.clone().as_ref().infer_data_type(),
                    }),
                    dest_type: dest_type.clone(),
                },
                &BUILTIN_FUNCTIONS,
            )?;
            let end: F64 = check_number(
                None,
                FunctionContext::default(),
                &Expr::<usize>::Cast {
                    span: None,
                    is_try: false,
                    expr: Box::new(Expr::Constant {
                        span: None,
                        scalar: self.end.clone(),
                        data_type: self.end.clone().as_ref().infer_data_type(),
                    }),
                    dest_type: dest_type.clone(),
                },
                &BUILTIN_FUNCTIONS,
            )?;
            let step: F64 = check_number(
                None,
                FunctionContext::default(),
                &Expr::<usize>::Cast {
                    span: None,
                    is_try: false,
                    expr: Box::new(Expr::Constant {
                        span: None,
                        scalar: self.step.clone(),
                        data_type: self.step.clone().as_ref().infer_data_type(),
                    }),
                    dest_type,
                },
                &BUILTIN_FUNCTIONS,
            )?;

            self.finished = true;
            Ok(Some(DataBlock::new_from_columns(vec![
                Float64Type::from_data(range(start, end, step).into_iter()),
            ])))
        } else {
            let dest_type = DataType::Number(NumberDataType::Int64);
            let start: i64 = check_number(
                None,
                FunctionContext::default(),
                &Expr::<usize>::Cast {
                    span: None,
                    is_try: false,
                    expr: Box::new(Expr::Constant {
                        span: None,
                        scalar: self.start.clone(),
                        data_type: self.start.clone().as_ref().infer_data_type(),
                    }),
                    dest_type: dest_type.clone(),
                },
                &BUILTIN_FUNCTIONS,
            )?;
            let end: i64 = check_number(
                None,
                FunctionContext::default(),
                &Expr::<usize>::Cast {
                    span: None,
                    is_try: false,
                    expr: Box::new(Expr::Constant {
                        span: None,
                        scalar: self.end.clone(),
                        data_type: self.end.clone().as_ref().infer_data_type(),
                    }),
                    dest_type: dest_type.clone(),
                },
                &BUILTIN_FUNCTIONS,
            )?;
            let step: i64 = check_number(
                None,
                FunctionContext::default(),
                &Expr::<usize>::Cast {
                    span: None,
                    is_try: false,
                    expr: Box::new(Expr::Constant {
                        span: None,
                        scalar: self.step.clone(),
                        data_type: self.step.clone().as_ref().infer_data_type(),
                    }),
                    dest_type,
                },
                &BUILTIN_FUNCTIONS,
            )?;
            self.finished = true;
            Ok(Some(DataBlock::new_from_columns(vec![
                Int64Type::from_data(range(start, end, step).into_iter()),
            ])))
        }
    }
}

pub fn number_scalars_have_float(scalars: Vec<Scalar>) -> bool {
    for scalar in scalars {
        match scalar {
            Scalar::Number(n) => match n {
                NumberScalar::UInt8(_) => (),
                NumberScalar::UInt16(_) => (),
                NumberScalar::UInt32(_) => (),
                NumberScalar::UInt64(_) => (),
                NumberScalar::Int8(_) => (),
                NumberScalar::Int16(_) => (),
                NumberScalar::Int32(_) => (),
                NumberScalar::Int64(_) => (),
                NumberScalar::Float32(_) => return true,
                NumberScalar::Float64(_) => return true,
            },
            Scalar::Decimal(_) => return true,
            _ => (),
        }
    }
    false
}
