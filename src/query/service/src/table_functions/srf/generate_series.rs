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
use std::mem::discriminant;
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
use common_expression::infer_schema_type;
use common_expression::type_check::check_number;
use common_expression::types::DataType;
use common_expression::types::DateType;
use common_expression::types::Int64Type;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::types::TimestampType;
use common_expression::DataBlock;
use common_expression::Expr;
use common_expression::FromData;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::TableField;
use common_expression::TableSchema;
use common_functions::BUILTIN_FUNCTIONS;
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
        println!("args:{:?}", table_args.positioned);
        validate_args(&table_args.positioned, table_func_name)?;

        // The data types of start and end have been checked for consistency, and the input types are returned
        let schema = TableSchema::new(vec![TableField::new(
            "generate_series",
            infer_schema_type(&table_args.positioned[0].as_ref().infer_data_type())?,
        )]);

        let start = table_args.positioned[0].clone();
        let end = table_args.positioned[1].clone();
        let mut step = Scalar::Number(NumberScalar::Int64(1));
        if table_args.positioned.len() == 3 {
            step = table_args.positioned[2].clone();
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

        let columns = match self.start {
            Scalar::Number(_) => Int64Type::from_data(range(start, end, step).into_iter()),
            Scalar::Timestamp(_) => TimestampType::from_data(range(start, end, step).into_iter()),
            Scalar::Date(_) => {
                DateType::from_data(range(start as i32, end as i32, step as i32).into_iter())
            }
            _ => {
                return Err(ErrorCode::BadDataValueType(format!(
                    "No support Type, got start is {:?} and end is {:?}",
                    self.start, self.end
                )));
            }
        };
        Ok(Some(DataBlock::new_from_columns(vec![columns])))
    }
}

pub fn validate_args(args: &Vec<Scalar>, table_func_name: &str) -> Result<()> {
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
