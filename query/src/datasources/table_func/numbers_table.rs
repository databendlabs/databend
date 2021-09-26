//  Copyright 2021 Datafuse Labs.
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
//

use std::any::Any;
use std::mem::size_of;
use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::SendableDataBlockStream;

use super::numbers_stream::NumbersStream;
use crate::catalogs::Table;
use crate::catalogs::TableFunction;
use crate::datasources::common::generate_parts;
use crate::sessions::DatabendQueryContextRef;

pub struct NumbersTable {
    db_name: String,
    table_name: String,
    table_id: u64, // to be removed, if func never renamed
    schema: DataSchemaRef,
    total: u64,
}

impl NumbersTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: Option<Expression>,
    ) -> Result<Arc<dyn TableFunction>> {
        let mut total = None;
        if let Some(Expression::Literal { value, .. }) = &table_args {
            total = Some(value.as_u64()?);
        }

        let total = total.ok_or_else(|| {
            eprintln!("NumbersTable: creation failed");
            ErrorCode::BadArguments(format!(
                "Must have one number argument for table: system.{}",
                &table_func_name
            ))
        })?;

        Ok(Arc::new(NumbersTable {
            db_name: database_name.to_string(),
            table_name: table_func_name.to_string(),
            table_id,
            schema: DataSchemaRefExt::create(vec![DataField::new(
                "number",
                DataType::UInt64,
                false,
            )]),
            total,
        }))
    }
}

#[async_trait::async_trait]
impl Table for NumbersTable {
    fn name(&self) -> &str {
        &self.table_name
    }

    fn database(&self) -> &str {
        &self.db_name
    }

    fn get_id(&self) -> u64 {
        self.table_id
    }

    fn engine(&self) -> &str {
        match self.table_name.as_str() {
            "numbers" => "SystemNumbers",
            "numbers_mt" => "SystemNumbersMt",
            "numbers_local" => "SystemNumbersLocal",
            _ => unreachable!(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    // As remote for performance test.
    fn is_local(&self) -> bool {
        self.table_name.as_str() == "numbers_local"
    }

    fn read_plan(
        &self,
        ctx: DatabendQueryContextRef,
        push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan> {
        let total = self.total;

        let statistics =
            Statistics::new_exact(total as usize, ((total) * size_of::<u64>() as u64) as usize);
        ctx.try_set_statistics(&statistics)?;
        ctx.add_total_rows_approx(statistics.read_rows);

        let tbl_arg = Some(Expression::create_literal(DataValue::UInt64(Some(
            self.total,
        ))));

        Ok(ReadDataSourcePlan {
            db: self.db_name.clone(),
            table: self.table_name.clone(),
            table_id: self.table_id,
            table_version: None,
            schema: self.schema.clone(),
            parts: generate_parts(0, ctx.get_settings().get_max_threads()?, total),
            statistics: statistics.clone(),
            description: format!(
                "(Read from system.{} table, Read Rows:{}, Read Bytes:{})",
                &self.table_name, statistics.read_rows, statistics.read_bytes
            ),
            scan_plan: Default::default(), // scan_plan will be removed form ReadSourcePlan soon
            remote: false,
            tbl_args: tbl_arg,
            push_downs,
        })
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(NumbersStream::try_create(
            ctx,
            self.schema.clone(),
        )?))
    }
}

impl TableFunction for NumbersTable {
    fn function_name(&self) -> &str {
        &self.table_name
    }

    fn db(&self) -> &str {
        &self.db_name
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
