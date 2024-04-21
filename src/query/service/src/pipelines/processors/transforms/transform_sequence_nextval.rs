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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::NumberColumnBuilder;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_meta_app::tenant::Tenant;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_sql::plans::FunctionCallSource;
use databend_common_table_function::SequenceTableFunctionApi;

use crate::sessions::QueryContext;

pub struct SequenceNextvalSource {
    row: usize,
    tenant: Tenant,
    function_call: FunctionCallSource,
    is_finished: bool,
    ctx: Arc<QueryContext>,
}

impl SequenceNextvalSource {
    pub fn create(
        row: usize,
        function_call: FunctionCallSource,
        ctx: Arc<QueryContext>,
    ) -> SequenceNextvalSource {
        let tenant = ctx.get_tenant();

        SequenceNextvalSource {
            row,
            tenant,
            function_call,
            is_finished: false,
            ctx,
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for SequenceNextvalSource {
    const NAME: &'static str = "SequenceNextvalSource";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        let nextval = SequenceTableFunctionApi::get_sequence_nextval(
            self.ctx.get_default_catalog()?,
            self.tenant.clone(),
            self.function_call.arguments[0].clone(),
        )
        .await?;

        let mut builder = NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, self.row);
        builder.push(NumberScalar::UInt64(nextval));
        let builder = ColumnBuilder::Number(builder);
        let column = builder.build();

        let block = DataBlock::new_from_columns(vec![column]);

        self.is_finished = true;
        Ok(Some(block))
    }
}
