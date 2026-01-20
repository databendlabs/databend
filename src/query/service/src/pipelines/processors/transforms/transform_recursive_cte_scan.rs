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

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_storages_basic::RecursiveCteMemoryTable;

use crate::sessions::QueryContext;

pub struct TransformRecursiveCteScan {
    ctx: Arc<QueryContext>,
    table: Option<Arc<dyn Table>>,
    table_name: String,
    exec_id: Option<u64>,
}

impl TransformRecursiveCteScan {
    pub fn create(
        ctx: Arc<QueryContext>,
        output_port: Arc<OutputPort>,
        table_name: String,
        exec_id: Option<u64>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(
            ctx.get_scan_progress(),
            output_port,
            TransformRecursiveCteScan {
                ctx,
                table: None,
                table_name,
                exec_id,
            },
        )
    }
}

#[async_trait::async_trait]
impl AsyncSource for TransformRecursiveCteScan {
    const NAME: &'static str = "RecursiveCteScan";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.table.is_none() {
            let table = self
                .ctx
                .get_table(
                    &self.ctx.get_current_catalog(),
                    &self.ctx.get_current_database(),
                    &self.table_name,
                )
                .await?;
            self.table = Some(table);
        }
        let memory_table = self
            .table
            .as_ref()
            .unwrap()
            .as_any()
            .downcast_ref::<RecursiveCteMemoryTable>()
            .unwrap();
        let data = if let Some(id) = self.exec_id {
            memory_table.take_by_id(id)
        } else {
            return Err(ErrorCode::Internal(format!(
                "Internal, TransformRecursiveCteScan not exec_id on CTE: {}",
                self.table_name,
            )));
        };
        if data.is_empty() {
            return Ok(None);
        }
        let data = DataBlock::concat(&data)?;
        if data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(data))
        }
    }
}
