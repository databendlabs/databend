// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_planners::TableOptions;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;
use futures::stream::StreamExt;

use super::MemoryTableStream;
use crate::datasources::Common;
use crate::datasources::Table;
use crate::sessions::FuseQueryContextRef;

pub struct MemoryTable {
    db: String,
    name: String,
    schema: DataSchemaRef,
    blocks: Arc<RwLock<Vec<DataBlock>>>,
}

impl MemoryTable {
    pub fn try_create(
        db: String,
        name: String,
        schema: DataSchemaRef,
        _options: TableOptions,
    ) -> Result<Box<dyn Table>> {
        let table = Self {
            db,
            name,
            schema,
            blocks: Arc::new(RwLock::new(vec![])),
        };
        Ok(Box::new(table))
    }
}

#[async_trait::async_trait]
impl Table for MemoryTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn engine(&self) -> &str {
        "Memory"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn is_local(&self) -> bool {
        true
    }

    fn read_plan(
        &self,
        ctx: FuseQueryContextRef,
        scan: &ScanPlan,
        _partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        let blocks = self.blocks.read();
        let rows = blocks.iter().map(|block| block.num_rows()).sum();
        let bytes = blocks.iter().map(|block| block.memory_size()).sum();

        Ok(ReadDataSourcePlan {
            db: self.db.clone(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            parts: Common::generate_parts(
                0,
                ctx.get_settings().get_max_threads()?,
                blocks.len() as u64,
            ),
            statistics: Statistics::new_exact(rows, bytes),
            description: format!("(Read from Memory Engine table  {}.{})", self.db, self.name),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        ctx: FuseQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let blocks = self.blocks.read();
        Ok(Box::pin(MemoryTableStream::try_create(
            ctx,
            blocks.clone(),
        )?))
    }

    async fn append_data(
        &self,
        _ctx: FuseQueryContextRef,
        insert_plan: common_planners::InsertIntoPlan,
    ) -> Result<()> {
        let mut s = {
            let mut inner = insert_plan.input_stream.lock();
            (*inner).take()
        }
        .ok_or_else(|| ErrorCode::EmptyData("input stream consumed"))?;

        if insert_plan.schema().as_ref() != self.schema.as_ref() {
            return Err(ErrorCode::BadArguments("DataBlock schema mismatch"));
        }

        while let Some(block) = s.next().await {
            let mut blocks = self.blocks.write();
            blocks.push(block);
        }
        Ok(())
    }

    async fn truncate(
        &self,
        _ctx: FuseQueryContextRef,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        let mut blocks = self.blocks.write();
        blocks.clear();
        Ok(())
    }
}
