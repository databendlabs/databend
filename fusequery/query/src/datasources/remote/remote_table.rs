// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::mpsc::channel;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_flights::ScanPartitionResult;
use common_planners::InsertIntoPlan;
use common_planners::Partition;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_planners::TableOptions;
use common_streams::SendableDataBlockStream;

use crate::datasources::remote::StoreClientProvider;
use crate::datasources::ITable;
use crate::sessions::FuseQueryContextRef;

#[allow(dead_code)]
pub struct RemoteTable {
    pub(crate) db: String,
    pub(crate) name: String,
    pub(crate) schema: DataSchemaRef,
    pub(crate) store_client_provider: StoreClientProvider,
}

impl RemoteTable {
    pub fn try_create(
        db: String,
        name: String,
        schema: DataSchemaRef,
        store_client_provider: StoreClientProvider,
        _options: TableOptions,
    ) -> Result<Box<dyn ITable>> {
        let table = Self {
            db,
            name,
            schema,
            store_client_provider,
        };
        Ok(Box::new(table))
    }
}

#[async_trait::async_trait]
impl ITable for RemoteTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn engine(&self) -> &str {
        "remote"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn is_local(&self) -> bool {
        false
    }

    fn read_plan(
        &self,
        ctx: FuseQueryContextRef,
        scan: &ScanPlan,
        _partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        // Change this method to async at current stage might be harsh
        let (tx, rx) = channel();
        let cli_provider = self.store_client_provider.clone();
        let db_name = self.db.clone();
        let tbl_name = self.name.clone();
        {
            let scan = scan.clone();
            ctx.execute_task(async move {
                match cli_provider.try_get_client().await {
                    Ok(mut client) => {
                        let parts_info = client
                            .scan_partition(db_name, tbl_name, &scan)
                            .await
                            .map_err(ErrorCodes::from);
                        let _ = tx.send(parts_info);
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                    }
                }
            });
        }

        rx.recv()
            .map_err(ErrorCodes::from_std_error)?
            .map(|v| self.partitions_to_plan(v, scan.clone()))
    }

    async fn read(&self, ctx: FuseQueryContextRef) -> Result<SendableDataBlockStream> {
        self.do_read(ctx).await
    }

    async fn append_data(&self, _ctx: FuseQueryContextRef, plan: InsertIntoPlan) -> Result<()> {
        let opt_stream = {
            let mut inner = plan.input_stream.lock().unwrap();
            (*inner).take()
        };

        {
            let block_stream =
                opt_stream.ok_or_else(|| ErrorCodes::EmptyData("input stream consumed"))?;
            let mut client = self.store_client_provider.try_get_client().await?;
            client
                .append_data(
                    plan.db_name.clone(),
                    plan.tbl_name.clone(),
                    (&plan).schema().clone(),
                    block_stream,
                )
                .await?;
        }

        Ok(())
    }
}

impl RemoteTable {
    fn partitions_to_plan(
        &self,
        res: ScanPartitionResult,
        scan_plan: ScanPlan,
    ) -> ReadDataSourcePlan {
        let mut partitions = vec![];
        let mut statistics = Statistics {
            read_rows: 0,
            read_bytes: 0,
        };

        if let Some(parts) = res {
            for part in parts {
                partitions.push(Partition {
                    name: part.partition.name,
                    version: 0,
                });
                statistics.read_rows += part.stats.read_rows;
                statistics.read_bytes += part.stats.read_bytes;
            }
        }

        ReadDataSourcePlan {
            db: self.db.clone(),
            table: self.name.clone(),
            schema: self.schema.clone(),
            partitions,
            statistics,
            description: "".to_string(),
            scan_plan: Arc::new(scan_plan),
            remote: true,
        }
    }
}
