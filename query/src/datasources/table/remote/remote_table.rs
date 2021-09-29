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
use std::sync::mpsc::channel;

use common_datavalues::DataSchemaRef;
use common_dfs_api_vo::ReadPlanResult;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Extras;
use common_planners::InsertIntoPlan;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;
use crate::catalogs::TableInfo;
use crate::common::StoreApiProvider;
use crate::datasources::table_engine::TableEngine;
use crate::sessions::DatabendQueryContextRef;

#[allow(dead_code)]
pub struct RemoteTable {
    pub(crate) tbl_info: TableInfo,
    pub(crate) store_api_provider: StoreApiProvider,
}

#[async_trait::async_trait]
impl Table for RemoteTable {
    fn name(&self) -> &str {
        &self.tbl_info.name
    }

    fn database(&self) -> &str {
        &self.tbl_info.db
    }

    fn engine(&self) -> &str {
        &self.tbl_info.engine
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.tbl_info.schema.clone())
    }

    fn get_id(&self) -> u64 {
        self.tbl_info.table_id
    }

    fn is_local(&self) -> bool {
        false
    }

    fn read_plan(
        &self,
        ctx: DatabendQueryContextRef,
        push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan> {
        let (tx, rx) = channel();
        let cli_provider = self.store_api_provider.clone();
        let db_name = self.tbl_info.db.clone();
        let tbl_name = self.tbl_info.name.clone();
        {
            let push_downs = push_downs.clone();
            ctx.execute_task(async move {
                match cli_provider.try_get_storage_client().await {
                    Ok(client) => {
                        let parts_info = client
                            .read_plan(db_name, tbl_name, push_downs)
                            .await
                            .map_err(ErrorCode::from);
                        let _ = tx.send(parts_info);
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                    }
                }
            })?;
        }

        rx.recv()
            .map_err(ErrorCode::from_std_error)?
            .map(|v| self.partitions_to_plan(v, push_downs))
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        self.do_read(ctx, source_plan).await
    }

    async fn append_data(&self, _ctx: DatabendQueryContextRef, plan: InsertIntoPlan) -> Result<()> {
        let opt_stream = {
            let mut inner = plan.input_stream.lock();
            (*inner).take()
        };

        {
            let block_stream =
                opt_stream.ok_or_else(|| ErrorCode::EmptyData("input stream consumed"))?;

            let client = self.store_api_provider.try_get_storage_client().await?;

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

    async fn truncate(&self, _ctx: DatabendQueryContextRef, plan: TruncateTablePlan) -> Result<()> {
        let client = self.store_api_provider.try_get_storage_client().await?;
        client.truncate(plan.db.clone(), plan.table.clone()).await?;
        Ok(())
    }
}

impl RemoteTable {
    pub fn create(tbl_info: TableInfo, store_api_provider: StoreApiProvider) -> Box<dyn Table> {
        let table = Self {
            tbl_info,
            store_api_provider,
        };
        Box::new(table)
    }

    fn partitions_to_plan(
        &self,
        res: ReadPlanResult,
        push_downs: Option<Extras>,
    ) -> ReadDataSourcePlan {
        let mut partitions = vec![];
        let mut statistics = Statistics {
            read_rows: 0,
            read_bytes: 0,
            is_exact: false,
        };

        if let Some(parts) = res {
            for part in parts {
                partitions.push(Part {
                    name: part.part.name,
                    version: 0,
                });
                statistics.read_rows += part.stats.read_rows;
                statistics.read_bytes += part.stats.read_bytes;
                statistics.is_exact &= part.stats.is_exact;
            }
        }

        ReadDataSourcePlan {
            db: self.tbl_info.db.clone(),
            table: self.tbl_info.name.clone(),
            table_id: self.tbl_info.table_id,
            table_version: None,
            schema: self.tbl_info.schema.clone(),
            parts: partitions,
            statistics,
            description: "".to_string(),
            scan_plan: Default::default(),
            remote: true,
            tbl_args: None,
            push_downs,
        }
    }
}

pub struct RemoteTableFactory;
impl TableEngine for RemoteTableFactory {
    fn try_create(
        &self,
        tbl_info: TableInfo,
        store_provider: StoreApiProvider,
    ) -> Result<Box<dyn Table>> {
        Ok(RemoteTable::create(tbl_info, store_provider))
    }
}
