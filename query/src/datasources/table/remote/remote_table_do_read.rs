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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_store_api::ReadAction;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::datasources::table::remote::remote_table::RemoteTable;
use crate::sessions::DatabendQueryContextRef;

impl RemoteTable {
    #[inline]
    pub(in crate::datasources) async fn do_read(
        &self,
        ctx: DatabendQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let client = self.store_api_provider.try_get_storage_client().await?;
        let progress_callback = ctx.progress_callback();

        let plan = source_plan.clone();
        let iter = std::iter::from_fn(move || match ctx.try_get_partitions(1) {
            Err(_) => None,
            Ok(parts) if parts.is_empty() => None,
            Ok(parts) => {
                let plan = plan.clone();
                Some(ReadAction {
                    part: parts[0].clone(),
                    push_down: PlanNode::ReadSource(plan),
                })
            }
        });

        let schema = self.schema.clone();
        let parts = futures::stream::iter(iter);
        let streams = parts.then(move |parts| {
            let client = client.clone();
            let schema = schema.clone();
            async move {
                let r = client.read_partition(schema, &parts).await;
                r.unwrap_or_else(|e| {
                    Box::pin(futures::stream::once(async move {
                        Err(ErrorCode::CannotReadFile(format!(
                            "get partition failure. partition [{:?}], error {}",
                            &parts, e
                        )))
                    }))
                })
            }
        });

        let stream = ProgressStream::try_create(Box::pin(streams.flatten()), progress_callback?)?;
        Ok(Box::pin(stream))
    }
}
