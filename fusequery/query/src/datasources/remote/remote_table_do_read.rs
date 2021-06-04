// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCodes;
use common_exception::Result;
use common_flights::ReadAction;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::datasources::remote::remote_table::RemoteTable;
use crate::sessions::FuseQueryContextRef;

impl RemoteTable {
    #[inline]
    pub(super) async fn do_read(
        &self,
        ctx: FuseQueryContextRef,
    ) -> Result<SendableDataBlockStream> {
        let client = self.store_client_provider.try_get_client().await?;
        let schema = self.schema.clone();
        let db = self.db.to_string();
        let tbl = self.name.to_string();
        let progress_callback = ctx.progress_callback();

        let iter = std::iter::from_fn(move || match ctx.try_get_partitions(1) {
            Err(_) => None,
            Ok(parts) if parts.is_empty() => None,
            Ok(parts) => Some(ReadAction {
                partition: parts[0].clone(),
                push_down: PlanNode::ReadSource(ReadDataSourcePlan {
                    db: db.clone(),
                    table: tbl.clone(),
                    schema: schema.clone(),
                    remote: true,
                    ..ReadDataSourcePlan::empty()
                }),
            }),
        });

        let schema = self.schema.clone();
        let parts = futures::stream::iter(iter);
        let streams = parts.then(move |parts| {
            let mut client = client.clone();
            let schema = schema.clone();
            async move {
                let r = client.read_partition(schema, &parts).await;
                r.unwrap_or_else(|e| {
                    Box::pin(futures::stream::once(async move {
                        Err(ErrorCodes::CannotReadFile(format!(
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
