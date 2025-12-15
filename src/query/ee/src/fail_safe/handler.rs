// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::TableInfo;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaReaders;
use databend_enterprise_fail_safe::FailSafeHandler;
use databend_enterprise_fail_safe::FailSafeHandlerWrapper;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use log::info;
use log::warn;
use opendal::ErrorKind;
use opendal::Operator;

pub struct RealFailSafeHandler {}

impl RealFailSafeHandler {}

#[async_trait::async_trait]
impl FailSafeHandler for RealFailSafeHandler {
    async fn recover_table_data(
        &self,
        ctx: &Arc<dyn TableContext>,
        table_info: TableInfo,
    ) -> Result<()> {
        let fuse_table = FuseTable::create_without_refresh_table_info(
            table_info,
            ctx.get_settings().get_s3_storage_class()?,
        )?;
        let op = fuse_table.get_operator();

        let amender = Amender::new(op);
        amender.recover_snapshot(fuse_table).await?;
        Ok(())
    }
}

impl RealFailSafeHandler {
    pub fn init() -> Result<()> {
        let rm = RealFailSafeHandler {};
        let wrapper = FailSafeHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}

struct Amender {
    op: Operator,
}

impl Amender {
    fn new(op: Operator) -> Self {
        Self { op }
    }

    async fn recover_snapshot(&self, table: Box<FuseTable>) -> Result<()> {
        match table.read_table_snapshot_without_cache().await {
            Ok(Some(snapshot)) => {
                let schema = table.schema();
                let operator = table.get_operator();
                self.recover_segments(&snapshot.segments, schema, operator)
                    .await?
            }
            Ok(None) => (),
            Err(e) => {
                if e.code() == ErrorCode::STORAGE_NOT_FOUND {
                    let snapshot_location = table.snapshot_loc().unwrap();
                    self.recover_object(&snapshot_location).await?;
                    let snapshot = table.read_table_snapshot().await?;
                    let schema = table.schema();
                    let operator = table.get_operator();
                    if let Some(snapshot) = snapshot {
                        self.recover_segments(&snapshot.segments, schema, operator)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn recover_segments(
        &self,
        segment_locations: &[Location],
        schema: Arc<TableSchema>,
        operator: Operator,
    ) -> Result<()> {
        let segment_reader =
            MetaReaders::segment_info_reader_without_cache(operator.clone(), schema);
        for (path, ver) in segment_locations {
            let segment = segment_reader
                .read(&LoadParams {
                    location: path.to_string(),
                    len_hint: None,
                    ver: *ver,
                    put_cache: false,
                })
                .await;

            match segment {
                Ok(segment) => {
                    self.recover_segment(segment.as_ref(), operator.clone())
                        .await?;
                }
                Err(e) => {
                    if e.code() == ErrorCode::STORAGE_NOT_FOUND {
                        self.recover_object(path).await?;
                        // load the segment again
                        let segment = segment_reader
                            .read(&LoadParams {
                                location: path.to_string(),
                                len_hint: None,
                                ver: *ver,
                                put_cache: false,
                            })
                            .await?;
                        self.recover_segment(segment.as_ref(), operator.clone())
                            .await?;
                    } else {
                        info!("other errors (ignored) {}", e);
                    }
                }
            }
        }
        Ok(())
    }

    async fn recover_segment(
        &self,
        segment: &CompactSegmentInfo,
        operator: Operator,
    ) -> Result<()> {
        let block_metas = segment.block_metas()?;
        let block_locations = block_metas
            .iter()
            .map(|item| &item.location)
            .collect::<Vec<_>>();
        self.recover_blocks(&block_locations, operator).await?;
        Ok(())
    }

    async fn recover_blocks(
        &self,
        snapshot_location: &[&Location],
        operator: Operator,
    ) -> Result<()> {
        for (path, _ver) in snapshot_location {
            if let Err(e) = operator.stat(path).await {
                if e.kind() == ErrorKind::NotFound {
                    self.recover_object(path).await?
                } else {
                    return Err(ErrorCode::StorageOther(format!(
                        "stat block file {} failed {}",
                        path, e
                    )));
                }
            } else {
                info!("block at {} is ok", path)
            }
        }

        Ok(())
    }

    async fn recover_object(&self, key: &str) -> Result<()> {
        info!("recovering object, key: {}", key);

        let mut versions = self
            .op
            .list_with(key)
            .versions(true)
            .deleted(true)
            .await
            .map_err(|err| {
                ErrorCode::StorageOther(format!(
                    "recover object {key} failed, failed to list all versions: {err:?}",
                ))
            })?;

        // Ensure all versions here are the same key.
        versions.retain(|v| v.path() == key);

        // find the latest version
        let latest_version = versions
            .iter()
            .filter(|v| v.metadata().version().is_some()) // Ensure the version_id exists
            .filter(|v| !v.metadata().is_deleted()) // Ensure this is not a delete marker
            .max_by(|a, b| {
                a.metadata()
                    .last_modified()
                    .cmp(&b.metadata().last_modified())
            });

        let Some(latest_version) = latest_version else {
            warn!(
                "latest version of object {} not found, not recoverable, versions returned {:?}",
                key, versions,
            );
            return Err(ErrorCode::StorageOther(format!(
                "recover object {key} failed, no version found",
            )));
        };

        // Find if there is a delete marker that is the latest one
        if !versions
            .iter()
            .any(|v| v.metadata().is_current() == Some(true) && v.metadata().is_deleted())
        {
            info!("current version is not a deleter marker, object {key}  no need to recover",);
            return Ok(());
        }

        // Get the latest version of the object
        let latest_version_id = latest_version.metadata().version().unwrap();
        self.recover_object_inner(key, latest_version_id)
            .await
            .map_err(|err| {
                ErrorCode::StorageOther(format!(
                    "recover object {} from version {} failed: {:?}",
                    key, latest_version_id, err
                ))
            })?;

        info!("object {} restored successfully", key);
        Ok(())
    }

    /// TODO: maybe we can implement this by remove the latest delete marker.
    ///
    /// Wait for opendal tp provide better APIs.
    async fn recover_object_inner(
        &self,
        key: &str,
        latest_version_id: &str,
    ) -> opendal::Result<()> {
        let content = self.op.read_with(key).version(latest_version_id).await?;
        self.op.write(key, content).await?;

        Ok(())
    }
}
