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
use std::time::Duration;

use aws_config::meta::region::RegionProviderChain;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::config::SharedCredentialsProvider;
use aws_sdk_s3::Client;
use databend_common_base::base::tokio::io::AsyncReadExt;
use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table::Table;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::FuseTable;
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
    async fn recover_table_data(&self, table_info: TableInfo) -> Result<()> {
        let storage_params = match &table_info.meta.storage_params {
            // External or attached table.
            Some(sp) => sp.clone(),
            // Normal table.
            None => {
                let config = GlobalConfig::instance();
                config.storage.params.clone()
            }
        };

        let fuse_table = FuseTable::do_create(table_info)?;

        let amender = Amender::new(storage_params).await;

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
    client: Client,
    bucket: String,
    root: String,
}

impl Amender {
    async fn new(storage_param: StorageParams) -> Self {
        if let StorageParams::S3(s3_config) = storage_param {
            if s3_config.access_key_id.is_empty() {
                panic!()
            }
            // Create credentials from access key and secret key
            let base_credentials = Credentials::new(
                &s3_config.access_key_id,
                s3_config.secret_access_key,
                None,
                None,
                "not_secure",
            );

            // Define the region you want to use
            let region_provider =
                RegionProviderChain::first_try(Region::new(s3_config.region)).or_else("us-east-1");

            let retry_config = RetryConfig::standard().with_max_attempts(5);
            let timeout_config = TimeoutConfig::builder()
                .operation_attempt_timeout(Duration::from_secs(2))
                .operation_timeout(Duration::from_secs(5))
                .build();

            // Load the configuration using the base credentials
            let config = aws_config::from_env()
                .region(region_provider)
                .endpoint_url(s3_config.endpoint_url)
                .credentials_provider(SharedCredentialsProvider::new(base_credentials))
                .retry_config(retry_config)
                .timeout_config(timeout_config)
                .load()
                .await;

            let root = s3_config.root;
            let bucket = s3_config.bucket;
            let client = Client::new(&config);

            Self {
                client,
                bucket,
                root,
            }
        } else {
            panic!()
        }
    }
    async fn recover_snapshot(&self, table: Box<FuseTable>) -> Result<()> {
        match table.read_table_snapshot().await {
            Ok(Some(snapshot)) => {
                let schema = table.schema();
                let operator = table.get_operator();
                self.recover_segments(&snapshot.segments, schema, operator)
                    .await?
            }
            Ok(None) => (),
            Err(e) => {
                if e.code() == ErrorCode::STORAGE_NOT_FOUND {
                    let snapshot_location = table.snapshot_loc().await?.unwrap();
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
        let segment_reader = MetaReaders::segment_info_reader_no_cache(operator.clone(), schema);
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
        let full_path;
        let obj_key = {
            if self.root.is_empty() {
                key
            } else {
                full_path = if self.root.ends_with('/') {
                    format!("{}{}", self.root, key)
                } else {
                    format!("{}/{}", self.root, key)
                };
                &full_path
            }
        };

        // trim the leading "/"
        let obj_key = if let Some(stripped) = obj_key.strip_prefix('/') {
            stripped
        } else {
            obj_key
        };

        info!("recovering object, key: {}", obj_key);

        // list versions of the object.
        //
        // the object is not supposed to have too many versions,
        // we do not bother checking versions beyond 1000.
        let list_object_versions_output = self
            .client
            .list_object_versions()
            .bucket(self.bucket.as_str())
            .prefix(obj_key)
            .send()
            .await
            .map_err(|e| {
                ErrorCode::StorageOther(format!("failed to list object versions. {}", e))
            })?;

        // find the latest version
        let latest_version = list_object_versions_output
            .versions()
            .iter()
            .filter(|v| v.key() == Some(obj_key)) // Ensure it matches the specified key
            .filter(|v| v.version_id().is_some()) // Ensure the version_id exists
            .max_by(|a, b| a.last_modified.cmp(&b.last_modified));

        let Some(latest_version) = latest_version else {
            warn!(
                "latest version of object {} not found, not recoverable, versions returned {:?}",
                obj_key,
                list_object_versions_output.versions(),
            );
            return Err(ErrorCode::StorageOther(format!(
                "recover object {} failed, no version found",
                obj_key
            )));
        };

        let delete_markers = &list_object_versions_output.delete_markers;

        match delete_markers {
            None => {
                info!(
                    "no delete markers there, object {}  no need to recover",
                    obj_key
                );
                return Ok(());
            }
            Some(markers) => {
                // Check if the latest one is DeleteMarker, if it is NOT, do not bother recovering it.
                //
                // Examples from aws shows that there might be both "Version" and "DeleteMarker" are "<IsLatest>true</IsLatest>"
                // https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html#API_ListObjectVersions_ResponseElements
                // , so we cannot check if the latest one is a DeleteMarker by `markers.iter().any(|v| v.is_latest)`
                let latest_delete_marker = markers
                    .iter()
                    .max_by(|a, b| a.last_modified.cmp(&b.last_modified))
                    .unwrap();

                if latest_delete_marker.last_modified < latest_version.last_modified {
                    info!(
                        "the latest one of object {} is not delete-marker, no need to recover (maybe recovered by other ones, concurrently)",
                        obj_key
                    );
                    return Ok(());
                }
            }
        }

        // Get the latest version of the object
        let latest_version_id = latest_version.version_id().unwrap();
        let get_object_response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(obj_key)
            .version_id(latest_version_id)
            .send()
            .await;

        match get_object_response {
            Ok(output) => {
                // read the object data
                let mut body = output.body.into_async_read();
                let mut data = Vec::new();
                body.read_to_end(&mut data).await?;

                // put the object back to restore it
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(obj_key)
                    .body(data.into())
                    .send()
                    .await
                    .map_err(|e| ErrorCode::StorageOther(format!("blah, {}", e)))?;

                info!("object {} restored successfully", obj_key);
                Ok(())
            }
            Err(e) => {
                info!("failed to get the object: {}", e);
                Err(ErrorCode::StorageOther(format!(
                    "recover object failed. cannot read obj {}, by version {}",
                    obj_key, latest_version_id
                )))
            }
        }
    }
}
