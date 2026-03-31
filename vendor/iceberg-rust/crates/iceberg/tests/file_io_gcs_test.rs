// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Integration tests for FileIO Google Cloud Storage (GCS).

#[cfg(all(test, feature = "storage-gcs"))]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::RwLock;

    use bytes::Bytes;
    use ctor::{ctor, dtor};
    use iceberg::io::{FileIO, FileIOBuilder, GCS_NO_AUTH, GCS_SERVICE_PATH};
    use iceberg_test_utils::docker::DockerCompose;
    use iceberg_test_utils::{normalize_test_name, set_up};

    static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);
    static FAKE_GCS_PORT: u16 = 4443;
    static FAKE_GCS_BUCKET: &str = "test-bucket";

    #[ctor]
    fn before_all() {
        let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
        let docker_compose = DockerCompose::new(
            normalize_test_name(module_path!()),
            format!("{}/testdata/file_io_gcs", env!("CARGO_MANIFEST_DIR")),
        );
        docker_compose.up();
        guard.replace(docker_compose);
    }

    #[dtor]
    fn after_all() {
        let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
        guard.take();
    }

    async fn get_file_io_gcs() -> FileIO {
        set_up();

        let ip = DOCKER_COMPOSE_ENV
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .get_container_ip("gcs-server");
        let addr = SocketAddr::new(ip, FAKE_GCS_PORT);

        // A bucket must exist for FileIO
        create_bucket(FAKE_GCS_BUCKET, addr.to_string())
            .await
            .unwrap();

        FileIOBuilder::new("gcs")
            .with_props(vec![
                (GCS_SERVICE_PATH, format!("http://{addr}")),
                (GCS_NO_AUTH, "true".to_string()),
            ])
            .build()
            .unwrap()
    }

    // Create a bucket against the emulated GCS storage server.
    async fn create_bucket(name: &str, server_addr: String) -> anyhow::Result<()> {
        let mut bucket_data = HashMap::new();
        bucket_data.insert("name", name);

        let client = reqwest::Client::new();
        let endpoint = format!("http://{server_addr}/storage/v1/b");
        client.post(endpoint).json(&bucket_data).send().await?;
        Ok(())
    }

    fn get_gs_path() -> String {
        format!("gs://{FAKE_GCS_BUCKET}")
    }

    #[tokio::test]
    async fn gcs_exists() {
        let file_io = get_file_io_gcs().await;
        assert!(file_io.exists(format!("{}/", get_gs_path())).await.unwrap());
    }

    #[tokio::test]
    async fn gcs_write() {
        let gs_file = format!("{}/write-file", get_gs_path());
        let file_io = get_file_io_gcs().await;
        let output = file_io.new_output(&gs_file).unwrap();
        output
            .write(bytes::Bytes::from_static(b"iceberg-gcs!"))
            .await
            .expect("Write to test output file");
        assert!(file_io.exists(gs_file).await.unwrap())
    }

    #[tokio::test]
    async fn gcs_read() {
        let gs_file = format!("{}/read-gcs", get_gs_path());
        let file_io = get_file_io_gcs().await;
        let output = file_io.new_output(&gs_file).unwrap();
        output
            .write(bytes::Bytes::from_static(b"iceberg!"))
            .await
            .expect("Write to test output file");
        assert!(file_io.exists(&gs_file).await.unwrap());

        let input = file_io.new_input(gs_file).unwrap();
        assert_eq!(input.read().await.unwrap(), Bytes::from_static(b"iceberg!"));
    }
}
