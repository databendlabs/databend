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

use databend_common_exception::Result;
use databend_common_meta_app::principal::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_version::BUILD_INFO;
use databend_query::sessions::BuildInfoRef;
use databend_query::sessions::QueryContext;
use databend_query::sessions::Session;
use databend_query::sql::Planner;
use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::dataframe::default_box_size;
use crate::utils::RUNTIME;
use crate::utils::wait_for_future;

#[pyclass(name = "SessionContext", module = "databend", subclass)]
#[derive(Clone)]
pub(crate) struct PySessionContext {
    pub(crate) session: Arc<Session>,
    version: BuildInfoRef,
}

#[pymethods]
impl PySessionContext {
    #[new]
    #[pyo3(signature = (_tenant = None, data_path = ".databend"))]
    fn new(_tenant: Option<&str>, data_path: &str, _py: Python) -> PyResult<Self> {
        // Auto-initialize if not already done
        if !crate::EMBEDDED_INIT_STATE.load(std::sync::atomic::Ordering::Acquire) {
            crate::init_embedded(_py, data_path)?;
        }

        // Create session using the initialized services

        let session_result = RUNTIME.block_on(async {
            use databend_common_catalog::session_type::SessionType;
            use databend_common_meta_app::principal::UserInfo;
            use databend_query::sessions::SessionManager;

            let session_manager = SessionManager::instance();
            let dummy_session = session_manager.create_session(SessionType::Dummy).await?;
            let session = session_manager.register_session(dummy_session)?;

            // Set root user with full permissions for embedded mode
            let mut user_info = UserInfo::new_no_auth("root", "%");
            // Grant all global privileges
            user_info.grants.grant_privileges(
                &databend_common_meta_app::principal::GrantObject::Global,
                databend_common_meta_app::principal::UserPrivilegeSet::available_privileges_on_global(),
            );
            // Grant account_admin role
            user_info
                .grants
                .grant_role(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string());
            user_info
                .option
                .set_default_role(Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()));
            // Set all flags to bypass various checks
            user_info.option.set_all_flag();
            session
                .set_authed_user(user_info, Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()))
                .await?;

            Ok::<Arc<Session>, Box<dyn std::error::Error + Send + Sync>>(session)
        });

        let session = session_result.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Session creation failed: {}",
                e
            ))
        })?;

        let res = Self {
            session,
            version: &BUILD_INFO,
        };

        Ok(res)
    }

    fn sql(&mut self, sql: &str, py: Python) -> PyResult<PyDataFrame> {
        // Use standard query context creation - embedded mode will be handled automatically
        let ctx =
            wait_for_future(py, self.session.create_query_context(self.version)).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create query context: {}",
                    e
                ))
            })?;
        let res = wait_for_future(py, plan_sql(&ctx, sql));

        match res {
            Err(err) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Error: {}",
                err
            ))),
            Ok(res) => Ok(res),
        }
    }

    #[pyo3(signature = (name, path, pattern = None, connection = None))]
    fn register_parquet(
        &mut self,
        name: &str,
        path: &str,
        pattern: Option<&str>,
        connection: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        self.register_table(name, path, "parquet", pattern, connection, py)
    }

    #[pyo3(signature = (name, path, pattern = None, connection = None))]
    fn register_csv(
        &mut self,
        name: &str,
        path: &str,
        pattern: Option<&str>,
        connection: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        self.register_table(name, path, "csv", pattern, connection, py)
    }

    #[pyo3(signature = (name, path, pattern = None, connection = None))]
    fn register_ndjson(
        &mut self,
        name: &str,
        path: &str,
        pattern: Option<&str>,
        connection: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        self.register_table(name, path, "ndjson", pattern, connection, py)
    }

    #[pyo3(signature = (name, path, pattern = None, connection = None))]
    fn register_tsv(
        &mut self,
        name: &str,
        path: &str,
        pattern: Option<&str>,
        connection: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        self.register_table(name, path, "tsv", pattern, connection, py)
    }

    fn register_table(
        &mut self,
        name: &str,
        path: &str,
        file_format: &str,
        pattern: Option<&str>,
        connection: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        let sql = if let Some(connection_name) = connection {
            let pattern_clause = pattern
                .map(|p| format!(", pattern => '{}'", p))
                .unwrap_or_default();
            format!(
                "create view {} as select * from '{}' (file_format => '{}'{}, connection => '{}')",
                name, path, file_format, pattern_clause, connection_name
            )
        } else {
            let mut path = path.to_owned();
            if path.starts_with('/') {
                path = format!("fs://{}", path);
            }

            if !path.contains("://") {
                path = format!(
                    "fs://{}/{}",
                    std::env::current_dir().unwrap().to_str().unwrap(),
                    path.as_str()
                );
            }

            let pattern_clause = pattern
                .map(|p| format!(", pattern => '{}'", p))
                .unwrap_or_default();
            format!(
                "create view {} as select * from '{}' (file_format => '{}'{})",
                name, path, file_format, pattern_clause
            )
        };

        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    #[pyo3(signature = (name, access_key_id, secret_access_key, endpoint_url = None, region = None))]
    fn create_s3_connection(
        &mut self,
        name: &str,
        access_key_id: &str,
        secret_access_key: &str,
        endpoint_url: Option<&str>,
        region: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        let endpoint_clause = endpoint_url
            .map(|e| format!(" endpoint_url = '{}'", e))
            .unwrap_or_default();
        let region_clause = region
            .map(|r| format!(" region = '{}'", r))
            .unwrap_or_default();
        let sql = format!(
            "CREATE OR REPLACE CONNECTION {} STORAGE_TYPE = 'S3' access_key_id = '{}' secret_access_key = '{}'{}{}",
            name, access_key_id, secret_access_key, endpoint_clause, region_clause
        );
        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    #[pyo3(signature = (name, endpoint_url, account_name, account_key))]
    fn create_azblob_connection(
        &mut self,
        name: &str,
        endpoint_url: &str,
        account_name: &str,
        account_key: &str,
        py: Python,
    ) -> PyResult<()> {
        let sql = format!(
            "CREATE OR REPLACE CONNECTION {} STORAGE_TYPE = 'AZBLOB' endpoint_url = '{}' account_name = '{}' account_key = '{}'",
            name, endpoint_url, account_name, account_key
        );
        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    #[pyo3(signature = (name, endpoint_url, credential))]
    fn create_gcs_connection(
        &mut self,
        name: &str,
        endpoint_url: &str,
        credential: &str,
        py: Python,
    ) -> PyResult<()> {
        let sql = format!(
            "CREATE OR REPLACE CONNECTION {} STORAGE_TYPE = 'GCS' endpoint_url = '{}' credential = '{}'",
            name, endpoint_url, credential
        );
        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    #[pyo3(signature = (name, endpoint_url, access_key_id, secret_access_key))]
    fn create_oss_connection(
        &mut self,
        name: &str,
        endpoint_url: &str,
        access_key_id: &str,
        secret_access_key: &str,
        py: Python,
    ) -> PyResult<()> {
        let sql = format!(
            "CREATE OR REPLACE CONNECTION {} STORAGE_TYPE = 'OSS' endpoint_url = '{}' access_key_id = '{}' secret_access_key = '{}'",
            name, endpoint_url, access_key_id, secret_access_key
        );
        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    #[pyo3(signature = (name, endpoint_url, access_key_id, secret_access_key))]
    fn create_cos_connection(
        &mut self,
        name: &str,
        endpoint_url: &str,
        access_key_id: &str,
        secret_access_key: &str,
        py: Python,
    ) -> PyResult<()> {
        let sql = format!(
            "CREATE OR REPLACE CONNECTION {} STORAGE_TYPE = 'COS' endpoint_url = '{}' access_key_id = '{}' secret_access_key = '{}'",
            name, endpoint_url, access_key_id, secret_access_key
        );
        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    fn list_connections(&mut self, py: Python) -> PyResult<PyDataFrame> {
        self.sql("SHOW CONNECTIONS", py)
    }

    fn describe_connection(&mut self, name: &str, py: Python) -> PyResult<PyDataFrame> {
        let sql = format!("DESC CONNECTION {}", name);
        self.sql(&sql, py)
    }

    fn drop_connection(&mut self, name: &str, py: Python) -> PyResult<()> {
        let sql = format!("DROP CONNECTION {}", name);
        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    #[pyo3(signature = (name, url, connection_name))]
    fn create_stage(
        &mut self,
        name: &str,
        url: &str,
        connection_name: &str,
        py: Python,
    ) -> PyResult<()> {
        let sql = format!(
            "CREATE OR REPLACE STAGE {} URL='{}' CONNECTION = (connection_name='{}')",
            name, url, connection_name
        );
        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    fn show_stages(&mut self, py: Python) -> PyResult<PyDataFrame> {
        self.sql("SHOW STAGES", py)
    }

    fn list_stages(&mut self, stage_name: &str, py: Python) -> PyResult<PyDataFrame> {
        let sql = format!("LIST @{}", stage_name);
        self.sql(&sql, py)
    }

    fn describe_stage(&mut self, name: &str, py: Python) -> PyResult<PyDataFrame> {
        let sql = format!("DESC STAGE {}", name);
        self.sql(&sql, py)
    }

    fn drop_stage(&mut self, name: &str, py: Python) -> PyResult<()> {
        let sql = format!("DROP STAGE {}", name);
        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }
}

async fn plan_sql(ctx: &Arc<QueryContext>, sql: &str) -> Result<PyDataFrame> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    Ok(PyDataFrame::new(ctx.clone(), plan, default_box_size()))
}
