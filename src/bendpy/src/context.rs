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

use databend_common_catalog::session_type::SessionType;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;
use databend_common_version::BUILD_INFO;
use databend_query::sessions::BuildInfoRef;
use databend_query::sessions::QueryContext;
use databend_query::sessions::Session;
use databend_query::sessions::SessionManager;
use databend_query::sql::Planner;
use pyo3::prelude::*;

use crate::dataframe::default_box_size;
use crate::dataframe::PyDataFrame;
use crate::utils::wait_for_future;
use crate::utils::RUNTIME;

#[pyclass(name = "SessionContext", module = "databend", subclass)]
#[derive(Clone)]
pub(crate) struct PySessionContext {
    pub(crate) session: Arc<Session>,
    version: BuildInfoRef,
}

#[pymethods]
impl PySessionContext {
    #[new]
    #[pyo3(signature = (tenant = None, config = None, local_dir = None))]
    fn new(tenant: Option<&str>, config: Option<&str>, local_dir: Option<&str>, py: Python) -> PyResult<Self> {
        let config = config.unwrap_or("");
        let local_dir = local_dir.unwrap_or(".databend");
        crate::ensure_service_initialized(config, local_dir)?;
        
        let session = RUNTIME.block_on(async {
            let session_manager = SessionManager::instance();
            let mut session = session_manager
                .create_session(SessionType::Local)
                .await
                .unwrap();

            let tenant = tenant.unwrap_or("default");
            let tenant = Tenant::new_or_err(tenant, "PySessionContext::new()").map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Error: {}", e))
            })?;

            session.set_current_tenant(tenant.clone());

            let session = session_manager.register_session(session).unwrap();

            let config = GlobalConfig::instance();
            UserApiProvider::try_create_simple(
                config.meta.to_meta_grpc_client_conf(&BUILD_INFO),
                &tenant,
            )
            .await
            .unwrap();

            let mut user = UserInfo::new_no_auth("root", "%");
            user.grants.grant_privileges(
                &GrantObject::Global,
                UserPrivilegeSet::available_privileges_on_global(),
            );

            session.set_authed_user(user, None).await.unwrap();
            Ok::<Arc<Session>, PyErr>(session)
        })?;

        let mut res = Self {
            session,
            version: &BUILD_INFO,
        };

        res.sql("CREATE DATABASE IF NOT EXISTS default", py)
            .and_then(|df| df.collect(py))?;
        Ok(res)
    }

    fn sql(&mut self, sql: &str, py: Python) -> PyResult<PyDataFrame> {
        let ctx = wait_for_future(py, self.session.create_query_context(self.version)).unwrap();
        let res = wait_for_future(py, plan_sql(&ctx, sql));

        match res {
            Err(err) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Error: {}",
                err
            ))),
            Ok(res) => {
                // if res.df.has_result_set() {
                //     return Ok(res);
                // } else {
                //     return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                //         "Error: sql method only supports SELECT queries",
                //     ));
                // }
                Ok(res)
            }
        }
    }

    fn register_parquet(
        &mut self,
        name: &str,
        path: &str,
        pattern: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        self.register_table(name, path, "parquet", pattern, py)
    }

    fn register_csv(
        &mut self,
        name: &str,
        path: &str,
        pattern: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        self.register_table(name, path, "csv", pattern, py)
    }

    fn register_ndjson(
        &mut self,
        name: &str,
        path: &str,
        pattern: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        self.register_table(name, path, "ndjson", pattern, py)
    }

    fn register_tsv(
        &mut self,
        name: &str,
        path: &str,
        pattern: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        self.register_table(name, path, "tsv", pattern, py)
    }

    fn register_table(
        &mut self,
        name: &str,
        path: &str,
        file_format: &str,
        pattern: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
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

        // Example: select * from '/home/sundy/dataset/hits_p/' (file_format => 'parquet', pattern => '.*.parquet') limit 3;
        let sql = if let Some(pattern) = pattern {
            format!(
                "create view {} as select * from '{}' (file_format => '{}', pattern => '{}')",
                name, path, file_format, pattern
            )
        } else {
            format!(
                "create view {} as select * from '{}' (file_format => '{}')",
                name, path, file_format
            )
        };

        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    fn create_s3_connection(
        &mut self,
        name: &str,
        access_key_id: &str,
        secret_access_key: &str,
        endpoint_url: Option<&str>,
        region: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        let endpoint_clause = endpoint_url.map(|e| format!(" endpoint_url = '{}'", e)).unwrap_or_default();
        let region_clause = region.map(|r| format!(" region = '{}'", r)).unwrap_or_default();
        let sql = format!(
            "CREATE OR REPLACE CONNECTION {} TYPE = 'S3' access_key_id = '{}' secret_access_key = '{}'{}{}", 
            name, access_key_id, secret_access_key, endpoint_clause, region_clause
        );
        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    fn create_azblob_connection(
        &mut self,
        name: &str,
        endpoint_url: &str,
        account_name: &str,
        account_key: &str,
        py: Python,
    ) -> PyResult<()> {
        let sql = format!(
            "CREATE OR REPLACE CONNECTION {} TYPE = 'AZBLOB' endpoint_url = '{}' account_name = '{}' account_key = '{}'",
            name, endpoint_url, account_name, account_key
        );
        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    fn create_gcs_connection(
        &mut self,
        name: &str,
        endpoint_url: &str,
        credential: &str,
        py: Python,
    ) -> PyResult<()> {
        let sql = format!(
            "CREATE OR REPLACE CONNECTION {} TYPE = 'GCS' endpoint_url = '{}' credential = '{}'",
            name, endpoint_url, credential
        );
        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    fn create_oss_connection(
        &mut self,
        name: &str,
        endpoint_url: &str,
        access_key_id: &str,
        secret_access_key: &str,
        py: Python,
    ) -> PyResult<()> {
        let sql = format!(
            "CREATE OR REPLACE CONNECTION {} TYPE = 'OSS' endpoint_url = '{}' access_key_id = '{}' secret_access_key = '{}'",
            name, endpoint_url, access_key_id, secret_access_key
        );
        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }

    fn create_cos_connection(
        &mut self,
        name: &str,
        endpoint_url: &str,
        access_key_id: &str,
        secret_access_key: &str,
        py: Python,
    ) -> PyResult<()> {
        let sql = format!(
            "CREATE OR REPLACE CONNECTION {} TYPE = 'COS' endpoint_url = '{}' access_key_id = '{}' secret_access_key = '{}'",
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
}

async fn plan_sql(ctx: &Arc<QueryContext>, sql: &str) -> Result<PyDataFrame> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    Ok(PyDataFrame::new(ctx.clone(), plan, default_box_size()))
}
