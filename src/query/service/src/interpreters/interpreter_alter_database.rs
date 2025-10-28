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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_sql::planner::binder::ddl::database::DEFAULT_STORAGE_CONNECTION;
use databend_common_sql::planner::binder::ddl::database::DEFAULT_STORAGE_PATH;
use databend_common_sql::plans::AlterDatabasePlan;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct AlterDatabaseInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterDatabasePlan,
}

impl AlterDatabaseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterDatabasePlan) -> Result<Self> {
        Ok(AlterDatabaseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterDatabaseInterpreter {
    fn name(&self) -> &str {
        "AlterDatabaseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "alter_database_execute");
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let database = match catalog
            .get_database(&self.plan.tenant, &self.plan.database)
            .await
        {
            Ok(db) => db,
            Err(err) => {
                if self.plan.if_exists
                    && err.code() == databend_common_exception::ErrorCode::UNKNOWN_DATABASE
                {
                    return Ok(PipelineBuildResult::create());
                }
                return Err(err);
            }
        };

        // Merge provided options with the existing database options
        let mut merged_options = database.options().clone();
        for (key, value) in &self.plan.options {
            merged_options.insert(key.clone(), value.clone());
        }

        let connection_value = merged_options.get(DEFAULT_STORAGE_CONNECTION).cloned();
        let path_value = merged_options.get(DEFAULT_STORAGE_PATH).cloned();

        // Check if both options are present together in the final merged state
        // This ensures that after ALTER, the database still has both options configured
        if connection_value.is_some() != path_value.is_some() {
            return Err(databend_common_exception::ErrorCode::BadArguments(
                "DEFAULT_STORAGE_CONNECTION and DEFAULT_STORAGE_PATH options must be used together",
            ));
        }

        let connection = if let Some(ref connection_name) = connection_value {
            match self.ctx.get_connection(connection_name).await {
                Ok(conn) => Some(conn),
                Err(_) => {
                    return Err(databend_common_exception::ErrorCode::BadArguments(format!(
                        "Connection '{}' does not exist. Please create the connection first using CREATE CONNECTION",
                        connection_name
                    )));
                }
            }
        } else {
            None
        };

        if let (Some(connection), Some(path)) = (connection, path_value.clone()) {
            let connection_name = connection_value
                .as_deref()
                .expect("connection name must exist when connection is Some");

            let uri_for_scheme =
                databend_common_ast::ast::UriLocation::from_uri(path.clone(), BTreeMap::new())
                    .map_err(|e| {
                        databend_common_exception::ErrorCode::BadArguments(format!(
                            "Invalid storage path '{}': {}",
                            path, e
                        ))
                    })?;

            let path_protocol = uri_for_scheme.protocol.to_ascii_lowercase();
            let connection_protocol = connection.storage_type.to_ascii_lowercase();

            if path_protocol != connection_protocol {
                return Err(databend_common_exception::ErrorCode::BadArguments(format!(
                    "{} protocol '{}' does not match connection '{}' protocol '{}'",
                    DEFAULT_STORAGE_PATH,
                    uri_for_scheme.protocol,
                    connection_name,
                    connection.storage_type
                )));
            }

            let mut uri_location = databend_common_ast::ast::UriLocation::from_uri(
                path.clone(),
                connection.storage_params,
            )?;

            let storage_params = databend_common_sql::binder::parse_storage_params_from_uri(
                &mut uri_location,
                Some(&*self.ctx),
                "when setting database DEFAULT_STORAGE_PATH",
            )
            .await
            .map_err(|e| {
                databend_common_exception::ErrorCode::BadArguments(format!(
                    "Invalid storage path '{}': {}",
                    path, e
                ))
            })?;

            if !storage_params.is_secure()
                && !databend_common_config::GlobalConfig::instance()
                    .storage
                    .allow_insecure
            {
                return Err(databend_common_exception::ErrorCode::StorageInsecure(
                    "Database default storage path points to insecure storage, which is not allowed",
                ));
            }

            let operator =
                databend_common_storage::init_operator(&storage_params).map_err(|e| {
                    databend_common_exception::ErrorCode::BadArguments(format!(
                        "Failed to access storage location '{}': {}",
                        path, e
                    ))
                })?;

            databend_common_sql::binder::verify_external_location_privileges(operator)
                .await
                .map_err(|e| {
                    databend_common_exception::ErrorCode::BadArguments(format!(
                    "Failed to verify permissions for database default storage location '{}': {}",
                    path, e
                ))
                })?;
        }

        // Persist the merged options through the catalog database interface
        database.update_options(merged_options).await?;

        Ok(PipelineBuildResult::create())
    }
}
