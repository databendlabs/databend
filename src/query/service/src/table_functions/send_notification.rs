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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_cloud_control::client_config::build_client_config;
use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb::EnqueueNotificationRequest;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_storages_fuse::table_functions::SimpleTableFunc;

pub struct SendNotification {
    name: String,
    message: String,
}

#[async_trait::async_trait]
impl SimpleTableFunc for SendNotification {
    fn table_args(&self) -> Option<TableArgs> {
        Some(TableArgs::new_positioned(vec![
            Scalar::String(self.name.clone()),
            Scalar::String(self.message.clone()),
        ]))
    }

    fn schema(&self) -> TableSchemaRef {
        // The binder requires at least one column; execution never emits a row.
        TableSchemaRefExt::create(vec![TableField::new("result", TableDataType::String)])
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        if GlobalConfig::instance()
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot send notification without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let query_id = ctx.get_id();
        let tenant = ctx.get_tenant().tenant_name().to_string();
        let user = ctx.get_current_user()?.identity().display().to_string();
        let mut config = build_client_config(
            tenant.clone(),
            user,
            query_id.clone(),
            cloud_api.get_timeout(),
        );
        config.add_notification_version_info();
        let request = EnqueueNotificationRequest {
            tenant_id: tenant,
            name: self.name.clone(),
            message: self.message.clone(),
            query_id: Some(query_id),
        };
        // The local, single-source template invokes apply once, and only when
        // the pipeline is executed. It produces no rows, so CALL and SELECT
        // return the placeholder `result` column with zero rows.
        cloud_api
            .get_notification_client()
            .enqueue_notification(make_request(request, config))
            .await?;
        Ok(None)
    }

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self> {
        let args = table_args.expect_all_positioned(func_name, Some(2))?;
        let name = args[0].as_string().cloned().ok_or_else(|| {
            ErrorCode::BadArguments(format!("{func_name} expects a string integration name"))
        })?;
        let message = args[1].as_string().cloned().ok_or_else(|| {
            ErrorCode::BadArguments(format!("{func_name} expects a string message"))
        })?;
        if name.is_empty() {
            return Err(ErrorCode::BadArguments(
                "notification integration name must not be empty",
            ));
        }
        if message.is_empty() || message.len() > 262_144 {
            return Err(ErrorCode::BadArguments(
                "notification message must contain between 1 and 262144 UTF-8 bytes",
            ));
        }
        Ok(Self { name, message })
    }
}
