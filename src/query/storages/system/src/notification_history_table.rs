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

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_cloud_control::client_config::build_client_config;
use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::notification_utils;
use databend_common_cloud_control::pb::ListNotificationHistoryRequest;
use databend_common_cloud_control::pb::NotificationHistory;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::infer_table_schema;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_sql::plans::notification_history_schema;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::extract_leveled_strings;

fn parse_history_to_block(histories: Vec<NotificationHistory>) -> Result<DataBlock> {
    let mut created_on: Vec<i64> = Vec::with_capacity(histories.len());
    let mut processed: Vec<Option<i64>> = Vec::with_capacity(histories.len());
    let mut message_source: Vec<String> = Vec::with_capacity(histories.len());
    let mut integration_name: Vec<String> = Vec::with_capacity(histories.len());
    let mut message: Vec<String> = Vec::with_capacity(histories.len());
    let mut status: Vec<String> = Vec::with_capacity(histories.len());
    let mut error_message: Vec<String> = Vec::with_capacity(histories.len());

    for history in histories {
        let h: notification_utils::NotificationHistory = history.try_into()?;
        created_on.push(h.created_time.timestamp_micros());
        processed.push(h.processed_time.map(|v| v.timestamp_micros()));
        message_source.push(h.message_source.clone());
        integration_name.push(h.name.clone());
        message.push(h.message.clone());
        status.push(h.status.clone());
        error_message.push(h.error_message.clone());
    }

    Ok(DataBlock::new_from_columns(vec![
        TimestampType::from_data(created_on),
        TimestampType::from_opt_data(processed),
        StringType::from_data(message_source),
        StringType::from_data(integration_name),
        StringType::from_data(message),
        StringType::from_data(status),
        StringType::from_data(error_message),
    ]))
}

pub struct NotificationHistoryTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for NotificationHistoryTable {
    const NAME: &'static str = "system.notification_history";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let config = GlobalConfig::instance();
        if config
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot view system.notification_history table without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }

        let tenant = ctx.get_tenant();
        let query_id = ctx.get_id();
        let user = ctx.get_current_user()?.identity().display().to_string();

        let result_limit = push_downs
            .as_ref()
            .map(|v| v.limit.map(|i| i as i32))
            .unwrap_or(None);
        let mut notification_name = None;
        if let Some(push_downs) = push_downs {
            if let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter) {
                let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
                let func_ctx = ctx.get_function_context()?;
                let (name, _) = extract_leveled_strings(&expr, &["integration_name"], &func_ctx)?;
                // find_filters will collect integration_name = xx or integration_name = yy.
                // So if name.len() != 1 integration_name should be None.
                if name.len() == 1 {
                    notification_name = Some(name[0].clone())
                };
            }
        }

        let req = ListNotificationHistoryRequest {
            tenant_id: tenant.tenant_name().to_string(),
            result_limit,
            notification_name,
            ..Default::default()
        };

        let cloud_api = CloudControlApiProvider::instance();
        let notification_client = cloud_api.get_notification_client();
        let mut cfg = build_client_config(
            tenant.tenant_name().to_string(),
            user,
            query_id,
            cloud_api.get_timeout(),
        );
        cfg.add_notification_version_info();
        let req = make_request(req, cfg);

        let resp = notification_client.list_notification_histories(req).await?;
        let histories = resp.notification_histories;

        parse_history_to_block(histories)
    }
}

impl NotificationHistoryTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = infer_table_schema(&notification_history_schema())
            .expect("failed to parse notification history table schema");

        let table_info = TableInfo {
            desc: "'system'.'notification_history'".to_string(),
            name: "notification_history".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemNotificationHistory".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}
