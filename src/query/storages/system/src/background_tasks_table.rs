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
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_api::BackgroundApi;
use databend_common_meta_app::background::ListBackgroundTasksReq;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_users::UserApiProvider;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct BackgroundTaskTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for BackgroundTaskTable {
    const NAME: &'static str = "system.background_tasks";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let tasks = meta_api
            .list_background_tasks(ListBackgroundTasksReq::new(&tenant))
            .await?;
        let mut names = Vec::with_capacity(tasks.len());
        let mut types = Vec::with_capacity(tasks.len());
        let mut stats = Vec::with_capacity(tasks.len());
        let mut messages = Vec::with_capacity(tasks.len());
        let mut database_ids = Vec::with_capacity(tasks.len());
        let mut table_ids = Vec::with_capacity(tasks.len());
        let mut compaction_stats = Vec::with_capacity(tasks.len());
        let mut vacuum_stats = Vec::with_capacity(tasks.len());
        let mut task_run_secs = Vec::with_capacity(tasks.len());
        let mut creators = Vec::with_capacity(tasks.len());
        let mut trigger = Vec::with_capacity(tasks.len());
        let mut create_timestamps = Vec::with_capacity(tasks.len());
        let mut update_timestamps = Vec::with_capacity(tasks.len());
        for (_, name, task) in tasks {
            names.push(name);
            types.push(task.task_type.to_string());
            stats.push(task.task_state.to_string());
            messages.push(task.message);
            compaction_stats.push(
                task.compaction_task_stats
                    .as_ref()
                    .map(|s| serde_json::to_vec(s).unwrap_or_default()),
            );
            vacuum_stats.push(
                task.vacuum_stats
                    .as_ref()
                    .map(|s| serde_json::to_vec(s).unwrap_or_default()),
            );
            if let Some(compact_stats) = task.compaction_task_stats.as_ref() {
                database_ids.push(compact_stats.db_id);
                table_ids.push(compact_stats.table_id);
                task_run_secs.push(compact_stats.total_compaction_time.map(|s| s.as_secs()));
            } else {
                database_ids.push(0);
                table_ids.push(0);
                task_run_secs.push(None);
            }
            creators.push(task.creator.map(|s| s.to_string()));
            trigger.push(task.manual_trigger.map(|s| s.trigger.display().to_string()));
            create_timestamps.push(task.created_at.timestamp_micros());
            update_timestamps.push(task.last_updated.unwrap_or_default().timestamp_micros());
        }
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(types),
            StringType::from_data(stats),
            StringType::from_data(messages),
            NumberType::from_data(database_ids),
            NumberType::from_data(table_ids),
            VariantType::from_opt_data(compaction_stats),
            VariantType::from_opt_data(vacuum_stats),
            NumberType::from_opt_data(task_run_secs),
            StringType::from_opt_data(creators),
            StringType::from_opt_data(trigger),
            TimestampType::from_data(create_timestamps),
            TimestampType::from_data(update_timestamps),
        ]))
    }
}

impl BackgroundTaskTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("id", TableDataType::String),
            TableField::new("type", TableDataType::String),
            TableField::new("state", TableDataType::String),
            TableField::new("message", TableDataType::String),
            TableField::new("database_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("table_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("compaction_stats", TableDataType::Variant.wrap_nullable()),
            TableField::new("vacuum_stats", TableDataType::Variant.wrap_nullable()),
            TableField::new(
                "task_running_secs",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new("creator", TableDataType::String.wrap_nullable()),
            TableField::new("trigger", TableDataType::String.wrap_nullable()),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new("updated_on", TableDataType::Timestamp),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'background_tasks'".to_string(),
            name: "background_tasks".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemBackgroundTasks".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}
