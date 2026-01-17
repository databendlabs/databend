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
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::utils::FromData;
use databend_common_meta_api::tag_api::TagApi;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_users::UserApiProvider;

use crate::meta_service_error;
use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct TagsTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for TagsTable {
    const NAME: &'static str = "system.tags";

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
        let meta_client = UserApiProvider::instance().get_meta_store_client();
        let mut tags = meta_client
            .list_tags(&tenant)
            .await
            .map_err(meta_service_error)?;
        tags.sort_by(|a, b| a.name.cmp(&b.name));

        let mut names = Vec::with_capacity(tags.len());
        let mut allowed_values = Vec::with_capacity(tags.len());
        let mut comments = Vec::with_capacity(tags.len());
        let mut created_on = Vec::with_capacity(tags.len());

        for tag in tags {
            names.push(tag.name);
            if let Some(values) = tag.meta.data.allowed_values.as_ref() {
                allowed_values.push(Some(format!(
                    "[{}]",
                    values
                        .iter()
                        .map(|v| format!("'{}'", v.replace('\'', "\\'")))
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            } else {
                allowed_values.push(None);
            }
            comments.push(tag.meta.data.comment.clone());
            created_on.push(tag.meta.data.created_on.timestamp_micros());
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_opt_data(allowed_values),
            StringType::from_data(comments),
            TimestampType::from_data(created_on),
        ]))
    }
}

impl TagsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new(
                "allowed_values",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("comment", TableDataType::String),
            TableField::new("created_on", TableDataType::Timestamp),
        ]);
        let table_info = TableInfo {
            desc: "'system'.'tags'".to_string(),
            name: "tags".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemTags".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        AsyncOneBlockSystemTable::create(TagsTable { table_info })
    }
}
