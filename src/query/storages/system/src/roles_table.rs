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
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_users::UserApiProvider;
use itertools::Itertools;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct RolesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for RolesTable {
    const NAME: &'static str = "system.roles";

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
        let roles = UserApiProvider::instance()
            .get_roles(
                &tenant,
                ctx.get_settings().get_enable_upgrade_meta_data_to_pb()?,
            )
            .await?;

        let names: Vec<&str> = roles.iter().map(|x| x.name.as_str()).collect();
        let inherited_roles: Vec<u64> = roles
            .iter()
            .map(|x| x.grants.roles().len() as u64)
            .collect();
        let inherited_roles_names: Vec<String> = roles
            .iter()
            .map(|x| x.grants.roles().iter().sorted().join(", ").to_string())
            .collect();

        let created_on = roles
            .iter()
            .map(|role| role.created_on.timestamp_micros())
            .collect();
        let update_on = roles
            .iter()
            .map(|role| role.update_on.timestamp_micros())
            .collect();

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            UInt64Type::from_data(inherited_roles),
            StringType::from_data(inherited_roles_names),
            TimestampType::from_data(created_on),
            TimestampType::from_data(update_on),
        ]))
    }
}

impl RolesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new(
                "inherited_roles",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("inherited_roles_name", TableDataType::String),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new("update_on", TableDataType::Timestamp),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'roles'".to_string(),
            name: "roles".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemRoles".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        AsyncOneBlockSystemTable::create(RolesTable { table_info })
    }
}
