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

use std::any::Any;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use chrono::DateTime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression::udf_client::UDFFlightClient;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::OneBlockSource;
use databend_common_storages_factory::Table;
use url::Url;

pub struct UdfEchoTable {
    table_info: TableInfo,
    arg: String,
    address: String,
}

impl UdfEchoTable {
    pub fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![TableField::new("result", TableDataType::String)])
    }

    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = table_args.expect_all_positioned(table_func_name, Some(2))?;
        let mut args = TableArgs::expect_all_strings(args)?;
        let arg = args.pop().unwrap();
        let address = args.pop().unwrap();

        {
            let url_addr = Url::parse(&address)
                .map_err_to_code(ErrorCode::InvalidArgument, || {
                    format!("udf server address '{address}' is invalid, please check the address",)
                })?;

            let udf_server_allow_list =
                &GlobalConfig::instance().query.common.udf_server_allow_list;
            if udf_server_allow_list.iter().all(|allow_url| {
                if let Ok(allow_url) = Url::parse(allow_url) {
                    allow_url.host_str() != url_addr.host_str()
                } else {
                    true
                }
            }) {
                return Err(ErrorCode::InvalidArgument(format!(
                    "Unallowed UDF server address, '{address}' is not in udf_server_allow_list"
                )));
            }
        }

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("udf_echo"),
            meta: TableMeta {
                schema: Self::schema(),
                engine: String::from(table_func_name),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(UdfEchoTable {
            table_info,
            address,
            arg,
        }))
    }
}

#[async_trait::async_trait]
impl Table for UdfEchoTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        // dummy statistics
        let settings = ctx.get_settings();
        let connect_timeout = settings.get_external_server_connect_timeout_secs()?;
        let request_timeout = settings.get_external_server_request_timeout_secs()?;

        let endpoint = UDFFlightClient::build_endpoint(
            &self.address,
            connect_timeout,
            request_timeout,
            &ctx.get_version().udf_client_user_agent(),
        )?;
        let name = "builtin_echo";
        let mut client = UDFFlightClient::connect(name, endpoint, connect_timeout, 65536)
            .await?
            .with_tenant(ctx.get_tenant().tenant_name())?
            .with_func_name(name)?
            .with_handler_name(name)?
            .with_query_id(&ctx.get_id())?;

        let num_rows = 1;
        let block_entries = vec![BlockEntry::new_const_column(
            DataType::String,
            Scalar::String(self.arg.clone()),
            1,
        )];
        let return_type = DataType::Nullable(Box::new(DataType::String));

        let result = client
            .do_exchange(name, name, Some(num_rows), block_entries, &return_type)
            .await?;

        let scalar = unsafe { result.get_by_offset(0).index_unchecked(0) };
        let value = scalar.as_string().unwrap();
        let parts = vec![Arc::new(Box::new(StringPart {
            value: value.to_string(),
        }) as _)];
        Ok((
            PartStatistics::new_exact(1, 1, 1, 1),
            Partitions::create(PartitionsShuffleKind::Seq, parts),
        ))
    }

    fn table_args(&self) -> Option<TableArgs> {
        let args = vec![
            Scalar::String(self.address.clone()),
            Scalar::String(self.arg.clone()),
        ];
        Some(TableArgs::new_positioned(args))
    }

    fn read_data(
        &self,
        _ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        let part = plan.parts.partitions.first().unwrap();
        let part = part.as_any().downcast_ref::<StringPart>().unwrap();

        pipeline.add_source(
            move |output| {
                let columns = vec![StringType::from_data(vec![part.value.clone()])];
                let data_block = DataBlock::new_from_columns(columns);
                OneBlockSource::create(output, data_block)
            },
            1,
        )?;

        Ok(())
    }
}

impl TableFunction for UdfEchoTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq)]
pub struct StringPart {
    pub value: String,
}

#[typetag::serde(name = "string_part")]
impl PartInfo for StringPart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<StringPart>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.value.hash(&mut s);
        s.finish()
    }

    fn part_type(&self) -> PartInfoType {
        PartInfoType::BlockLevel
    }
}
