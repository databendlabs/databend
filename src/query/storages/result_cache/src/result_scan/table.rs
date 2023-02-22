use std::any::Any;
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_context::TableContext;
use common_catalog::table_function::TableFunction;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::TableSchema;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;
use common_storages_fuse::table_functions::string_value;
use common_users::UserApiProvider;

use crate::ResultCacheReader;

const RESULT_SCAN: &str = "result_scan";

pub struct ResultScan {
    table_info: TableInfo,
    query_id: String,
}

impl ResultScan {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let query_id = parse_result_scan_args(&table_args)?;

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{database_name}'.'{table_func_name}'"),
            name: String::from(table_func_name),
            meta: TableMeta {
                schema: Arc::new(TableSchema::empty()),
                engine: String::from(RESULT_SCAN),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(ResultScan {
            table_info,
            query_id,
        }))
    }
}

#[async_trait::async_trait]
impl Table for ResultScan {
    fn is_local(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        _: Arc<dyn TableContext>,
        _: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        // dummy statistics
        Ok((PartStatistics::new_exact(1, 1, 1, 1), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        let args = vec![Scalar::String(self.query_id.as_bytes().to_vec())];

        Some(TableArgs::new_positioned(args))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_source(
            |output| ResultScanSource::create(ctx.clone(), output, self.query_id.to_owned()),
            1,
        )?;

        Ok(())
    }
}

struct ResultScanSource {
    finish: bool,
    ctx: Arc<dyn TableContext>,
    arg_query_id: String,
}

impl ResultScanSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        arg_query_id: String,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, ResultScanSource {
            ctx,
            finish: false,
            arg_query_id,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for ResultScanSource {
    const NAME: &'static str = "fuse_segment";

    #[async_trait::unboxed_simple]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finish {
            return Ok(None);
        }

        self.finish = true;
        let meta_key = self.ctx.get_result_cache_key(&self.arg_query_id);
        if self.ctx.get_settings().get_enable_query_result_cache()? && meta_key.is_some() {
            // 1. Try to get result from cache.
            let kv_store = UserApiProvider::instance().get_meta_store_client();
            let cache_reader = ResultCacheReader::create(
                self.ctx.clone(),
                kv_store.clone(),
                self.ctx
                    .get_settings()
                    .get_tolerate_inconsistent_result_cache()?,
            );

            let blocks = cache_reader
                .try_read_cached_result_with_meta_key(meta_key.unwrap())
                .await?;
            return match blocks {
                Some(blocks) => {
                    // 2.1 If found, return the result directly.
                    Ok(Some(DataBlock::concat(&blocks)?))
                }
                None => Ok(None),
            };
        }
        Ok(None)
    }
}

impl TableFunction for ResultScan {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

fn parse_result_scan_args(table_args: &TableArgs) -> Result<String> {
    let args = table_args.expect_all_positioned(RESULT_SCAN, Some(1))?;
    string_value(&args[0])
}
