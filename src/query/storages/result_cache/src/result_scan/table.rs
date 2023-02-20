use std::any::Any;
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::Scalar;
use common_meta_app::schema::TableInfo;
use common_pipeline_core::Pipeline;

pub struct ResultScan {
    table_info: TableInfo,
    query_id: String,
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
        // pipeline.add_source(
        //     |output| AsyncCrashMeSource::create(ctx.clone(), output, self.panic_message.clone()),
        //     1,
        // )?;

        Ok(())
    }
}
