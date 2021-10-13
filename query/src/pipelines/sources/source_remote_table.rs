// use crate::pipelines::sources::source::Source;
// use std::task::{Context, Poll};
// use common_datablocks::DataBlock;
// use crate::sessions::DatabendQueryContextRef;
// use common_planners::ReadDataSourcePlan;
// use common_streams::{SendableDataBlockStream, DataBlockStream};
// use futures::{StreamExt};
// use common_exception::{Result};
// use crate::catalogs::TablePtr;
//
//
// struct RemoteTableSource {
//     context: DatabendQueryContextRef,
//     source_plan: ReadDataSourcePlan,
//     inner_stream: SendableDataBlockStream,
// }
//
// // TODO(Winter): unsafe. remove this.
// unsafe impl Sync for RemoteTableSource {}
//
// impl RemoteTableSource {
//     pub fn try_create(ctx: DatabendQueryContextRef, plan: ReadDataSourcePlan) -> Result<Self> {
//         let empty_stream = DataBlockStream::create(plan.schema(), None, vec![]);
//
//         Ok(RemoteTableSource {
//             context: ctx,
//             source_plan: plan,
//             inner_stream: Box::pin(empty_stream),
//         })
//     }
//
//     fn get_table(context: &DatabendQueryContextRef, plan: &ReadDataSourcePlan) -> Result<TablePtr> {
//         let ReadDataSourcePlan { table_info, }
//         let ReadDataSourcePlan { db, table, table_id, tbl_args, table_version, .. } = plan;
//
//         match tbl_args {
//             None => Ok(
//                 context.get_table_by_id(db, *table_id, *table_version)?
//                     .raw()
//                     .clone()
//             ),
//             Some(_) => Ok(context.get_table_function(&table, tbl_args.clone())?
//                 .raw()
//                 .clone()
//                 .as_table()
//             ),
//         }
//     }
// }
//
// #[async_trait::async_trait]
// impl Source for RemoteTableSource {
//     const NAME: &'static str = "RemoteTable";
//
//     async fn ready(&mut self) -> Result<()> {
//         let table = Self::get_table(&self.context, &self.source_plan)?;
//         let get_stream = table.read(self.context.clone(), &self.source_plan);
//         self.inner_stream = Box::pin(self.context.try_create_abortable(get_stream.await?)?);
//         Ok(())
//     }
//
//     #[inline]
//     fn generate(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<DataBlock>>> {
//         self.inner_stream.poll_next_unpin(cx)
//     }
// }
