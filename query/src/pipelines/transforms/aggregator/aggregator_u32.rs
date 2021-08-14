// use std::pin::Pin;
// use std::task::Context;
// use std::task::Poll;
//
// use futures::{Stream, StreamExt};
//
// use common_datablocks::{DataBlock, HashMethod, HashMethodKind};
// use common_datavalues::columns::DataColumn;
// use common_datavalues::prelude::Series;
// use common_exception::{Result, ErrorCode};
// use common_functions::aggregates::{StateAddr, AggregateFunctionRef};
// use common_infallible::RwLock;
// use common_streams::SendableDataBlockStream;
//
// use crate::common::{DefaultHasher, DefaultHashTableEntity, HashTable};
// use common_planners::Expression;
// use common_datavalues::DataSchemaRef;
//
// pub struct AggregatorWithU32 {
//     aggregator_exprs: Vec<Expression>,
// }
//
// impl AggregatorWithU32 {
//     pub fn create(
//         schema: DataSchemaRef,
//         group_exprs: Vec<Expression>,
//         aggregate_exprs: Vec<Expression>,
//     ) -> Result<AggregatorWithU32> {
//         let mut funcs = Vec::with_capacity(aggregate_exprs.len());
//         let mut arg_names = Vec::with_capacity(aggregate_exprs.len());
//         let mut aggr_cols = Vec::with_capacity(aggregate_exprs.len());
//
//         for agg_expression in aggregate_exprs.iter() {
//             funcs.push(agg_expression.to_aggregate_function(&schema)?);
//             arg_names.push(agg_expression.to_aggregate_function_names()?);
//             aggr_cols.push(agg_expression.column_name());
//         }
//
//         let group_cols = group_exprs
//             .iter()
//             .map(|x| x.column_name())
//             .collect::<Vec<_>>();
//
//         Err(ErrorCode::UnImplement(""))
//         // AggregatorWithU32 {
//         //
//         //     // method,
//         //     // aggregator_table: RwLock::new(HashTable::new()),
//         // }
//     }
//
//     fn append_aggregate<M: HashMethod>(&self, method: M, input: SendableDataBlockStream) {
//         type Hasher = DefaultHasher<M::HashKey>;
//         type Entity = DefaultHashTableEntity<M::HashKey, usize>;
//         let aggregated_data = HashTable::<M::HashKey, Entity, Hasher>::new();
//
//         // let (funcs, arg_names, aggr_cols) = self.init()?;
//
//         // TODO:
//     }
//
//     // pub async fn aggregator(
//     //     &self,
//     //     mut stream: SendableDataBlockStream,
//     //     group_cols: Vec<String>,
//     //     aggr_cols: Vec<String>,
//     // ) {
//     //     while let Some(block) = stream.next().await {}
//     // }
//     //
//     // #[inline(always)]
//     // fn get_group_columns(block: &DataBlock, group_cols: &[String]) -> Result<Vec<&DataColumn>> {
//     //     group_cols.iter()
//     //         .map(|column_name| block.try_column_by_name(column_name))
//     //         .collect::<Result<Vec<_>>>()
//     // }
//     //
//     // fn get_aggregate_columns(block: &DataBlock, aggr_cols: &[String]) -> Result<Vec<Vec<Series>>> {
//     //     let mut aggr_arg_columns = Vec::with_capacity(aggr_cols.len());
//     //     for (idx, _aggr_col) in aggr_cols.iter().enumerate() {
//     //         let arg_columns = arg_names[idx]
//     //             .iter()
//     //             .map(|arg| block.try_column_by_name(arg).and_then(|c| c.to_array()))
//     //             .collect::<Result<Vec<Series>>>()?;
//     //         aggr_arg_columns.push(arg_columns);
//     //     }
//     //     aggr_arg_columns
//     // }
// }
//
// struct AggregatorPartialStream<Method: HashMethod> {
//     method: Method,
//     inner: SendableDataBlockStream,
//     aggregated_data: HashTable<Method::HashKey, DefaultHashTableEntity<Method::HashKey, usize>, DefaultHasher<Method::HashKey>>,
// }
//
// impl<Method: HashMethod> AggregatorPartialStream<Method> {
//     fn aggregate(&mut self, data_block: Result<DataBlock>) -> Result<()> {
//         let group_cols: Vec<String> = vec![];
//         let aggr_cols: Vec<String> = vec![];
//
//         let data_block = data_block?;
//
//         // 1.1 and 1.2.
//         let mut group_columns = Vec::with_capacity(group_cols.len());
//         {
//             for col in group_cols.iter() {
//                 group_columns.push(data_block.try_column_by_name(col)?);
//             }
//         }
//
//         let mut aggr_arg_columns = Vec::with_capacity(aggr_cols.len());
//         for (idx, _aggr_col) in aggr_cols.iter().enumerate() {
//             let arg_columns = arg_names[idx]
//                 .iter()
//                 .map(|arg| data_block.try_column_by_name(arg).and_then(|c| c.to_array()))
//                 .collect::<Result<Vec<Series>>>()?;
//             aggr_arg_columns.push(arg_columns);
//         }
//
//         // this can benificial for the case of dereferencing
//         let aggr_arg_columns_slice = &aggr_arg_columns;
//
//         let mut places = Vec::with_capacity(data_block.num_rows());
//         let group_keys = self.method.build_keys(&group_columns, data_block.num_rows())?;
//         {
//             let aggregated_data = &mut self.aggregated_data;
//             for group_key in group_keys.iter() {
//                 let mut inserted = true;
//                 let entity = aggregated_data.insert_key(group_key, &mut inserted);
//
//                 match inserted {
//                     true => {
//                         if aggr_funcs_len == 0 {
//                             entity.set_value(0);
//                         } else {
//                             let place: StateAddr = arena.alloc_layout(layout).into();
//                             for idx in 0..aggr_len {
//                                 let arg_place =
//                                     place.next(offsets_aggregate_states[idx]);
//                                 funcs[idx].init_state(arg_place);
//                             }
//                             places.push(place);
//                             entity.set_value(place.addr());
//                         }
//                     }
//                     false => {
//                         let place: StateAddr = (*entity.get_value()).into();
//                         places.push(place);
//                     }
//                 }
//             }
//
//             for ((idx, func), args) in
//             funcs.iter().enumerate().zip(aggr_arg_columns_slice.iter())
//             {
//                 func.accumulate_keys(
//                     &places,
//                     offsets_aggregate_states[idx],
//                     args,
//                     data_block.num_rows(),
//                 )?;
//             }
//         }
//
//         Ok(())
//     }
// }
//
// impl<Method: HashMethod> Stream for AggregatorPartialStream<Method> {
//     type Item = ();
//
//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         while let Poll::Ready(data_block) = self.inner.poll_next_unpin(cx) {
//             match data_block {
//                 None => { return Poll::Ready(None); }
//                 Some(data_block) => { self.aggregate(data_block)?; }
//             }
//         }
//
//         Poll::Pending
//     }
// }
//
