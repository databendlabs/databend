// use common_datablocks::{HashMethodKind, HashMethod, DataBlock};
// use common_streams::SendableDataBlockStream;
// use futures::StreamExt;
// use common_datavalues::prelude::Series;
// use common_exception::Result;
// use common_functions::aggregates::StateAddr;
// use crate::common::{HashTable, DefaultHashTableEntity, DefaultHasher};
// use common_infallible::RwLock;
// use common_datavalues::columns::DataColumn;
//
// pub struct AggregatorWithU32<Method: HashMethod> {
//     method: Method,
//     aggregator_table: RwLock<HashTable<u32, DefaultHashTableEntity<u32, usize>, DefaultHasher<u32>>>,
// }
//
// impl<Method: HashMethod> AggregatorWithU32<Method> {
//     pub fn create(method: Method) -> AggregatorWithU32<Method> {
//         AggregatorWithU32 {
//             method,
//             aggregator_table: RwLock::new(HashTable::new()),
//         }
//     }
//
//     pub async fn aggregator(
//         &self,
//         mut stream: SendableDataBlockStream,
//         group_cols: Vec<String>,
//         aggr_cols: Vec<String>,
//     ) {
//         while let Some(block) = stream.next().await {
//             let block = block?;
//             // 1.1 and 1.2.
//             let group_columns = Self::get_group_columns(&block, &group_cols)?;
//             let aggr_arg_columns = Self::get_aggregate_columns(&block, &aggr_cols)?;
//
//             // this can benificial for the case of dereferencing
//             let aggr_arg_columns_slice = &aggr_arg_columns;
//
//             let mut places = Vec::with_capacity(block.num_rows());
//             let group_keys = self.method.build_keys(&group_columns, block.num_rows())?;
//
//             // TODO: remove lock
//             let mut aggregator_table = self.aggregator_table.write();
//             for group_key in group_keys.iter() {
//                 let mut inserted = true;
//                 let entity = aggregator_table.insert_key(group_key, inserted);
//
//                 match inserted {
//                     true if aggr_funcs_len == 0 => {
//                         entity.set_value(0);
//                     }
//                     true => {
//                         let place: StateAddr = arena.alloc_layout(layout).into();
//                         for idx in 0..aggr_len {
//                             let arg_place = place.next(offsets_aggregate_states[idx]);
//                             funcs[idx].init_state(arg_place);
//                         }
//                         places.push(place);
//                         entity.set_value(place.addr());
//                     }
//                     false => {
//                         let place: StateAddr = entity.get_value().into();
//                         places.push(place);
//                     }
//                 }
//             }
//
//             for ((idx, func), args) in funcs.iter().enumerate().zip(aggr_arg_columns_slice.iter()) {
//                 func.accumulate_keys(
//                     &places,
//                     offsets_aggregate_states[idx],
//                     args,
//                     block.num_rows(),
//                 )?;
//             }
//         }
//     }
//
//     #[inline(always)]
//     fn get_group_columns(block: &DataBlock, group_cols: &[String]) -> Result<Vec<&DataColumn>> {
//         group_cols.iter()
//             .map(|column_name| block.try_column_by_name(column_name))
//             .collect::<Result<Vec<_>>>()
//     }
//
//     fn get_aggregate_columns(block: &DataBlock, aggr_cols: &[String]) -> Result<Vec<Vec<Series>>> {
//         let mut aggr_arg_columns = Vec::with_capacity(aggr_cols.len());
//         for (idx, _aggr_col) in aggr_cols.iter().enumerate() {
//             let arg_columns = arg_names[idx]
//                 .iter()
//                 .map(|arg| block.try_column_by_name(arg).and_then(|c| c.to_array()))
//                 .collect::<Result<Vec<Series>>>()?;
//             aggr_arg_columns.push(arg_columns);
//         }
//         aggr_arg_columns
//     }
// }
