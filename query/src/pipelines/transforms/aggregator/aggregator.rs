use std::alloc::Layout;

use bumpalo::Bump;
use futures::StreamExt;

use common_datablocks::{DataBlock, HashMethod, HashMethodFixedKeys};
use common_datavalues::{DataSchema, DataSchemaRefExt, UInt32Type, DataSchemaRef, DFNumericType};
use common_datavalues::arrays::{ArrayBuilder, BinaryArrayBuilder, DFUInt32ArrayBuilder, PrimitiveArrayBuilder};
use common_datavalues::prelude::{Arc, IntoSeries, Series};
use common_functions::aggregates::{AggregateFunction, AggregateFunctionRef, StateAddr, get_layout_offsets};
use common_infallible::RwLock;
use common_io::prelude::BytesMut;
use common_streams::{DataBlockStream, SendableDataBlockStream};

use crate::common::{DefaultHashTableEntity, HashTable, HashTableEntity, DefaultHasher, KeyHasher, HashMap};
use common_planners::Expression;
use common_exception::Result;
use std::hash::Hash;
use common_datavalues::series::SeriesWrap;
use common_arrow::arrow::array::ArrayRef;

pub struct Aggregator<Method: HashMethod> {
    method: Method,
    funcs: Vec<AggregateFunctionRef>,
    arg_names: Vec<Vec<String>>,
    aggr_cols: Vec<String>,
    layout: Layout,
    offsets_aggregate_states: Vec<usize>,
}

impl<Method: HashMethod> Aggregator<Method>
    where DefaultHasher<Method::HashKey>: KeyHasher<Method::HashKey>,
          DefaultHashTableEntity<Method::HashKey, usize>: HashTableEntity<Method::HashKey>
{
    pub fn create(
        method: Method,
        aggr_exprs: &[Expression],
        schema: DataSchemaRef,
    ) -> Result<Aggregator<Method>> {
        let mut funcs = Vec::with_capacity(aggr_exprs.len());
        let mut arg_names = Vec::with_capacity(aggr_exprs.len());
        let mut aggr_cols = Vec::with_capacity(aggr_exprs.len());

        for aggr_expr in aggr_exprs.iter() {
            funcs.push(aggr_expr.to_aggregate_function(&schema)?);
            arg_names.push(aggr_expr.to_aggregate_function_names()?);
            aggr_cols.push(aggr_expr.column_name());
        }

        let (layout, offsets_aggregate_states) = unsafe { get_layout_offsets(&funcs) };

        Ok(Aggregator { method, funcs, arg_names, aggr_cols, layout, offsets_aggregate_states })
    }

    pub async fn aggregate(&self, group_cols: Vec<String>, mut stream: SendableDataBlockStream) -> Result<RwLock<(HashMap<Method::HashKey, usize>, Bump)>> {
        let aggr_len = self.funcs.len();
        let aggr_cols = &self.aggr_cols;
        let aggr_args_name = &self.arg_names;
        let layout = self.layout;
        let func = &self.funcs;
        let hash_method = &self.method;
        let offsets_aggregate_states = &self.offsets_aggregate_states;

        let groups_locker = RwLock::new((HashMap::<Method::HashKey, usize>::new(), Bump::new()));

        while let Some(block) = stream.next().await {
            let block = block?;
            // 1.1 and 1.2.
            let mut group_columns = Vec::with_capacity(group_cols.len());
            {
                for col in group_cols.iter() {
                    group_columns.push(block.try_column_by_name(col)?);
                }
            }

            let mut aggr_arg_columns = Vec::with_capacity(aggr_len);
            for (idx, _aggr_col) in aggr_cols.iter().enumerate() {
                let arg_columns = aggr_args_name[idx]
                    .iter()
                    .map(|arg| block.try_column_by_name(arg).and_then(|c| c.to_array()))
                    .collect::<Result<Vec<Series>>>()?;
                aggr_arg_columns.push(arg_columns);
            }

            // this can benificial for the case of dereferencing
            let aggr_arg_columns_slice = &aggr_arg_columns;

            let mut places = Vec::with_capacity(block.num_rows());
            let group_keys = hash_method.build_keys(&group_columns, block.num_rows())?;
            let mut groups = groups_locker.write();
            {
                for group_key in group_keys.iter() {
                    let mut inserted = true;
                    let entity = groups.0.insert_key(group_key, &mut inserted);

                    match inserted {
                        true => {
                            if aggr_len == 0 {
                                entity.set_value(0);
                            } else {
                                let place: StateAddr = groups.1.alloc_layout(layout).into();
                                for idx in 0..aggr_len {
                                    let aggr_state = offsets_aggregate_states[idx];
                                    let aggr_state_place = place.next(aggr_state);
                                    func[idx].init_state(aggr_state_place);
                                }
                                places.push(place);
                                entity.set_value(place.addr());
                            }
                        }
                        false => {
                            let place: StateAddr = (*entity.get_value()).into();
                            places.push(place);
                        }
                    }
                }

                for ((idx, func), args) in func.iter().enumerate().zip(aggr_arg_columns_slice.iter()) {
                    func.accumulate_keys(
                        &places,
                        offsets_aggregate_states[idx],
                        args,
                        block.num_rows(),
                    )?;
                }
            }
        }
        Ok(groups_locker)
    }

    pub fn aggregate_finalized<T: DFNumericType>(
        &self,
        groups: &HashMap<T::Native, usize>,
        schema: DataSchemaRef,
    ) -> Result<SendableDataBlockStream>
        where DefaultHasher<T::Native>: KeyHasher<T::Native>,
              DefaultHashTableEntity<T::Native, usize>: HashTableEntity<T::Native> {
        let aggr_len = self.funcs.len();
        if groups.len() == 0 {
            return Ok(Box::pin(DataBlockStream::create(
                DataSchemaRefExt::create(vec![]),
                None,
                vec![],
            )));
        }

        // Builders.
        let mut state_builders: Vec<BinaryArrayBuilder> = (0..aggr_len)
            .map(|_| BinaryArrayBuilder::with_capacity(groups.len() * 4))
            .collect();


        let mut group_key_builder = PrimitiveArrayBuilder::<T>::with_capacity(groups.len());

        let mut bytes = BytesMut::new();
        let funcs = &self.funcs;
        let offsets_aggregate_states = &self.offsets_aggregate_states;
        for group_entity in groups.iter() {
            let place: StateAddr = (*group_entity.get_value()).into();

            for (idx, func) in funcs.iter().enumerate() {
                let arg_place = place.next(offsets_aggregate_states[idx]);
                func.serialize(arg_place, &mut bytes)?;
                state_builders[idx].append_value(&bytes[..]);
                bytes.clear();
            }

            group_key_builder.append_value((*(group_entity.get_key())).clone());
        }

        let mut columns: Vec<Series> = Vec::with_capacity(schema.fields().len());
        for mut builder in state_builders {
            columns.push(builder.finish().into_series());
        }
        let array = group_key_builder.finish();
        columns.push(array.array.into_series());

        let block = DataBlock::create_by_array(schema.clone(), columns);
        Ok(Box::pin(DataBlockStream::create(
            schema.clone(),
            None,
            vec![block],
        )))
    }
}
