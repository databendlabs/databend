use std::borrow::BorrowMut;
use std::sync::Arc;
use bumpalo::Bump;
use bytes::BytesMut;
use common_datablocks::DataBlock;
use common_datavalues::arrays::DFStringArray;
use common_datavalues::DataSchemaRef;
use common_datavalues::prelude::{create_mutable_array, IntoSeries, MutableArrayBuilder, Series, StringArrayBuilder};
use common_exception::Result;
use common_functions::aggregates::{AggregateFunctionRef, get_layout_offsets, StateAddr};
use common_planners::Expression;
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform_aggregator::Aggregator;

pub type FinalSingleKeyAggregator = SingleKeyAggregatorImpl<true>;
pub type PartialSingleKeyAggregator = SingleKeyAggregatorImpl<false>;

/// SELECT COUNT | SUM FROM table;
pub struct SingleKeyAggregator;

pub struct SingleKeyAggregatorImpl<const FINAL: bool> {
    funcs: Vec<AggregateFunctionRef>,
    arg_names: Vec<Vec<String>>,
    schema: DataSchemaRef,
    arena: Bump,
    places: Vec<usize>,
    is_finished: bool,
}

impl<const FINAL: bool> SingleKeyAggregatorImpl<FINAL> {
    pub fn try_create(params: &Arc<AggregatorParams>) -> Result<Self> {
        let arena = Bump::new();
        let (layout, offsets_aggregate_states) = unsafe { get_layout_offsets(&params.aggregate_functions) };

        let places: Vec<usize> = {
            let place: StateAddr = arena.alloc_layout(layout).into();
            params.aggregate_functions
                .iter()
                .enumerate()
                .map(|(idx, func)| {
                    let arg_place = place.next(offsets_aggregate_states[idx]);
                    func.init_state(arg_place);
                    arg_place.addr()
                })
                .collect()
        };

        Ok(Self {
            arena,
            places,
            funcs: params.aggregate_functions.clone(),
            arg_names: params.aggregate_functions_arguments_name.clone(),
            schema: params.schema.clone(),
            is_finished: false,
        })
    }
}


impl Aggregator for SingleKeyAggregatorImpl<true> {
    const NAME: &'static str = "FinalSingleKeyAggregator";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        for (index, func) in self.funcs.iter().enumerate() {
            let place = self.places[index].into();

            let binary_array = block.column(index).to_array()?;
            let binary_array: &DFStringArray = binary_array.string()?;
            let array = binary_array.inner();

            let mut data = array.value(0);
            let s = self.funcs[index].state_layout();
            let temp = self.arena.alloc_layout(s);
            let temp_addr = temp.into();
            self.funcs[index].init_state(temp_addr);

            func.deserialize(temp_addr, &mut data)?;
            func.merge(place, temp_addr)?;
        }

        Ok(())
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        self.is_finished = true;
        let mut aggr_values: Vec<Box<dyn MutableArrayBuilder>> = {
            let mut values = vec![];
            for func in &self.funcs {
                let array = create_mutable_array(func.return_type()?);
                values.push(array)
            }
            values
        };

        for (index, func) in self.funcs.iter().enumerate() {
            let place = self.places[index].into();
            let array: &mut dyn MutableArrayBuilder = aggr_values[index].borrow_mut();
            let _ = func.merge_result(place, array)?;
        }

        let mut columns: Vec<Series> = Vec::with_capacity(self.funcs.len());
        for mut array in aggr_values {
            columns.push(array.as_series());
        }

        Ok(Some(DataBlock::create_by_array(self.schema.clone(), columns)))
    }
}

impl Aggregator for SingleKeyAggregatorImpl<false> {
    const NAME: &'static str = "PartialSingleKeyAggregator";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        let rows = block.num_rows();
        for (idx, func) in self.funcs.iter().enumerate() {
            let mut arg_columns = vec![];
            for name in self.arg_names[idx].iter() {
                arg_columns.push(block.try_column_by_name(name)?.to_array()?);
            }
            let place = self.places[idx].into();
            func.accumulate(place, &arg_columns, rows)?;
        }

        Ok(())
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        self.is_finished = true;
        let mut columns = Vec::with_capacity(self.funcs.len());
        let mut bytes = BytesMut::new();

        for (idx, func) in self.funcs.iter().enumerate() {
            let place = self.places[idx].into();
            func.serialize(place, &mut bytes)?;
            let mut array_builder = StringArrayBuilder::with_capacity(4);
            array_builder.append_value(&bytes[..]);
            bytes.clear();
            columns.push(array_builder.finish().into_series());
        }

        // TODO: create with temp schema
        Ok(Some(DataBlock::create_by_array(self.schema.clone(), columns)))
    }
}

// impl Aggregator for SingleKeyAggregator {
//     const NAME: &'static str = "WithoutGroupByAggregator";
//
//     fn consume(&mut self, block: DataBlock) -> Result<()> {
//         // let rows = block.num_rows();
//         // for (idx, func) in self.funcs.iter().enumerate() {
//         //     let mut arg_columns = vec![];
//         //     for name in self.arg_names[idx].iter() {
//         //         arg_columns.push(block.try_column_by_name(name)?.to_array()?);
//         //     }
//         //     let place = self.places[idx].into();
//         //     func.accumulate(place, &arg_columns, rows)?;
//         // }
//
//         Ok(())
//     }
//
//     fn generate(&mut self) -> Result<Option<DataBlock>> {
//         // if self.is_finished {
//         //     return Ok(None);
//         // }
//         //
//         // self.is_finished = true;
//         // let mut columns = Vec::with_capacity(self.funcs.len());
//         // let mut bytes = BytesMut::new();
//         //
//         // for (idx, func) in self.funcs.iter().enumerate() {
//         //     let place = self.places[idx].into();
//         //     func.serialize(place, &mut bytes)?;
//         //     let mut array_builder = StringArrayBuilder::with_capacity(4);
//         //     array_builder.append_value(&bytes[..]);
//         //     bytes.clear();
//         //     columns.push(array_builder.finish().into_series());
//         // }
//
//         unimplemented!()
//         // Ok(Some(DataBlock::create_by_array(self.schema.clone(), columns)))
//     }
// }
