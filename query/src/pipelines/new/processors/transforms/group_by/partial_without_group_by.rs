use bumpalo::Bump;
use bytes::BytesMut;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::prelude::{IntoSeries, StringArrayBuilder};
use common_exception::Result;
use common_functions::aggregates::{AggregateFunctionRef, get_layout_offsets, StateAddr};
use common_planners::Expression;
use crate::pipelines::new::processors::transforms::transform_aggregator::Aggregator;

/// SELECT COUNT | SUM FROM table;
pub struct WithoutGroupBy {
    funcs: Vec<AggregateFunctionRef>,
    arg_names: Vec<Vec<String>>,
    schema: DataSchemaRef,
    arena: Bump,
    places: Vec<usize>,
    is_finished: bool,
}

impl WithoutGroupBy {
    pub fn create(schema: DataSchemaRef, schema_before_group_by: DataSchemaRef, exprs: &[Expression]) -> Result<WithoutGroupBy> {
        let funcs = exprs
            .iter()
            .map(|expr| expr.to_aggregate_function(&schema_before_group_by))
            .collect::<Result<Vec<_>>>()?;

        let arg_names = exprs
            .iter()
            .map(|expr| expr.to_aggregate_function_names())
            .collect::<Result<Vec<_>>>()?;

        let arena = Bump::new();
        let (layout, offsets_aggregate_states) = unsafe { get_layout_offsets(&funcs) };

        let places: Vec<usize> = {
            let place: StateAddr = arena.alloc_layout(layout).into();
            funcs
                .iter()
                .enumerate()
                .map(|(idx, func)| {
                    let arg_place = place.next(offsets_aggregate_states[idx]);
                    func.init_state(arg_place);
                    arg_place.addr()
                })
                .collect()
        };

        Ok(WithoutGroupBy { funcs, arg_names, schema, arena, places, is_finished: false })
    }
}

impl Aggregator for WithoutGroupBy {
    const NAME: &'static str = "WithoutGroupByAggregator";

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

        Ok(Some(DataBlock::create_by_array(self.schema.clone(), columns)))
    }
}
