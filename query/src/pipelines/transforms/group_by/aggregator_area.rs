use std::alloc::Layout;
use bumpalo::Bump;
use common_exception::Result;
use crate::pipelines::transforms::group_by::aggregator_params::{AggregatorParams, AggregatorParamsRef};
use common_functions::aggregates::{get_layout_offsets, StateAddr};
use std::sync::Arc;


pub struct AggregatorArea {
    area: Bump,
    layout: Layout,
    params: AggregatorParamsRef,
    offsets_aggregate_states: Vec<usize>,
}

pub type AggregatorAreaRef = Arc<AggregatorArea>;

impl AggregatorArea {
    pub fn try_create(params: &AggregatorParamsRef) -> Result<AggregatorAreaRef> {
        let aggregate_functions = &params.aggregate_functions;
        let (states_layout, states_offsets) = unsafe { get_layout_offsets(aggregate_functions) };

        Ok(Arc::new(AggregatorArea {
            area: Bump::new(),
            layout: states_layout,
            params: params.clone(),
            offsets_aggregate_states: states_offsets,
        }))
    }

    #[inline(always)]
    pub fn alloc_aggregate_states(&self, area: &Bump) -> StateAddr {
        let aggregator_params = self.params.as_ref();
        let place: StateAddr = area.alloc_layout(self.layout).into();
        for index in 0..self.offsets_aggregate_states.len() {
            let aggr_state = self.offsets_aggregate_states[index];
            let aggr_state_place = place.next(aggr_state);
            aggregator_params.aggregate_functions[index].init_state(aggr_state_place);
        }

        place
    }
}


