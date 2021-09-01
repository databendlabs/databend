use std::alloc::Layout;
use crate::pipelines::transforms::group_by::AggregatorParams;
use common_functions::aggregates::get_layout_offsets;

pub struct AggregatorLayout {
    pub layout: Layout,
    pub offsets_aggregate_states: Vec<usize>,
}

impl AggregatorLayout {
    pub fn create(params: &AggregatorParams) -> AggregatorLayout {
        let aggregate_functions = &params.aggregate_functions;
        let (states_layout, states_offsets) = unsafe { get_layout_offsets(aggregate_functions) };

        AggregatorLayout {
            layout: states_layout,
            offsets_aggregate_states: states_offsets,
        }
    }
}
