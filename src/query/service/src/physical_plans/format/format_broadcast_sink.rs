// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::Result;
use itertools::Itertools;

use crate::physical_plans::format::format_output_columns;
use crate::physical_plans::format::plan_stats_info_to_format_tree;
use crate::physical_plans::format::pretty_display_agg_desc;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::AggregateFinal;
use crate::physical_plans::AggregatePartial;
use crate::physical_plans::AsyncFunction;
use crate::physical_plans::BroadcastSink;
use crate::physical_plans::IPhysicalPlan;

pub struct BroadcastSinkFormatter<'a> {
    _inner: &'a BroadcastSink,
}

impl<'a> BroadcastSinkFormatter<'a> {
    pub fn new(_inner: &'a BroadcastSink) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(BroadcastSinkFormatter { _inner })
    }
}

impl<'a> PhysicalFormat for BroadcastSinkFormatter<'a> {
    fn format(&self, _ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::new("RuntimeFilterSink".to_string()))
    }
}
