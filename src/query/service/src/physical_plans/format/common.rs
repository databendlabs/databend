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

use std::collections::HashMap;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_base::base::format_byte_size;
use databend_common_base::runtime::profile::get_statistics_desc;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::runtime_filter_info::RuntimeFilterReport;
use databend_common_expression::DataSchemaRef;
use databend_common_sql::executor::physical_plans::AggregateFunctionDesc;
use databend_common_sql::IndexType;
use databend_common_sql::Metadata;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::physical_format::PhysicalFormat;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::PhysicalRuntimeFilter;

pub struct FormatContext<'a> {
    pub metadata: &'a Metadata,
    pub profs: HashMap<u32, PlanProfile>,
    pub scan_id_to_runtime_filters: HashMap<IndexType, Vec<PhysicalRuntimeFilter>>,
    pub runtime_filter_reports: HashMap<IndexType, Vec<RuntimeFilterReport>>,
}

pub fn pretty_display_agg_desc(desc: &AggregateFunctionDesc, metadata: &Metadata) -> String {
    format!(
        "{}({})",
        desc.sig.name,
        desc.arg_indices
            .iter()
            .map(|&index| { metadata.column(index).name() })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn format_pruning_cost_suffix(cost_micros: u64) -> String {
    if cost_micros == 0 {
        String::new()
    } else if cost_micros < 1_000 {
        " cost: <1 ms".to_string()
    } else {
        format!(" cost: {} ms", cost_micros / 1_000)
    }
}

pub fn part_stats_info_to_format_tree(info: &PartStatistics) -> Vec<FormatTreeNode<String>> {
    let read_size = format_byte_size(info.read_bytes);
    let mut items = vec![
        FormatTreeNode::new(format!("read rows: {}", info.read_rows)),
        FormatTreeNode::new(format!("read size: {}", read_size)),
        FormatTreeNode::new(format!("partitions total: {}", info.partitions_total)),
        FormatTreeNode::new(format!("partitions scanned: {}", info.partitions_scanned)),
    ];

    // format is like "pruning stats: [segments: <range pruning: x to y>, blocks: <range pruning: x to y>]"
    let mut blocks_pruning_description = String::new();

    // range pruning status.
    if info.pruning_stats.blocks_range_pruning_before > 0 {
        blocks_pruning_description += &format!(
            "range pruning: {} to {}{}",
            info.pruning_stats.blocks_range_pruning_before,
            info.pruning_stats.blocks_range_pruning_after,
            format_pruning_cost_suffix(info.pruning_stats.blocks_range_pruning_cost)
        );
    }

    // bloom pruning status.
    if info.pruning_stats.blocks_bloom_pruning_before > 0 {
        if !blocks_pruning_description.is_empty() {
            blocks_pruning_description += ", ";
        }
        blocks_pruning_description += &format!(
            "bloom pruning: {} to {}{}",
            info.pruning_stats.blocks_bloom_pruning_before,
            info.pruning_stats.blocks_bloom_pruning_after,
            format_pruning_cost_suffix(info.pruning_stats.blocks_bloom_pruning_cost)
        );
    }

    // inverted index pruning status.
    if info.pruning_stats.blocks_inverted_index_pruning_before > 0 {
        if !blocks_pruning_description.is_empty() {
            blocks_pruning_description += ", ";
        }
        blocks_pruning_description += &format!(
            "inverted pruning: {} to {}{}",
            info.pruning_stats.blocks_inverted_index_pruning_before,
            info.pruning_stats.blocks_inverted_index_pruning_after,
            format_pruning_cost_suffix(info.pruning_stats.blocks_inverted_index_pruning_cost)
        );
    }

    // topn pruning status.
    if info.pruning_stats.blocks_topn_pruning_before > 0 {
        if !blocks_pruning_description.is_empty() {
            blocks_pruning_description += ", ";
        }
        blocks_pruning_description += &format!(
            "topn pruning: {} to {}",
            info.pruning_stats.blocks_topn_pruning_before,
            info.pruning_stats.blocks_topn_pruning_after
        );
    }

    // vector index pruning status.
    if info.pruning_stats.blocks_vector_index_pruning_before > 0 {
        if !blocks_pruning_description.is_empty() {
            blocks_pruning_description += ", ";
        }
        blocks_pruning_description += &format!(
            "vector pruning: {} to {}{}",
            info.pruning_stats.blocks_vector_index_pruning_before,
            info.pruning_stats.blocks_vector_index_pruning_after,
            format_pruning_cost_suffix(info.pruning_stats.blocks_vector_index_pruning_cost)
        );
    }

    // Combine segment pruning and blocks pruning descriptions if any
    if info.pruning_stats.segments_range_pruning_before > 0
        || !blocks_pruning_description.is_empty()
    {
        let mut pruning_description = String::new();

        if info.pruning_stats.segments_range_pruning_before > 0 {
            pruning_description += &format!(
                "segments: <range pruning: {} to {}{}>",
                info.pruning_stats.segments_range_pruning_before,
                info.pruning_stats.segments_range_pruning_after,
                format_pruning_cost_suffix(info.pruning_stats.segments_range_pruning_cost)
            );
        }

        if !blocks_pruning_description.is_empty() {
            if !pruning_description.is_empty() {
                pruning_description += ", ";
            }
            pruning_description += &format!("blocks: <{}>", blocks_pruning_description);
        }

        items.push(FormatTreeNode::new(format!(
            "pruning stats: [{}]",
            pruning_description
        )));
    }

    items
}

pub fn plan_stats_info_to_format_tree(info: &PlanStatsInfo) -> Vec<FormatTreeNode<String>> {
    vec![FormatTreeNode::new(format!(
        "estimated rows: {0:.2}",
        info.estimated_rows
    ))]
}

pub fn format_output_columns(
    output_schema: DataSchemaRef,
    metadata: &Metadata,
    format_table: bool,
) -> String {
    output_schema
        .fields()
        .iter()
        .map(|field| match field.name().parse::<usize>() {
            Ok(column_index) => {
                if column_index == usize::MAX {
                    return String::from("dummy value");
                }
                let column_entry = metadata.column(column_index);
                match column_entry.table_index() {
                    Some(table_index) if format_table => match metadata
                        .table(table_index)
                        .alias_name()
                    {
                        Some(alias_name) => {
                            format!("{}.{} (#{})", alias_name, column_entry.name(), column_index)
                        }
                        None => format!(
                            "{}.{} (#{})",
                            metadata.table(table_index).name(),
                            column_entry.name(),
                            column_index,
                        ),
                    },
                    _ => format!("{} (#{})", column_entry.name(), column_index),
                }
            }
            _ => format!("#{}", field.name()),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn append_output_rows_info(
    children: &mut Vec<FormatTreeNode<String>>,
    profs: &HashMap<u32, PlanProfile>,
    plan_id: u32,
) {
    if let Some(prof) = profs.get(&plan_id) {
        for (_, desc) in get_statistics_desc().iter() {
            if desc.display_name != "output rows" {
                continue;
            }
            if prof.statistics[desc.index] != 0 {
                children.push(FormatTreeNode::new(format!(
                    "{}: {}",
                    desc.display_name.to_lowercase(),
                    desc.human_format(prof.statistics[desc.index])
                )));
            }
            break;
        }
    }
}

use databend_common_exception::Result;
use databend_common_pipeline::core::PlanProfile;

pub struct SimplePhysicalFormat<'a> {
    meta: &'a PhysicalPlanMeta,
    children: Vec<Box<dyn PhysicalFormat + 'a>>,
}

impl<'a> SimplePhysicalFormat<'a> {
    pub fn create(
        meta: &'a PhysicalPlanMeta,
        children: Vec<Box<dyn PhysicalFormat + 'a>>,
    ) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(Self { meta, children })
    }
}

impl<'a> PhysicalFormat for SimplePhysicalFormat<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.meta
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let mut children = vec![];
        for child in self.children.iter() {
            children.push(child.dispatch(ctx)?);
        }

        Ok(FormatTreeNode::with_children(
            self.meta.name.clone(),
            children,
        ))
    }

    #[recursive::recursive]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        if self.children.len() == 1 {
            return self.children[0].format_join(ctx);
        }

        let mut children = vec![];
        for child in self.children.iter() {
            children.push(child.format_join(ctx)?);
        }

        Ok(FormatTreeNode::with_children(
            self.meta.name.clone(),
            children,
        ))
    }

    #[recursive::recursive]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        if self.children.len() == 1 {
            return self.children[0].partial_format(ctx);
        }

        let mut children = vec![];
        for child in self.children.iter() {
            children.push(child.partial_format(ctx)?);
        }

        Ok(FormatTreeNode::with_children(
            self.meta.name.clone(),
            children,
        ))
    }
}
