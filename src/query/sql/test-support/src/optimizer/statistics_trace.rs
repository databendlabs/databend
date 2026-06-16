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

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_statistics::Histogram;
use serde::Deserialize;

use super::HistogramStats;
use super::histogram_from_stats;

#[derive(Debug, Clone, Deserialize)]
pub struct StatisticsTraceInput {
    table_stats: HashMap<usize, TableStatistics>,
    column_stats: Vec<StatisticsTraceColumn>,
    scan_mappings: Vec<StatisticsTraceScanMapping>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StatisticsTraceColumn {
    pub table_index: usize,
    pub column_name: String,
    pub column_position: Option<usize>,
    pub data_type: TableDataType,
    pub statistics: Option<BasicColumnStatistics>,
    pub histogram: Option<HistogramStats>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StatisticsTraceScanMapping {
    pub table_index: usize,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct StatisticsTraceTableSpec {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub fields: Vec<TableField>,
    pub table_stats: Option<TableStatistics>,
    pub column_stats: HashMap<String, BasicColumnStatistics>,
    pub histograms: HashMap<String, Histogram>,
}

impl StatisticsTraceInput {
    pub fn from_json_str(input: &str) -> Result<Self> {
        serde_json::from_str(input)
            .map_err(|err| ErrorCode::Internal(format!("invalid statistics trace JSON: {err}")))
    }

    pub fn table_specs(&self) -> Result<Vec<StatisticsTraceTableSpec>> {
        let mut scan_mappings = HashMap::new();
        for mapping in &self.scan_mappings {
            scan_mappings
                .entry(mapping.table_index)
                .or_insert_with(|| mapping.clone());
        }

        let mut columns_by_table: HashMap<usize, Vec<&StatisticsTraceColumn>> = HashMap::new();
        for column in &self.column_stats {
            columns_by_table
                .entry(column.table_index)
                .or_default()
                .push(column);
        }

        let mut table_indices = scan_mappings.keys().copied().collect::<Vec<_>>();
        table_indices.sort_unstable();

        table_indices
            .into_iter()
            .map(|table_index| {
                let mapping = scan_mappings.get(&table_index).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "statistics trace missing scan mapping for table index {table_index}"
                    ))
                })?;
                let mut columns = columns_by_table.remove(&table_index).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "statistics trace missing columns for table {}.{}",
                        mapping.database, mapping.table
                    ))
                })?;
                columns.sort_by(|left, right| {
                    left.column_position
                        .cmp(&right.column_position)
                        .then_with(|| left.column_name.cmp(&right.column_name))
                });

                let fields = columns
                    .iter()
                    .map(|column| TableField::new(&column.column_name, column.data_type.clone()))
                    .collect();
                let column_stats = columns
                    .iter()
                    .filter_map(|column| {
                        column
                            .statistics
                            .clone()
                            .map(|stats| (column.column_name.clone(), stats))
                    })
                    .collect();
                let histograms = columns
                    .iter()
                    .filter_map(|column| {
                        column.histogram.as_ref().map(|histogram| {
                            histogram_from_stats(histogram)
                                .map(|histogram| (column.column_name.clone(), histogram))
                        })
                    })
                    .collect::<Result<HashMap<_, _>>>()?;

                Ok(StatisticsTraceTableSpec {
                    catalog: mapping.catalog.clone(),
                    database: mapping.database.clone(),
                    table: mapping.table.clone(),
                    fields,
                    table_stats: self.table_stats.get(&table_index).cloned(),
                    column_stats,
                    histograms,
                })
            })
            .collect()
    }
}
