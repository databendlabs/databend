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
use databend_common_expression::TableField;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_statistics::Histogram;
use serde::Deserialize;

use super::HistogramStats;
use super::histogram_from_stats;

#[derive(Debug, Clone, Deserialize)]
pub struct ReplayInput {
    #[serde(default)]
    pub views: Vec<ReplayView>,
    #[serde(default)]
    pub udfs: Vec<ReplayUdf>,
    pub table_stats: HashMap<usize, ReplayTable>,
    pub column_stats: Vec<ReplayColumn>,
    pub scan_mappings: Vec<ReplayScan>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReplayView {
    pub catalog: String,
    pub database: String,
    pub view: String,
    pub query: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReplayUdf {
    pub name: String,
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReplayTable {
    pub statistics: Option<TableStatistics>,
    pub fields: Vec<TableField>,
    #[serde(default)]
    pub row_access_policy: Option<ReplayRowAccessPolicy>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReplayRowAccessPolicy {
    pub policy_id: u64,
    pub columns: Vec<String>,
    pub args: Vec<(String, String)>,
    pub body: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReplayColumn {
    pub table_index: usize,
    pub column_name: String,
    pub statistics: Option<BasicColumnStatistics>,
    pub histogram: Option<HistogramStats>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReplayScan {
    pub table_index: usize,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

pub struct ReplayParts {
    pub views: Vec<ReplayView>,
    pub user_defined_functions: Vec<UserDefinedFunction>,
    pub tables: Vec<ReplayTableSpec>,
}

#[derive(Debug, Clone)]
pub struct ReplayTableSpec {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub fields: Vec<TableField>,
    pub table_stats: Option<TableStatistics>,
    pub column_stats: HashMap<String, BasicColumnStatistics>,
    pub histograms: HashMap<String, Histogram>,
    pub row_access_policy: Option<ReplayRowAccessPolicy>,
}

impl ReplayInput {
    pub fn into_parts(&self) -> Result<ReplayParts> {
        let user_defined_functions = self
            .udfs
            .iter()
            .map(ReplayUdf::to_udf)
            .collect::<Result<_>>()?;

        let mut scan_mappings = HashMap::new();
        for mapping in &self.scan_mappings {
            scan_mappings
                .entry(mapping.table_index)
                .or_insert_with(|| mapping.clone());
        }

        let mut table_indices = scan_mappings.keys().copied().collect::<Vec<_>>();
        table_indices.sort_unstable();

        let tables = table_indices
            .into_iter()
            .map(|table_index| {
                let mapping = scan_mappings.get(&table_index).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "statistics trace missing scan mapping for table index {table_index}"
                    ))
                })?;

                let table = self.table_stats.get(&table_index);
                let table = table.ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "statistics trace missing table stats for table index {table_index}"
                    ))
                })?;
                let fields = table.fields.clone();
                let column_stats = self
                    .column_stats
                    .iter()
                    .filter_map(|column| {
                        if column.table_index != table_index {
                            return None;
                        }
                        column
                            .statistics
                            .as_ref()
                            .map(|stats| (column.column_name.clone(), stats.clone()))
                    })
                    .collect();
                let histograms = self
                    .column_stats
                    .iter()
                    .filter_map(|column| {
                        if column.table_index != table_index {
                            return None;
                        }
                        column.histogram.as_ref().map(|histogram| {
                            histogram_from_stats(histogram)
                                .map(|histogram| (column.column_name.clone(), histogram))
                        })
                    })
                    .collect::<Result<HashMap<_, _>>>()?;

                Ok(ReplayTableSpec {
                    catalog: mapping.catalog.clone(),
                    database: mapping.database.clone(),
                    table: mapping.table.clone(),
                    fields,
                    table_stats: table.statistics,
                    column_stats,
                    histograms,
                    row_access_policy: table.row_access_policy.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ReplayParts {
            views: self.views.clone(),
            user_defined_functions,
            tables,
        })
    }
}

impl ReplayUdf {
    fn to_udf(&self) -> Result<UserDefinedFunction> {
        let arg_types = self
            .arg_types
            .iter()
            .map(infer_schema_type)
            .collect::<Result<Vec<_>>>()?;
        let return_type = infer_schema_type(&self.return_type)?;

        Ok(UserDefinedFunction::create_udf_script(
            &self.name,
            "return null;",
            &self.name,
            "javascript",
            arg_types,
            return_type,
            "",
            "",
            Some(false),
        ))
    }
}
