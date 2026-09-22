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

use std::collections::BTreeMap;
use std::collections::HashMap;

use async_channel::Receiver;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_storages_common_table_meta::table::OPT_KEY_ANALYZE_FREQUENCY_COLUMNS;
use databend_storages_common_table_meta::table::analyze_count_min_sketch_error_rate_from_options;
use databend_storages_common_table_meta::table::analyze_top_n_size_from_options;

/// Which histogram, if any, ANALYZE produces.
#[derive(Clone)]
pub enum AnalyzeHistogramInfo {
    None,
    /// Buckets computed by window queries running alongside the analyze pipeline, one
    /// receiver per column id.
    Window(HashMap<u32, Receiver<DataBlock>>),
    /// Equal-depth buckets derived from KLL sketches gathered while scanning blocks.
    KllFast {
        relative_error: f64,
    },
    /// Bucket bounds derived from KLL sketches, then exact counts from a second block scan.
    KllFull {
        relative_error: f64,
    },
}

impl AnalyzeHistogramInfo {
    pub fn kll_relative_error(&self) -> Option<f64> {
        match self {
            AnalyzeHistogramInfo::KllFast { relative_error }
            | AnalyzeHistogramInfo::KllFull { relative_error } => Some(*relative_error),
            AnalyzeHistogramInfo::None | AnalyzeHistogramInfo::Window(_) => None,
        }
    }
}

/// Frequency statistics (Top-N and count-min sketch) requested for a set of columns.
#[derive(Clone, Debug)]
pub struct FrequencyOptions {
    /// Comma separated column list, as written in `analyze_frequency_columns`.
    pub columns: String,
    pub top_n_size: Option<usize>,
    pub count_min_sketch_error_rate: Option<f64>,
}

/// Everything that shapes one ANALYZE run.
#[derive(Clone)]
pub struct AnalyzeOptions {
    pub histogram: AnalyzeHistogramInfo,
    pub frequency: Option<FrequencyOptions>,
    /// Only reuse persisted block statistics; blocks without them count as unanalyzed rows.
    pub no_scan: bool,
}

impl AnalyzeOptions {
    /// Frequency statistics as configured on the table; no histogram, full scan.
    pub fn from_table_options(options: &BTreeMap<String, String>) -> Result<Self> {
        let top_n_size = analyze_top_n_size_from_options(options)?;
        let count_min_sketch_error_rate =
            analyze_count_min_sketch_error_rate_from_options(options)?;
        let frequency = options
            .get(OPT_KEY_ANALYZE_FREQUENCY_COLUMNS)
            .filter(|columns| !columns.trim().is_empty())
            .filter(|_| top_n_size.is_some() || count_min_sketch_error_rate.is_some())
            .map(|columns| FrequencyOptions {
                columns: columns.clone(),
                top_n_size,
                count_min_sketch_error_rate,
            });
        Ok(Self {
            histogram: AnalyzeHistogramInfo::None,
            frequency,
            no_scan: false,
        })
    }

    pub fn with_histogram(mut self, histogram: AnalyzeHistogramInfo) -> Self {
        self.histogram = histogram;
        self
    }

    /// Frequency statistics need block data, so NOSCAN drops them.
    pub fn no_scan(mut self) -> Self {
        self.no_scan = true;
        self.frequency = None;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frequency_options_require_columns_and_a_statistic() {
        let mut options = BTreeMap::new();
        assert!(
            AnalyzeOptions::from_table_options(&options)
                .unwrap()
                .frequency
                .is_none()
        );

        options.insert("analyze_frequency_columns".to_string(), "c".to_string());
        assert!(
            AnalyzeOptions::from_table_options(&options)
                .unwrap()
                .frequency
                .is_none()
        );

        options.insert("analyze_top_n_size".to_string(), "3".to_string());
        let frequency = AnalyzeOptions::from_table_options(&options)
            .unwrap()
            .frequency
            .unwrap();
        assert_eq!(frequency.columns, "c");
        assert_eq!(frequency.top_n_size, Some(3));
        assert_eq!(frequency.count_min_sketch_error_rate, None);

        assert!(
            AnalyzeOptions::from_table_options(&options)
                .unwrap()
                .no_scan()
                .frequency
                .is_none()
        );
    }
}
